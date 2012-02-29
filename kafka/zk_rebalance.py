# pylint: disable=C0111

from collections import defaultdict
import logging

import zookeeper

import kafka.partition_topic_info
import kafka.topic_count
import kafka.utils

log = logging.getLogger(__name__)

MAX_N_RETRIES = 4

class ZKRebalancerListener(object):

  def __init__(self, group, consumer_id, consumer):
    self.group       = group
    self.consumer_id = consumer_id
    self.consumer    = consumer

    self.dirs = kafka.utils.GroupDirs(group)

    self.old_partitions_per_topic = dict()
    self.old_consumers_per_topic  = dict()

  # pylint: disable=W0613
  def handle_child_change(self, parent_path, cur_childs):
    self.synced_rebalance()

  def __release_partition_ownership(self):
    for topic, infos in self.consumer.topic_registry.iteritems():

      topic_dirs = kafka.utils.GroupTopicDirs(self.group, topic)

      for partition in infos.keys():

        znode = topic_dirs.consumer_owner_dir + "/" + partition

        self.consumer.zkclient.delete_path(znode)

        log.debug("Consumer %s releasing: %s", self.consumer_id, znode)

  def __get_consumers_per_topic(self):

    consumers = self.consumer.zkclient.get_children(self.dirs.consumer_registry_dir)

    consumers_per_topic = defaultdict(list)

    for consumer in consumers:

      topic_count = self.__get_topic_count(consumer)

      for topic, consumer_thread_ids in topic_count.consumer_thread_ids_per_topic().iteritems():

        for consumer_thread_id in consumer_thread_ids:
          consumers_per_topic[topic].append(consumer_thread_id)

    # 2nd pass to sort the consumer list.
    for topic, consumer_list in consumers_per_topic.iteritems():
      consumers_per_topic[topic] = sorted(consumer_list)

    return consumers_per_topic

  def __get_relevant_topic(self, my_topic_thread_ids, new_part, old_part, new_consumer, old_consumer):
    relevant_topic_thread_ids = dict()

    for topic, consumer_thread_ids in my_topic_thread_ids.iteritems():

      if old_part.get(topic, None) != new_part.get(topic, None) or old_consumer.get(topic, None) != new_consumer.get(topic, None):
        relevant_topic_thread_ids[topic] = consumer_thread_ids

    return relevant_topic_thread_ids

  def __get_topic_count(self, consumer_id):
    topic_count_json = self.consumer.zkclient.get(self.dirs.consumer_registry_dir + "/" + consumer_id)[0]

    return kafka.topic_count.construct(consumer_id, topic_count_json)

  def reset_state(self):
    self.consumer.topic_registry.clear()
    self.old_consumers_per_topic.clear()
    self.old_partitions_per_topic.clear()

  def synced_rebalance(self):
    #rebalanceLock synchronized {
    if True:

      for i in xrange(MAX_N_RETRIES):
        log.info("Begin rebalancing consumer %s try #%d", self.consumer_id, i)

        done = self.__rebalance()

        log.info("End rebalancing consumer %s try #%d", self.consumer_id, i)

        if done:
          return

        # Release all partitions, reset state and retry
        self.__release_partition_ownership()
        self.reset_state()
        #time.sleep(self.config.zk_sync_time_ms)

    raise RuntimeError("%s can't rebalance after %d retries." % (self.consumer_id, MAX_N_RETRIES))

  def __rebalance(self):

    my_topic_thread_ids = self.__get_topic_count(self.consumer_id).consumer_thread_ids_per_topic()

    cluster = kafka.utils.get_cluster(self.consumer.zkclient)

    consumers_per_topic = self.__get_consumers_per_topic()

    partitions_per_topic = kafka.utils.get_partitions_for_topics(self.consumer.zkclient, my_topic_thread_ids.iterkeys())

    relevant_topic_thread_ids = self.__get_relevant_topic(
      my_topic_thread_ids, partitions_per_topic, self.old_partitions_per_topic, consumers_per_topic, self.old_consumers_per_topic
    )

    if len(relevant_topic_thread_ids) <= 0:
      log.info("Consumer %s with %d doesn't need to rebalance.", self.consumer_id, consumers_per_topic)
      return True

    log.info("Committing all offsets")

    self.consumer.commit_offsets()

    log.info("Releasing partition ownership")
    self.__release_partition_ownership()

    for topic, consumer_thread_ids in relevant_topic_thread_ids.iteritems():

      if topic in self.consumer.topic_registry:
        del self.consumer.topic_registry[topic]

      topic_dirs     = kafka.utils.GroupTopicDirs(self.group, topic)
      cur_consumers  = consumers_per_topic[topic]
      cur_partitions = partitions_per_topic[topic]

      n_parts_per_consumer       = len(cur_partitions) / len(cur_consumers)
      n_consumers_with_extra_part = len(cur_partitions) % len(cur_consumers)

      log.info("Consumer %s rebalancing the following partitions: %s for topic %s with consumers: %s",
        self.consumer_id, cur_partitions, topic, cur_consumers
      )

      for consumer_thread_id in consumer_thread_ids:

        my_consumer_position = cur_consumers.index(consumer_thread_id)

        assert(my_consumer_position >= 0)

        start_part = n_parts_per_consumer * my_consumer_position + min([my_consumer_position, n_consumers_with_extra_part])
        n_parts    = n_parts_per_consumer + (0 if (my_consumer_position + 1 > n_consumers_with_extra_part) else 1)

        log.debug("start_part: %d n_parts: %d", start_part, n_parts)

        # Range-partition the sorted partitions to consumers for better locality.
        # The first few consumers pick up an extra partition, if any.
        if n_parts <= 0:
          log.warn("No broker partions consumed by consumer thread %d for topic: %s", consumer_thread_id, topic)

        for i in xrange(start_part + n_parts):
          partition = cur_partitions[i]

          log.info("%s attempting to claim partition: %s", consumer_thread_id, partition)

          if not self.__process_partition(topic_dirs, partition, topic, consumer_thread_id):
            return False

    self.__update_fetcher(cluster)
    self.old_partitions_per_topic = partitions_per_topic
    self.old_consumers_per_topic  = consumers_per_topic

    return True

  def __update_fetcher(self, cluster):
    """ Update partitions for fetcher. """
    all_partition_infos = list()

    for partition_infos in self.consumer.topic_registry.values():
      for partition in partition_infos.values():
        all_partition_infos.append(partition)

    log.info("Consumer %s selected partitions: %s", self.consumer_id, ', '.join(sorted([str(x) for x in all_partition_infos])))

    if self.consumer.fetcher:
      self.consumer.fetcher.init_connections(all_partition_infos, cluster)

  def __process_partition(self, topic_dirs, partition, topic, consumer_thread_id):

    partition_owner_path = topic_dirs.consumer_owner_dir + "/" + partition

    try:
      self.consumer.zkclient.create_ephemeral_path_expect_conflict(partition_owner_path, consumer_thread_id)
    except zookeeper.NodeExistsException:
      # The node hasn't been deleted by the original owner. So wait a bit and retry.
      log.info("Waiting for the partition ownership to be deleted: %d", partition)
      return False

    self.__add_partition_topic_info(topic_dirs, partition, topic, consumer_thread_id)

    return True

  def __add_partition_topic_info(self, topic_dirs, partition_string, topic, consumer_thread_id):

    partition     = kafka.cluster.parse_partition(partition_string)
    offset_string = self.consumer.zkclient.get_maybe_none(topic_dirs.consumer_offset_dir + "/" + partition.name)

    # If first time starting a consumer, use default offset.
    # TODO: handle this better (if client doesn't know initial offsets)
    if offset_string is None:
      offset = 0
    else:
      offset = long(offset_string)

    queue           = self.consumer.queues[(topic, consumer_thread_id)]
    consumed_offset = offset
    fetched_offset  = offset

    self.consumer.topic_registry[topic] = {
      partition.name: kafka.partition_topic_info.PartitionTopicInfo(
        topic, partition.broker_id, partition, queue, consumed_offset, fetched_offset, int(self.consumer.config.fetch_size)
      )
    }
