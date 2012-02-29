"""
  This class handles the consumer's interaction with ZooKeeper.

  Directories:
  1. Consumer id registry:
  /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
  A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
  and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
  A consumer subscribes to event changes of the consumer id registry within its group.

  The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
  ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
  whether the creation of a sequential znode has succeeded or not. More details can be found at
  (http:#wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)

  2. Broker node registry:
  /brokers/[0...N] --> { "host" : "host:port",
                         "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
                                     "topicN": ["partition1" ... "partitionN"] } }
  This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
  node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
  is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
  the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
  A consumer subscribes to event changes of the broker node registry.

  3. Partition owner registry:
  /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
  This stores the mapping between broker partitions and consumers. Each partition is owned by a unique consumer
  within a consumer group. The mapping is reestablished after each rebalancing.

  4. Consumer offset tracking:
  /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
  Each consumer tracks the offset of the latest message consumed for each partition.

"""

#from __future__ import absolute_import

import logging
import Queue
import socket
import threading
import time

import kafka.cluster
import kafka.fetcher
import kafka.partition_topic_info
import kafka.topic_count
import kafka.utils
import kafka.zk
import kafka.zk_rebalance

log = logging.getLogger(__name__)

MAX_N_RETRIES = 4

def shutdown_command():
  return kafka.partition_topic_info.FetchedDataChunk([], None)

class AutoCommitter(threading.Thread):

  def __init__(self, consumer, name, interval):
    threading.Thread.__init__(self, name=name)
    self.consumer = consumer
    self.interval = interval
    self.shutdown = False

  def run(self):

    while True:
      time.sleep(self.interval)
      log.debug("Running auto_commit.")
      self.consumer.commit_offsets()

      if self.shutdown:
        break

class ConsumerConnector(object):

  def __init__(self, config, enable_fetcher=True):
    self.config         = config
    self.is_shutdown    = False
    self.shutdown_lock  = None
    self.rebalance_lock = None
    self.fetcher        = None
    self.topic_registry = dict()

    # queues : (topic,consumerThreadId) -> queue
    self.queues         = dict()
    self.scheduler      = AutoCommitter(self, "Kafka-consumer-autocommit-", config.auto_commit_interval_ms)
    self.zkclient       = kafka.zk.Client(self.config.zkconnect, self.config.zkconnection_timeout_ms)

    #
    if enable_fetcher:
      self.fetcher = kafka.fetcher.Fetcher(config, self.zkclient)

    if config.auto_commit:
      log.info("Starting auto committer every %sms", config.auto_commit_interval_ms)
      self.scheduler.start()

  def create_message_streams(self, topic_count_map):

    log.debug("Entering create_message_streams()")

    if not topic_count_map:
      raise RuntimeError("topic_count_map is empty")

    dirs = kafka.utils.GroupDirs(self.config.group_id)
    ret  = dict()

    consumer_uuid = '-'.join([socket.gethostname(), str(time.time())])
    consumer_id   = '_'.join([self.config.group_id, consumer_uuid])
    topic_count   = kafka.topic_count.TopicCount(consumer_id, topic_count_map)

    # Listener to consumer and partition changes
    load_balancer_listener = kafka.zk_rebalance.ZKRebalancerListener(self.config.group_id, consumer_id, self)

    self.__register_consumer_in_zk(dirs, consumer_id, topic_count)

    log.info("Subscribe to child changes on: %s", dirs.consumer_registry_dir)
    #self.zkclient.subscribe_child_changes(dirs.consumer_registry_dir, load_balancer_listener)

    # create a queue per topic per consumer thread
    consumer_thread_ids_per_topic = topic_count.consumer_thread_ids_per_topic()

    for topic, thread_ids in consumer_thread_ids_per_topic.iteritems():
      stream_list = []

      for thread_id in thread_ids:
        stream = Queue.Queue(self.config.max_queued_chunks)

        self.queues[(topic, thread_id)] = stream

        stream_list.append(list()) #XXX KafkaMessageStream(stream, self.config.consumer_timeout_ms))

      ret[topic] = stream_list

      log.debug("Adding topic %s and stream to map.", topic)

      # Register on broker partition path changes.
      partition_path = kafka.utils.BROKER_TOPICS_PATH + "/" + topic

      self.zkclient.make_sure_persistent_path_exists(partition_path)
      print "partition_path: " + partition_path
      #self.zkclient.subscribe_child_changes(partition_path, load_balancer_listener)

    # Register listener for session expired event.
    #self.zkclient.subscribe_state_changes(ZKSessionExpireListener(dirs, consumer_id, topic_count, load_balancer_listener))

    # Explicitly trigger load balancing for this consumer.
    load_balancer_listener.synced_rebalance()

    return ret

  def shutdown(self):
    """ Shutdown the consumer: Turn off the scheduler, close the ZK connection, etc. """

    if self.is_shutdown:
      return

    self.scheduler.shutdown = True
    #self.scheduler.join()

    self.__send_shutdown_to_all_queues()

    if self.zkclient:
      self.zkclient.close()
      self.zkclient = None

    if self.fetcher:
      self.fetcher.shutdown()

    self.is_shutdown = True

  def __register_consumer_in_zk(self, dirs, consumer_id, topic_count):
    log.info("Begin registering consumer %s in ZK", consumer_id)

    self.zkclient.create_ephemeral_path_expect_conflict(dirs.consumer_registry_dir + "/" + consumer_id, topic_count.to_json())

    log.info("End registering consumer %s in ZK", consumer_id)

  def __send_shutdown_to_all_queues(self):

    for queue in self.queues.values():
      log.debug("Clearing up queue..")
      queue.queue.clear()
      queue.put(shutdown_command())
      log.debug("Cleared queue and sent shutdown command")

  def commit_offsets(self):
    if self.zkclient is None:
      return

    for topic, infos in self.topic_registry.iteritems():

      topic_dirs = kafka.utils.GroupTopicDirs(self.config.group_id, topic)

      for info in infos.values():
        new_offset = info.consumed_offset

        try:
          self.zkclient.update_persistent_path(topic_dirs.consumer_offset_dir + "/" + info.partition.name, str(new_offset))
        except StandardError, t:
          log.warn("Exception during commitOffsets: %s", t)

        log.debug("Committed offset %d for topic %s", new_offset, info.topic)
