import logging

import kafka.cluster

log = logging.getLogger(__name__)

CONSUMERS_PATH     = "/consumers"
BROKER_IDS_PATH    = "/brokers/ids"
BROKER_TOPICS_PATH = "/brokers/topics"

def get_cluster(client):
  cluster = kafka.cluster.Cluster()

  nodes = client.get_children(BROKER_IDS_PATH)

  for node in nodes:
    broker_string = client.get(BROKER_IDS_PATH + "/" + node)[0]
    cluster.add(kafka.cluster.create_broker(int(node), broker_string))

  return cluster

def get_partitions_for_topics(client, topics):
  partitions = dict()

  for topic in topics:
    part_list = list()
    brokers   = client.get_children(BROKER_TOPICS_PATH + "/" + topic)

    for broker in brokers:
      nparts = int(client.get('/'.join([BROKER_TOPICS_PATH, topic, broker]))[0])

      for part in xrange(nparts):
        part_list.append('%s-%d' % (broker, part))

    partitions[topic] = sorted(part_list)

  return partitions

def setup_partition(client, broker_id, host, port, topic, nparts):

  broker_id_path = BROKER_IDS_PATH + "/" + str(broker_id)
  broker         = kafka.cluster.Broker(broker_id, broker_id, host, port)

  client.create_ephemeral_path_expect_conflict(broker_id_path, broker.zk_string())

  broker_part_topic_path = '/'.join([BROKER_TOPICS_PATH, topic, str(broker_id)])

  client.create_ephemeral_path_expect_conflict(broker_part_topic_path, str(nparts))

def delete_partition(client, broker_id, topic):

  client.delete('/'.join([BROKER_IDS_PATH, str(broker_id)]))
  client.delete('/'.join([BROKER_TOPICS_PATH, topic, str(broker_id)]))

class GroupDirs(object):

  def __init__(self, group):
    self.group = group

  @property
  def consumer_dir(self):
    return CONSUMERS_PATH

  @property
  def consumer_group_dir(self):
    return self.consumer_dir + "/" + self.group

  @property
  def consumer_registry_dir(self):
    return self.consumer_group_dir + "/ids"

class GroupTopicDirs(GroupDirs):

  def __init__(self, group, topic):
    GroupDirs.__init__(self, group)

    self.group = group
    self.topic = topic

  @property
  def consumer_offset_dir(self):
    return self.consumer_group_dir + "/offsets/" + self.topic

  @property
  def consumer_owner_dir(self):
    return self.consumer_group_dir + "/owners/" + self.topic

class Config(object):

  def __init__(self, props):
    # ZK host string
    self.zkconnect = props.get("zk.connect", "localhost:2181")

    # zookeeper session timeout
    self.session_timeout_ms = int(props.get("zk.sessiontimeout.ms", 6000))

    # the max time that the client waits to establish a connection to zookeeper
    self.zkconnection_timeout_ms = int(props.get("zk.conncetiontimeout.ms", self.session_timeout_ms))

    # how far a ZK follower can be behind a ZK leader
    self.sync_time_ms = int(props.get("zk.synctime.ms", 2000))

    self.auto_commit = props.get("auto_commit", False)
    self.auto_commit_interval_ms = int(props.get('auto_commit.interval.ms', 10)) # XXX seconds really
    self.backoff_increment_ms = int(props.get("backoff.increment.ms", 1000))

    self.max_queued_chunks = int(props.get("queuedchunks.max", 100))

    self.fetch_size = int(props.get("fetch.size", 300 * 1024))

    # XXX
    self.group_id = props.get("group_id", 'group1')
