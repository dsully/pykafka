def create_broker(id, broker_info_string):
  return Broker(id, *broker_info_string.split(":"))

class Broker(object):

  """ A Kafka Broker. """

  def __init__(self, id, creator_id, host, port):
    self.id         = int(id)
    self.creator_id = creator_id
    self.host       = host
    self.port       = int(port)

  def __str__(self):
    return "id: %s, creator_id: %s, host: %s, port: %d" % (self.id, self.creator_id, self.host, self.port)

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return self.id == other.id and self.host == other.host and self.port == other.port
    return False

  def zk_string(self):
    return '%s:%s:%s' % (self.id, self.host, self.port)

  # override def hashCode(): Int = Utils.hashcode(id, host, port)

class Cluster(object):
  """ The set of active brokers in the cluster. """

  def __init__(self, broker_list=None):
    self.brokers = dict()

    if broker_list:
      for broker in broker_list:
        self.brokers[broker.id] = broker

  def get(self, id):
    return self.brokers.get(int(id))

  def add(self, broker):
    self.brokers[broker.id] = broker

  def remove(self, id):
    try:
      del self.brokers[int(id)]
    except KeyError:
      pass

  def size(self):
    return len(self.brokers.keys())

  def __str__(self):
    return "Cluster(%s)" % ",".join([str(x) for x in self.brokers.values()])

def parse_partition(string):
  parts = string.split("-")

  if len(parts) != 2:
    raise RuntimeError("Expected name in the form x-y.")

  return Partition(*parts)

class Partition(list):

  def __init__(self, broker_id=1, part_id=1):
    self.broker_id = int(broker_id)
    self.part_id   = int(part_id)

  @property
  def name(self):
    return '%d-%d' % (self.broker_id, self.part_id)

  def __str__(self):
    return self.name

  def compare(self, other):
    if self.broker_id == other.broker_id:
      return self.part_id - other.part_id
    else:
      return self.broker_id - other.broker_id
