import json

def construct(consumer_id, string):
  top = None

  try:
    top = json.loads(string)
  except StandardError:
    raise StandardError("Error constructing TopicCount from: %s" % str(string))

  return TopicCount(consumer_id, top)

class TopicCount(object):

  def __init__(self, consumer_id, top):
    self.id  = consumer_id
    self.map = top

  def consumer_thread_ids_per_topic(self):
    topics = dict()

    for topic, num_consumers in self.map.iteritems():
      consumer_set = set()

      for i in xrange(num_consumers):
        consumer_set.add('%s-%s' % (self.id, i))

      topics[topic] = consumer_set

    return topics

  def __eq__(self, other):
    return self.id == other.id and self.map == other.map

  def to_json(self):
    return json.dumps(self.map)
