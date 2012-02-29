import struct

import kafka.api

def read_from(buf):
  # Skip the request type, start at the second byte.
  length  = struct.unpack('>H', buf[2:4])[0]
  topic   = struct.unpack('>%ds' % length, buf[4:4+length])[0]
  payload = struct.unpack('>iQi', buf[4+length:])

  return FetchRequest(topic, *payload)

class FetchRequest(object):

  def __init__(self, topic, partition, offset, max_size):
    self.topic     = topic
    self.partition = int(partition)
    self.offset    = long(offset)
    self.max_size  = int(max_size)

  # REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
  def request_size(self):
    return 2 + 2 + len(self.topic) + 4 + 8 + 4

  def encode_request(self):
    length  = len(self.topic)
    packfmt = '>HH%dsiQi' % length

    return struct.pack(packfmt, kafka.api.FETCH, length, self.topic, self.partition, self.offset, self.max_size)

  def __str__(self):
    return "topic: %s, part: %d, offset: %d, max_size: %d" % (self.topic, self.partition, self.offset, self.max_size)
