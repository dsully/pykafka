import struct

import kafka.api
import kafka.exceptions

SMALLEST_TIME_STRING = "smallest"
LARGEST_TIME_STRING  = "largest"
LATEST_TIME          = -1L
EARLIEST_TIME        = -2L

def read_from(buf):
  # Skip the request type, start at the second byte.
  length  = struct.unpack('>H', buf[2:4])[0]
  topic   = struct.unpack('>%ds' % length, buf[4:4+length])[0]
  payload = struct.unpack('>iQi', buf[4+length:])

  return OffsetRequest(topic, *payload)

def deserialize_offset_array(array):
  length  = struct.unpack('>i', array[0:4])[0]
  offsets = list()

  for i in xrange(length):
    buf_offset = 4+(i*8)
    offsets[i] = struct.unpack('>Q', array[buf_offset:buf_offset+8])[0]

  return offsets

class OffsetRequest(object):

  def __init__(self, topic, partition, time, max_num_offsets):
    self.request_type    = kafka.api.OFFSETS
    self.topic           = topic
    self.partition       = partition
    self.time            = int(time)
    self.max_num_offsets = max_num_offsets

  def request_size(self):
    return 2 + len(self.topic) + 4 + 8 + 4

  def encode_request(self):
    length  = len(self.topic)
    packfmt = '>HH%dsiQi' % length

    return struct.pack(packfmt, self.request_type, length, self.topic, self.partition, self.time, self.max_num_offsets)

  def __str__(self):
    return "topic: %s partition: %s time: %s max_num_offsets: %s" % (self.topic, self.partition, self.time, self.max_num_offsets)
