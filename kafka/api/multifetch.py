import struct
import time

import kafka.api

def read_from(buf):
  # Skip the request type, start at the second byte.
  count   = struct.unpack('>H', buf[2:4])[0]
  fetches = []

  for i in xrange(count):
    # XXX - won't quite work. Since read_from skips over first 2 bytes.
    fetches.append(kafka.api.fetch.read_from(buf))

  return MultiFetchRequest(fetches)

class MultiFetchRequest(object):

  def __init__(self, requests):
    self.requests = requests

  def request_size(self):
    return 2 + 2 + sum([f.request_size() for f in self.requests])

  def encode_request(self):
    buf = struct.pack('>H', len(self.requests))

    for request in self.requests:
      buf += request.encode_request()

    return struct.pack(format, kafka.api.FETCH, length, self.topic, self.partition, self.offset, self.max_size)

  def __str__(self):
    return ",".join([str(f) for f in self.fetches])

class MultiFetchResponse(object):

  def __init__(self, buf, num_sets):
    self.buf      = buf
    self.message_sets = []

    for i in xrange(int(num_sets)):

      length = struct.unpack('>i', buf[2:4])[0]
      error  = struct.unpack('>H', buf[4:5])[0]

      #val copy = buffer.slice()
      #val payloadSize = size - 2
      #copy.limit(payloadSize)
      #buffer.position(buffer.position + payloadSize)
      #messageSets += new ByteBufferMessageSet(copy, errorCode)

  def __str__(self):
    return str(self.message_sets)
