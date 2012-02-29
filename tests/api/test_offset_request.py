import struct
import time
import unittest

#import sys
#sys.path.append('..')

import kafka.api.offset_request

class TestOffsetRequest(unittest.TestCase):

  def setUp(self):
    self.topic           = 'group1'
    self.partition       = 1
    self.time            = int(time.time())
    self.max_num_offsets = 10
    self.request         = kafka.api.offset_request.OffsetRequest(self.topic, self.partition, self.time, self.max_num_offsets)

  def test_size_in_bytes(self):
    self.assertEqual(self.request.request_size(), 24)

  def test_round_trip(self):
    buf = self.request.encode_request()
    req = kafka.api.offset_request.read_from(buf)

    self.assertEqual(req.topic, self.topic)
    self.assertEqual(req.partition, self.partition)
    self.assertEqual(req.time, self.time)
    self.assertEqual(req.max_num_offsets, self.max_num_offsets)

  def test_deserialize_offset_array(self):
    # XXX - implement test_deserialize_offset_array
    pass
