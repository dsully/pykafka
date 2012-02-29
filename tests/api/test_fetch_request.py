import struct
import time
import unittest

import kafka.api.fetch

class TestFetchRequest(unittest.TestCase):

  def setUp(self):
    self.topic     = 'group1'
    self.partition = 1
    self.offset    = 4
    self.max_size  = 1024
    self.request   = kafka.api.fetch.FetchRequest(self.topic, self.partition, self.offset, self.max_size)

  def test_size_in_bytes(self):
    self.assertEqual(self.request.request_size(), 26)

  def test_round_trip(self):
    buf = self.request.encode_request()
    req = kafka.api.fetch.read_from(buf)

    self.assertEqual(req.topic, self.topic)
    self.assertEqual(req.partition, self.partition)
    self.assertEqual(req.offset, self.offset)
    self.assertEqual(req.max_size, self.max_size)
