import struct
import unittest

import sys
sys.path.append('..')

import kafka

class TestMessage(unittest.TestCase):

  def setUp(self):
    self.message = kafka.message.Message()

  def test_default_magic_number(self):
    self.assertEqual(kafka.message.Message.MAGIC_IDENTIFIER_DEFAULT, 0)

  def test_magic_field_checksum_paload(self):
    self.assertTrue(hasattr(self.message, 'magic'))
    self.assertTrue(hasattr(self.message, 'checksum'))
    self.assertTrue(hasattr(self.message, 'payload'))

  def test_default_magic_value(self):
    self.assertEqual(self.message.magic, kafka.message.Message.MAGIC_IDENTIFIER_DEFAULT)

  def test_set_magic_value(self):
    self.message = kafka.message.Message("foo", 1)
    self.assertTrue(self.message.magic, 1)

  def test_payload_checksum(self):
    self.message.payload = 'kafka'
    self.assertTrue(self.message.calculate_checksum(), 1539077399)

  def test_message_crc32(self):
    self.message.payload = 'kafka'
    self.message.checksum = 1539077399

    self.assertTrue(self.message.is_valid())

    self.message.checksum = 0
    self.assertTrue(not self.message.is_valid())

    self.message = kafka.message.Message("kafka", 0, 66666666)
    self.assertTrue(not self.message.is_valid())

  def test_message_parsing(self):
    payload = 'ale'
    bytes = struct.pack('>iBi%ds' % len(payload), 12, 0, 1120192889, payload)

    message = kafka.message.parse_from(bytes)

    self.assertTrue(message.is_valid())
    self.assertEqual(message.magic, 0)
    self.assertEqual(message.checksum, 1120192889)
    self.assertEqual(message.payload, payload)
