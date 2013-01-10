import socket
import time
import unittest
from StringIO import StringIO

from fudge import patch

from kafka import message
from kafka import producer


class TestProducers(unittest.TestCase):
  """Tests kafka producers."""

  @patch('socket.socket.connect', 'kafka.io.IO.write')
  def test_producer(self, fake_connect, fake_write):
    """Test the basic kafka producer."""
    expected_msg = ('\x00\x00\x00$\x00\x00\x00\tFakeTopic\x00\x00\x00\x00'
                    '\x00\x00\x00\x0f\x00\x00\x00\x0b\x00\xcf\x02\xbb\\abc123')
    fake_socket = fake_connect.expects_call().with_args(('localhost', 1234)).returns_fake()
    fake_write.expects_call().with_args(expected_msg).returns(len(expected_msg))
    p = producer.Producer('FakeTopic', host='localhost', port=1234)
    p.send(message.Message('abc123'))


  @patch('socket.socket.connect', 'kafka.io.IO.write')
  def test_batch_producer(self, fake_connect, fake_write):
    """Test the batch kafka producer with a batch interval."""
    # Keep this small to keep test cases quick!
    batch_interval = 0.5
    expected_msg_one = ('\x00\x00\x00B\x00\x00\x00\tFakeTopic\x00\x00\x00\x00\x00'
                        '\x00\x00-\x00\x00\x00\x0b\x00\xcf\x02\xbb\\abc123\x00\x00'
                        '\x00\x0b\x00#\xd1\xa6ndef456\x00\x00\x00\x0b\x00"\xe1\xdd'
                        '\xa2ghi789')
    expected_msg_two = ('\x00\x00\x00$\x00\x00\x00\tFakeTopic\x00\x00\x00\x00\x00'
                        '\x00\x00\x0f\x00\x00\x00\x0b\x00\xf0\xb69\xb8jkl123')
    fake_socket = fake_connect.expects_call().with_args(('localhost', 1234)).returns_fake()
    fake_write.expects_call().with_args(expected_msg_one).returns(len(expected_msg_one)).next_call().with_args(expected_msg_two).returns(len(expected_msg_two))
    p = producer.BatchProducer('FakeTopic', batch_interval, host='localhost', port=1234)
    p.enqueue(message.Message('abc123'))
    p.enqueue(message.Message('def456'))
    p.enqueue(message.Message('ghi789'))
    time.sleep(batch_interval)
    p.enqueue(message.Message('jkl123'))
    p.close()
