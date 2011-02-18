import struct
import time

import kafka.io
import kafka.request_type

class Consumer(kafka.io.IO):

  CONSUME_REQUEST_TYPE = kafka.request_type.FETCH

  MAX_SIZE = 1024 * 1024

  # seconds.
  DEFAULT_POLLING_INTERVAL = 2

  def __init__(self, topic, partition=0, host='localhost', port=9092):
    kafka.io.IO.__init__(self, host, port)

    #: The topic queue to consume.
    self.topic        = topic

    #: The partition the topic queue is on.
    self.partition    = partition

    #: Offset in the Kafka queue in bytes?
    self.offset       = 0

    #: Maximum message size to consume.
    self.max_size     = self.MAX_SIZE
    self.request_type = self.CONSUME_REQUEST_TYPE
    self.polling      = self.DEFAULT_POLLING_INTERVAL

    self.connect()

  def consume(self):
    """ Consume data from the topic queue. """

    self.send_consume_request()

    return self.parse_message_set_from(self.read_data_response())

  def loop(self):
    """ Loop over incoming messages from the queue in a blocking fashion. Set `polling` for the check interval in seconds. """

    while True:
      messages = self.consume()

      if messages and isinstance(messages, list) and len(messages) > 0:
        for message in messages:
          yield message

      time.sleep(self.polling)

  # REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
  def request_size(self):
    return 2 + 2 + len(self.topic) + 4 + 8 + 4

  def encode_request_size(self):
    return struct.pack('>i', self.request_size())

  def encode_request(self):
    length = len(self.topic)

    return struct.pack('>HH%dsiQi' % length, self.request_type, length, self.topic, self.partition, self.offset, self.max_size)

  def send_consume_request(self):
    self.write(self.encode_request_size())
    self.write(self.encode_request())

  def read_data_response(self):
    buf_length = struct.unpack('>i', self.read(4))[0]

    # Start with a 2 byte offset
    return self.read(buf_length)[2:]

  def parse_message_set_from(self, data):
    messages  = []
    processed = 0
    length    = len(data) - 4

    while (processed <= length):
      message_size = struct.unpack('>i', data[processed:processed+4])[0]
      messages.append(kafka.message.parse_from(data[processed:processed + message_size + 4]))
      processed += 4 + message_size

    self.offset += processed

    return messages
