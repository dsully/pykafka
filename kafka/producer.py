import contextlib
import itertools
import struct

import kafka.io
import kafka.request_type

class Producer(kafka.io.IO):
  """ Class for sending data to a `Kafka <http://sna-projects.com/kafka/>`_ broker. """

  PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

  def __init__(self, topic, partition=0, host='localhost', port=9092):
    kafka.io.IO.__init__(self, host, port)

    self.topic     = topic
    self.partition = partition

    self.connect()

  def encode_request(self, messages):
    """ Encode a sequence of :class:`Message <kafka.message>` objects for sending to the broker. """

    # encode messages as <LEN: int><MESSAGE_BYTES>
    encoded = [message.encode() for message in messages]
    lengths = [len(em) for em in encoded]

    # Build up the struct format.
    mformat = '>' + ''.join(['i%ds' % l for l in lengths])

    # Flatten the two lists to match the format.
    message_set = struct.pack(mformat, *list(itertools.chain.from_iterable(zip(lengths, encoded))))

    topic_len = len(self.topic)
    mset_len  = len(message_set)

    # Create the request as <REQUEST_SIZE: int> <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
    pformat   = '>HH%dsii%ds' % (topic_len, mset_len)
    payload   = struct.pack(pformat, self.PRODUCE_REQUEST_ID, topic_len, self.topic, self.partition, mset_len, message_set)

    return struct.pack('>i%ds' % len(payload), len(payload), payload)

  def send(self, messages):
    """ Send a :class:`Message <kafka.message>` or a sequence of `Messages` to the Kafka server. """

    if isinstance(messages, kafka.message.Message):
      messages = [messages]

    return self.write(self.encode_request(messages))

  @contextlib.contextmanager
  def batch(self):
    """ Send messages with an implict `send`. """

    messages = []
    yield(messages)
    self.send(messages)
