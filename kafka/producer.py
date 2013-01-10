import atexit
import contextlib
import itertools
import struct
import threading

import kafka.io
import kafka.request_type


class Producer(kafka.io.IO):
  """Class for sending data to a `Kafka <http://sna-projects.com/kafka/>`_ broker.

    :param topic: The topic to produce to.
    :param partition: The broker partition to produce to.
    :param host: The kafka host.
    :param port: The kafka port.
    :param max_message_sz: The maximum allowed size of a produce request (in bytes). [default: 1MB]

  """

  PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

  def __init__(self, topic, partition=0, host='localhost', port=9092, max_message_sz=1048576):
    kafka.io.IO.__init__(self, host, port)
    self.max_message_sz = max_message_sz
    self.topic = topic
    self.partition = partition
    self.connect()

  def _pack_payload(self, messages):
    """Pack a list of messages into a sendable buffer.

      :param msgs: The packed messages to send.
      :param size: The size (in bytes) of all the `messages` to send.

    """
    payload = ''.join(messages)
    payload_sz = len(payload)
    topic_sz = len(self.topic)
    # Create the request as::
    #   <REQUEST_ID: short>
    #   <TOPIC_SIZE: short>
    #   <TOPIC: bytes>
    #   <PARTITION: int>
    #   <BUFFER_SIZE: int>
    #   <BUFFER: bytes>
    return struct.pack(
      '>HH%dsii%ds' % (topic_sz, payload_sz),
      self.PRODUCE_REQUEST_ID,
      topic_sz,
      self.topic,
      self.partition,
      payload_sz,
      payload
    )

  def _pack_kafka_message(self, payload):
    """Pack a payload in a format kafka expects."""
    return struct.pack('>i%ds' % len(payload), len(payload), payload)

  def encode_request(self, messages):
    """Encode a sequence of messages for sending to a kafka broker.

      Encoding a request can yeild multiple kafka messages if the payloads exceed
      the maximum produce size.

      :param messages: An iterable of :class:`Message <kafka.message>` objecjts.
      :rtype: A generator of packed kafka messages ready for sending.

    """
    encoded_msgs = []
    encoded_msgs_sz = 0
    for message in messages:
      encoded = message.encode()
      encoded_sz = len(encoded)
      if encoded_sz + encoded_msgs_sz > self.max_message_sz:
        yield self._pack_kafka_message(self._pack_payload(encoded_msgs))
        encoded_msgs = []
        encoded_msgs_sz = 0
      msg = struct.pack('>i%ds' % encoded_sz, encoded_sz, encoded)
      encoded_msgs.append(msg)
      encoded_msgs_sz += encoded_sz
    if encoded_msgs:
      yield self._pack_kafka_message(self._pack_payload(encoded_msgs))

  def send(self, messages):
    """Send a :class:`Message <kafka.message>` or a sequence of `Messages` to the Kafka server."""
    if isinstance(messages, kafka.message.Message):
      messages = [messages]
    for message in self.encode_request(messages):
      sent = self.write(message)
      if sent != len(message):
        raise IOError('Failed to send kafka message - sent %s/%s many bytes.' % (sent, len(message)))

  @contextlib.contextmanager
  def batch(self):
    """Send messages with an implict `send`."""
    messages = []
    yield(messages)
    self.send(messages)


class BatchProducer(Producer):
  """Class for batching messages to a `Kafka <http://sna-projects.com/kafka/>`_ broker with periodic flushing.

    :param topic: The topic to produce to.
    :param batch_interval: The amount of time (in seconds) to wait for messages before sending.
    :param partition: The broker partition to produce to.
    :param host: The kafka host.
    :param port: The kafka port.

  """

  PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

  def __init__(self, topic, batch_interval, partition=0, host='localhost', port=9092):
    Producer.__init__(self, topic, partition=partition, host=host, port=port)
    self.batch_interval = batch_interval
    self._message_queue = []
    self.event = threading.Event()
    self.lock = threading.Lock()
    self.timer = threading.Thread(target=self._interval_timer)
    self.timer.daemon = True
    self.timer.start()
    self.connect()
    atexit.register(self.close)

  def _interval_timer(self):
    """Flush the message queue every `batch_interval` seconds."""
    while not self.event.is_set():
      self.flush()
      self.event.wait(self.batch_interval)

  def enqueue(self, message):
    """Enqueue a message in the queue.

      .. note:: These messages are implicitly sent every `batch_interval` seconds.

      :param message: The message to queue for sending at next send interval.

    """
    with self.lock:
      self._message_queue.append(message)

  def flush(self):
    """Send all messages in the queue now."""
    with self.lock:
      if len(self._message_queue) > 0:
        self.send(self._message_queue)
        # Reset the queue
        del self._message_queue[:]

  def close(self):
    """Shutdown the timer thread and flush the message queue."""
    self.event.set()
    self.flush()
    self.timer.join()
