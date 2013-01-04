import atexit
import contextlib
import itertools
import struct
import threading

import kafka.io
import kafka.request_type

class Producer(kafka.io.IO):
  """Class for sending data to a `Kafka <http://sna-projects.com/kafka/>`_ broker."""

  PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

  def __init__(self, topic, partition=0, host='localhost', port=9092):
    """Setup a kafka producer.

      :param topic: The topic to produce to.
      :param partition: The broker partition to produce to.
      :param host: The kafka host.
      :param port: The kafka port.

    """
    kafka.io.IO.__init__(self, host, port)
    self.topic = topic
    self.partition = partition
    self.connect()

  def encode_request(self, messages):
    """Encode a sequence of messages for sending to the broker.

      :param messages: A sequence of :class:`Message <kafka.message>` objects.

    """
    # encode messages as::
    #   <LEN: int>
    #   <MESSAGE_BYTES>
    encoded = [message.encode() for message in messages]
    lengths = [len(em) for em in encoded]

    # Build up the struct format.
    mformat = '>' + ''.join(['i%ds' % l for l in lengths])

    # Flatten the two lists to match the format.
    message_set = struct.pack(mformat, *list(itertools.chain.from_iterable(zip(lengths, encoded))))

    # Create the request as::
    #   <REQUEST_SIZE: int>
    #   <REQUEST_ID: short>
    #   <TOPIC: bytes>
    #   <PARTITION: int>
    #   <BUFFER_SIZE: int>
    #   <BUFFER: bytes>
    topic_len = len(self.topic)
    mset_len = len(message_set)
    pformat = '>HH%dsii%ds' % (topic_len, mset_len)
    payload = struct.pack(pformat, self.PRODUCE_REQUEST_ID, topic_len, self.topic, self.partition, mset_len, message_set)
    return struct.pack('>i%ds' % len(payload), len(payload), payload)

  def send(self, messages):
    """Send a :class:`Message <kafka.message>` or a sequence of `Messages` to the Kafka server."""
    if isinstance(messages, kafka.message.Message):
      messages = [messages]
    return self.write(self.encode_request(messages))

  @contextlib.contextmanager
  def batch(self):
    """Send messages with an implict `send`."""
    messages = []
    yield(messages)
    self.send(messages)


class BatchProducer(Producer):
  """Class for batching messages to a `Kafka <http://sna-projects.com/kafka/>`_ broker with periodic flushing."""

  PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

  def __init__(self, topic, batch_interval, partition=0, host='localhost', port=9092):
    """Setup a batch kafka producer.

      :param topic: The topic to produce to.
      :param batch_interval: The amount of time (in seconds) to wait for messages before sending.
      :param partition: The broker partition to produce to.
      :param host: The kafka host.
      :param port: The kafka port.

    """
    Producer.__init__(self, topic, partition=partition, host=host, port=port)
    self.batch_interval = batch_interval
    self._message_queue = []
    self.event = threading.Event()
    self.lock = threading.Lock()
    self.timer = threading.Thread(target=self._interval_timer)
    self.timer.daemon = True
    self.connect()
    self.timer.start()
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
