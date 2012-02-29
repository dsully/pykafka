class FetchedDataChunk(object):

  def __init__(self, messages, topic_info):
    self.messages   = messages
    self.topic_info = topic_info

class PartitionTopicInfo(object):

  def __init__(self, topic, broker_id, partition, chunk_queue, consumed_offset, fetched_offset, fetch_size):
    self.topic           = topic
    self.broker_id       = int(broker_id)
    self.partition       = partition
    self.chunk_queue     = chunk_queue
    self.consumed_offset = consumed_offset
    self.fetched_offset  = fetched_offset
    self.fetch_size      = fetch_size

  def consumed(self, message_size):
    """ Record the given number of bytes as having been consumed. """

    # XXX(thread lock?)
    self.consumed_offset += message_size

  def enqueue(self, messages):
    """
      Enqueue a message set for processing.
      :return: The number of valid bytes
    """

    size = 0
    #val size = messages.validBytes
    #ByteBufferMessageSet

    if size > 0:
      self.fetched_offset += size

      # XXX - chunk_queue is a BlockingQueue in Java/Scala
      self.chunk_queue.append(FetchedDataChunk(messages, self))

    return size

  def enqueue_error(self, error):
    """ Add an empty message with the exception to the queue so that client can see the error. """

    messages = [] #new ByteBufferMessageSet(ErrorMapping.EMPTY_BYTEBUFFER, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))

    self.chunk_queue.append(FetchedDataChunk(messages, self))

  def __str__(self):
    return '%s:%s' % (self.topic, self.partition.name)
