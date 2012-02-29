import logging
import threading
import time

import kafka.api
import kafka.consumer
import kafka.exceptions
import kafka.utils

log = logging.getLogger(__name__)

class FetcherRunnable(threading.Thread):

  def __init__(self, name, zkclient, config, broker, partition_topic_infos):
    threading.Thread.__init__(self, name=name)

    self.zkclient = zkclient
    self.config   = config
    self.broker   = broker
    self.partition_topic_infos = partition_topic_infos

    self.latch = threading.Event()

    log.debug("Connecting to Broker: %s:%d", broker.host, broker.port)

    #XXX self.consumer = SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs, config.socketBufferSize)
    self.consumer = kafka.consumer.Consumer("topic", 0, broker.host, broker.port)

    # volatile
    self.stopped = False

  def shutdown(self):
    self.stopped = True

    #XXX - python threading doesn't have interrupt().
    #self.interrupt()

    log.debug("Awaiting shutdown on fetcher %s", self.name)

    self.latch.set()

    log.debug("Shutdown of fetcher %s thread complete.", self.name)

  def run(self):

    for info in self.partition_topic_infos:
      log.info("%s start fetching topic: %s part: %s offset: %d from: %s:%d", \
        self.name, info.topic, info.partition.part_id, info.fetched_offset, self.broker.host, self.broker.port)

    try:
      while not self.stopped:

        fetches = []

        for info in self.partition_topic_infos:
          #XXX - implement multifetch
          #XXX fetches.append(FetchRequest(info.topic, info.partition.part_id, info.fetched_offset, self.config.fetch_size))

          #self.write(self.encode_request_size())
          #self.write(self.encode_request())
          self.consumer.topic = info.topic
          self.consumer.partition = info.partition.part_id
          self.consumer.offset = info.fetched_offset
          self.consumer.max_size = self.config.fetch_size

        log.debug("fetch request: %s", fetches)

        #XXX response = self.consumer.multifetch(fetches)
        response = self.consumer.consume()

        read = 0

        for messages, info in zip(response, self.partition_topic_infos):

          try:
            done = False

            if messages.error_code == kafka.exceptions.ErrorMapping.OFFSET_OUT_OF_RANGE_CODE:

              log.info("offset %d out of range", info.fetched_offset)

              # see if we can fix this error
              if info.fetched_offset.get == info.consumed_offset.get:

                reset_offset = self.reset_consumer_offsets(info.topic, info.partition)

                if reset_offset >= 0:
                  info.fetched_offset.set(reset_offset)
                  info.consumed_offset.set(reset_offset)
                  done = True

            if not done:
              read += info.enqueue(messages)

          except Exception, e:
            if not self.stopped:
              log.error("Error in FetcherRunnable for %s: %s", info, e)
              info.enqueue_error(e)
            raise e

        #logger.trace("fetched bytes: " + read)

        if not read:
          time.sleep(self.config.backoff_increment_ms)

    except Exception, e:
      if self.stopped:
        log.info("FecherRunnable %s interrupted.", self)
      else:
        log.error("Error in FetcherRunnable %s", e)

    log.info("Stopping fetcher %s to host: %s", self.name, self.broker.host)

    try:
      self.consumer.disconnect()
    except Exception, e:
      log.info(e)

    self.shutdown_complete()

  def shutdown_complete(self):
   """ Record that the thread shutdown is complete. """
   self.latch.wait()

  def reset_consumer_offsets(self, topic, partition):
    offset = 0

    if self.config.auto_offset_reset == kafka.api.offset_request.SMALLEST_TIME_STRING:
      offset = kafka.api.offset_request.EARLIEST_TIME
    elif self.config.auto_offset_reset == kafka.api.offset_request.LARGEST_TIME_STRING:
      offset = kafka.api.offset_request.LATEST_TIME
    else:
      return -1

    # get mentioned offset from the broker
    offsets    = self.consumer.get_offsets_before(topic, partition.part_id, offset, 1)
    topic_dirs = kafka.utils.GroupTopicDirs(self.config.group_id, topic)

    # Reset manually in zookeeper
    if offset == kafka.api.offset_request.EARLIEST_TIME:
      offset_string = "earliest"
    else:
      offset_string = "latest"

    log.info("Updating partition %s with %s offset %d", partition.name, offset_string, offsets[0])

    self.zkclient.update_persistent_path(topic_dirs.consumer_offset_dir + "/" + partition.name, str(offsets[0]))

    return offsets[0]
