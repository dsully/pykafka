# The fetcher is a background thread that fetches data from a set of servers

import collections
import logging

import kafka.fetcher_runnable

log = logging.getLogger(__name__)

class Fetcher(object):

  def __init__(self, config, zkclient):
    self.config = config
    self.zkclient = zkclient

    self.fetcher_threads = list()
    self.current_topic_infos = list()

  def shutdown(self):
    """ Shutdown all fetch threads. """

    # shutdown the old fetcher threads, if any
    for thread in self.fetcher_threads:
      thread.shutdown()
    self.fetcher_threads = list()

  def clear_all_queues(self, topic_infos):
    for entry in topic_infos:
      entry.chunk_queue.clear()

  def init_connections(self, topic_infos, cluster):
    """ Open connections. """

    self.shutdown()

    if not topic_infos:
      return

    if len(self.current_topic_infos) > 0:
      self.clear_all_queues(self.current_topic_infos)

    self.current_topic_infos = topic_infos

    # re-arrange by broker id
    broker_map = collections.defaultdict(list)

    for info in topic_infos:
      broker_map[info.broker_id].append(info)

    # Open a new fetcher thread for each broker.
    ids     = set([n.broker_id for n in topic_infos])
    brokers = [cluster.get(n) for n in ids]

    for i, broker in enumerate(brokers):

      thread = kafka.fetcher_runnable.FetcherRunnable("FetchRunnable-%s" % i, self.zkclient, self.config, broker, broker_map[broker.id])
      thread.start()

      self.fetcher_threads.append(thread)
