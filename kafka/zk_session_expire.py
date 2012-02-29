import logging

log = logging.getLogger(__name__)

MAX_N_RETRIES = 4

class ZKSessionExpireListener(object): #XXX IZkStateListener):

  def __init__(self, dirs, consumer_id, topic_count, load_balancer_listener):
    self.dirs = dirs
    self.consumer_id = consumer_id
    self.topic_count = topic_count
    self.load_balancer_listener = load_balancer_listener

  def handle_state_changed(self, state):
    # Do nothing, since self.zkclient will do reconnect for us.
    pass

  # Called after the zookeeper session has expired and a new session has
  # been created. You would have to re-create any ephemeral nodes here.
  def handle_new_session(self):
    """
      When we get a SessionExpired event, we lost all ephemeral nodes and self.zkclient has reestablished a
      connection for us. We need to release the ownership of the current consumer and re-register this
      consumer in the consumer registry and trigger a rebalance.
    """

    log.info("ZK expired; release old broker parition ownership; re-register consumer %s", self.consumer_id)

    self.load_balancer_listener.reset_state()

    register_consumer_in_zk(self.dirs, self.consumer_id, self.topic_count)

    # explicitly trigger load balancing for this consumer
    self.load_balancer_listener.synced_rebalance()

    # There is no need to resubscribe to child and state changes.
    # The child change watchers will be set inside rebalance when we read the children list.
