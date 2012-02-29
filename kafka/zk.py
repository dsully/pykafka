from __future__ import absolute_import

import collections
import logging
import os
import threading
import time

import zookeeper

log = logging.getLogger('zk')

ZOO_OPEN_ACL_UNSAFE = {
  "perms" : zookeeper.PERM_ALL,
  "scheme": "world",
  "id"    : "anyone"
}

PERSISTENT = 0

#zookeeper.set_debug_level(zookeeper.LOG_LEVEL_DEBUG)
zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)

DEFAULT_TIMEOUT = 30000

class ClientError(Exception):
  pass

class Client(object):

  def __init__(self, servers, timeout=DEFAULT_TIMEOUT):
    self.timeout   = timeout
    self.connected = False
    self.conn_cv   = threading.Condition()

    self.conn_cv.acquire()

    log.info("Connecting to: %s", servers)

    start = time.time()

    self.handle = zookeeper.init(servers, self.connection_watcher, timeout)

    self.watchers_for_path = collections.defaultdict(set)
    self.watchers_lock     = threading.RLock()

    self.conn_cv.wait(timeout/1000)
    self.conn_cv.release()

    if not self.connected:
      raise ClientError("Unable to connect to %s" % servers)

    log.info("Connected in %d ms, handle is %d", int((time.time() - start) * 1000), self.handle)

  def connection_watcher(self, handle, type, state, path):
    self.handle = handle
    self.conn_cv.acquire()
    self.connected = True
    self.conn_cv.notifyAll()
    self.conn_cv.release()

  def close(self):
    return zookeeper.close(self.handle)

  def create(self, path, data="", mode=PERSISTENT, acl=None, create_parents=False):
    start  = time.time()
    result = None

    if acl is None:
      acl = [ZOO_OPEN_ACL_UNSAFE]

    try:
      log.info("Trying to create node: %s", path)

      result = zookeeper.create(self.handle, path, data, acl, mode)

    except zookeeper.NodeExistsException, e:
      if create_parents is False:
        log.warn(e)
        raise e

    except zookeeper.NoNodeException, e:
      if create_parents is False:
        log.warn(e)
        raise e

      parent_dir = os.path.dirname(path)
      self.create(parent_dir, data, mode, acl, create_parents)
      self.create(path, data, mode, acl, create_parents)

    log.info("Node %s created in %d ms", path, int((time.time() - start) * 1000))

    return result

  def create_ephemeral(self, path, data="", acl=None):
    return self.create(path, data, zookeeper.EPHEMERAL, acl)

  def make_sure_persistent_path_exists(self, path):
    """ Make sure a persistent path exists in ZK. Create the path if not exist. """

    if not self.exists(path):
      try:
        self.create(path, create_parents=True)
      except zookeeper.NodeExistsException:
        pass

  def create_parent_path(self, path):
    """ Create a parent path. """

    parent_dir = os.path.dirname(path)
    print "create_parent_path: path: %s parent_dir: %s" % (path, parent_dir)

    if parent_dir != '/':
      self.create(parent_dir, create_parents=True)

  def create_ephemeral_path(self, path, data):
    """ Create an ephemeral node with the given path and data. Create parents if necessary. """

    try:
      self.create_ephemeral(path, data)
    except zookeeper.NoNodeException:
      self.create_parent_path(path)
      self.create_ephemeral(path, data)

  def create_ephemeral_path_expect_conflict(self, path, data=""):
    """
      Create an ephemeral node with the given path and data.
      Raises NodeExistException if the node already exists.
    """

    try:
      self.create_ephemeral_path(path, data)

    except zookeeper.NodeExistsException, nee:
      # This can happen when there is connection loss; make sure the data is what we intend to write
      stored_data = None

      try:
        stored_data = self.get(path)
      except zookeeper.NoNodeException:
        # The node disappeared; treat as if node existed and let caller handles this.
        pass

      if stored_data is None or stored_data != data:
        log.info("Conflict in %s data: %s stored_data: %s", path, data, stored_data)
        raise nee
      else:
        # Otherwise, the creation succeeded, return normally
        log.info("%s exists with value: %s during connection loss; this is ok", path, data)

  def update_persistent_path(self, path, data=""):
    """
      Update the value of a persistent node with the given path and data.
      Create parrent directory if necessary. Never throw NodeExistException.
    """

    try:
      self.set(path, data)
    except zookeeper.NoNodeException:
      self.create_parent_path(path)
      self.create(path, data)

  def update_ephemeral_path(self, path, data=""):
    """
      Update the value of a persistent node with the given path and data.
      Create parrent directory if necessary. Never throw NodeExistException.
    """

    try:
      self.set(path, data)
    except zookeeper.NoNodeException:
      self.create_parent_path(path)
      self.create_ephemeral(path, data)

  def delete(self, path, version=-1):
    try:
      zookeeper.delete(self.handle, path, version)

    except zookeeper.NoNodeException:
      # This can happen during a connection loss event, return normally
      log.info("%s deleted during connection loss; this is ok", path)

  def delete_recursive(self, path, version=-1):

    while True:
      try:

        zookeeper.delete(path, version)
        log.info("Deleted path: %s", path)
        return True

      except zookeeper.NotEmptyException:

        for child in self.get_children():
          self.delete_recursive('/'.join(filter(lambda i: i != None and i != '', [path, child])))

      except zookeeper.NoNodeException:

        log.info("%s deleted during connection loss; this is ok", path)
        return True

  def subscribe_to_child_changes(self, path, watcher):
    with self.watchers_lock:

      self.watchers_for_path[path].add(watcher)

    return self.watch_for_children(path)

  def unsubscribe_to_child_changes(self, path, watcher):
    with self.watchers_lock:
      self.watchers_for_path[path].remove(watcher)

  def watch_for_children(self, path):
    """ Installs a child watch for the given path. """

    self.exists(path, True)

    try:
      return self.get_children(path, True)
    except zookeeper.NoNodeException:
      pass

    return []

  def get_maybe_none(self, path, watcher=None):
    try:
      return self.get(path, watcher)[0]
    except zookeeper.NoNodeException:
      return None

  def get(self, path, watcher=None):
    return zookeeper.get(self.handle, path, watcher)

  def exists(self, path, watcher=None):
    return zookeeper.exists(self.handle, path, watcher)

  def set(self, path, data="", version=-1):
    return zookeeper.set(self.handle, path, data, version)

  def set2(self, path, data="", version=-1):
    return zookeeper.set2(self.handle, path, data, version)

  def get_children(self, path, watcher=None):
    return zookeeper.get_children(self.handle, path, watcher)

  def async(self, path = "/"):
    return zookeeper.async(self.handle, path)

  def acreate(self, path, callback, data="", mode=PERSISTENT, acl=None):
    if acl is None:
      acl = [ZOO_OPEN_ACL_UNSAFE]

    return zookeeper.acreate(self.handle, path, data, acl, mode, callback)

  def adelete(self, path, callback, version=-1):
    return zookeeper.adelete(self.handle, path, version, callback)

  def aget(self, path, callback, watcher=None):
    return zookeeper.aget(self.handle, path, watcher, callback)

  def aexists(self, path, callback, watcher=None):
    return zookeeper.aexists(self.handle, path, watcher, callback)

  def aset(self, path, callback, data="", version=-1):
    return zookeeper.aset(self.handle, path, data, version, callback)

  def aget_children(self, path, callback, watcher=None):
    return zookeeper.aset(self.handle, path, watcher, callback)
