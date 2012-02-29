import struct
import unittest

import socket
import threading
import SocketServer

import sys
sys.path.append('..')

import kafka.io

class TCPHandler(SocketServer.BaseRequestHandler):
  def handle(self):
    self.data = self.request.recv(1024).strip()
    self.request.send(self.data)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
  # Exit the server thread when the main thread terminates
  daemon_threads = True
  allow_reuse_address = True

class IOTest(kafka.io.IO):
  pass

HOST = 'localhost'
PORT = 9093

server = ThreadedTCPServer((HOST, PORT), TCPHandler)
server_thread = threading.Thread(target=server.serve_forever)
server_thread.start()

class TestIO(unittest.TestCase):

  def setUp(self):
    self.io = IOTest(HOST, PORT)
    self.io.connect()

  def test_attributes(self):
    self.assertTrue(hasattr(self.io, 'host'))
    self.assertTrue(hasattr(self.io, 'port'))
    self.assertTrue(hasattr(self.io, 'socket'))

  def test_remember_host_and_port_on_conncet(self):
    self.io.connect()
    self.assertEqual(self.io.host, HOST)
    self.assertEqual(self.io.port, PORT)

  def test_socket_write(self):
    data = "some data"

    self.assertEqual(self.io.host, HOST)
    self.assertEqual(self.io.port, PORT)

    self.assertEqual(self.io.write(data), 9)

  def test_socket_read(self):
    length = 200
    #self.assertEqual(self.io.read(length), 0)

  def test_disconnect_on_timeout_when_reading_from_socket(self):
    length = 200
    #@mocked_socket.should_receive(:read).with(length).and_raise(Errno::EAGAIN)
    #self.io.should_receive(:disconnect)
    #lambda { self.io.read(length) }.should raise_error(Errno::EAGAIN)

  def test_disconnect(self):
    self.io.disconnect()
    self.assertTrue(self.io.socket is None)

  def test_reconncet(self):
    self.io.reconnect()
    self.assertTrue(self.io.socket)

  def test_reconnect_on_broken_pipe_errno(self):
    pass
    #[Errno::ECONNABORTED, Errno::EPIPE, Errno::ECONNRESET].each do |error|
    #  @mocked_socket.should_receive(:write).exactly(:twice).and_raise(error)
    #  @mocked_socket.should_receive(:close).exactly(:once).and_return(nil)
    #  lambda {
    #    self.io.write("some data to send")
    #  }.should raise_error(error)
