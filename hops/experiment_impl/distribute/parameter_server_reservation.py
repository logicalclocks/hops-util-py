from __future__ import absolute_import
from __future__ import division
from __future__ import nested_scopes
from __future__ import print_function

import logging
import pickle
import select
import socket
import struct
import threading
import time

from hops import util
from hops.experiment_impl.util import experiment_utils

MAX_RETRIES = 3
BUFSIZE = 1024*2

class Reservations:
  """Thread-safe store for node reservations."""

  def __init__(self, required):
    """

    Args:
        required:
    """
    self.required = required
    self.lock = threading.RLock()
    self.reservations = []
    self.cluster_spec = {}
    self.check_done = False

  def add(self, meta):
    """Add a reservation.

    Args:
        :meta: a dictonary of metadata about a node
    """
    with self.lock:
      self.reservations.append(meta)

      if self.remaining() == 0:

        cluster_spec = {"chief": [], "ps": [], "worker": []}
        added_chief=False
        for entry in self.reservations:
          if entry["task_type"] == "ps":
            cluster_spec["ps"].append(entry["host_port"])
          elif added_chief == False and entry["task_type"] == "worker":
            cluster_spec["chief"].append(entry["host_port"])
            added_chief = True
          else:
            cluster_spec["worker"].append(entry["host_port"])

        self.cluster_spec = cluster_spec

        self.check_done = True

  def done(self):
    """Returns True if the ``required`` number of reservations have been fulfilled."""
    with self.lock:
      return self.check_done

  def get(self):
    """Get the list of current reservations."""
    with self.lock:
      return self.cluster_spec

  def remaining(self):
    """Get a count of remaining/unfulfilled reservations."""
    with self.lock:
      num_registered = len(self.reservations)
      return self.required - num_registered


class WorkerFinished:
  """Thread-safe store for node reservations."""

  def __init__(self, required):
    """

    Args:
        :required: expected number of nodes in the cluster.
    """
    self.required = required
    self.lock = threading.RLock()
    self.finished = 0
    self.check_done = False

  def add(self):
    """Add a reservation.

    Args:
        :meta: a dictonary of metadata about a node
    """
    with self.lock:
      self.finished = self.finished + 1

      if self.remaining() == 0:
        self.check_done = True

  def done(self):
    """Returns True if the ``required`` number of reservations have been fulfilled."""
    with self.lock:
      return self.check_done

  def remaining(self):
    """Get a count of remaining/unfulfilled reservations."""
    with self.lock:
      return self.required - self.finished

class MessageSocket(object):
  """Abstract class w/ length-prefixed socket send/receive functions."""

  def receive(self, sock):
    """
    Receive a message on ``sock``

    Args:
        sock:

    Returns:

    """
    msg = None
    data = b''
    recv_done = False
    recv_len = -1
    while not recv_done:
      buf = sock.recv(BUFSIZE)
      if buf is None or len(buf) == 0:
        raise Exception("socket closed")
      if recv_len == -1:
        recv_len = struct.unpack('>I', buf[:4])[0]
        data += buf[4:]
        recv_len -= len(data)
      else:
        data += buf
        recv_len -= len(buf)
      recv_done = (recv_len == 0)

    msg = pickle.loads(data)
    return msg

  def send(self, sock, msg):
    """
    Send ``msg`` to destination ``sock``.

    Args:
        sock:
        msg:

    Returns:

    """
    data = pickle.dumps(msg)
    buf = struct.pack('>I', len(data)) + data
    sock.sendall(buf)


class Server(MessageSocket):
  """Simple socket server with length prefixed pickle messages"""
  reservations = None
  done = False

  def __init__(self, count):
    """

    Args:
        count:
    """
    assert count > 0
    self.reservations = Reservations(count)
    self.worker_finished = WorkerFinished(util.num_executors() - util.num_param_servers())

  def await_reservations(self, sc, status={}, timeout=600):
    """
    Block until all reservations are received.

    Args:
        sc:
        status:
        timeout:

    Returns:

    """
    timespent = 0
    while not self.reservations.done():
      logging.info("waiting for {0} reservations".format(self.reservations.remaining()))
      # check status flags for any errors
      if 'error' in status:
        sc.cancelAllJobs()
        #sc.stop()
        #sys.exit(1)
      time.sleep(1)
      timespent += 1
      if (timespent > timeout):
        raise Exception("timed out waiting for reservations to complete")
    logging.info("all reservations completed")
    return self.reservations.get()

  def _handle_message(self, sock, msg):
    """

    Args:
        sock:
        msg:

    Returns:

    """
    logging.debug("received: {0}".format(msg))
    msg_type = msg['type']
    if msg_type == 'REG':
      self.reservations.add(msg['data'])
      MessageSocket.send(self, sock, 'OK')
    elif msg_type == 'REG_DONE':
      self.worker_finished.add()
      MessageSocket.send(self, sock, 'OK')
    elif msg_type == 'QUERY':
      MessageSocket.send(self, sock, self.reservations.done())
    elif msg_type == 'QUERY_DONE':
      MessageSocket.send(self, sock, self.worker_finished.done())
    elif msg_type == 'QINFO':
      rinfo = self.reservations.get()
      MessageSocket.send(self, sock, rinfo)
    elif msg_type == 'STOP':
      logging.info("setting server.done")
      MessageSocket.send(self, sock, 'OK')
      self.done = True
    else:
      MessageSocket.send(self, sock, 'ERR')

  def start(self):
    """
    Start listener in a background thread

    Returns:
        address of the Server as a tuple of (host, port)
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('', 0))
    server_sock.listen(10)

    # hostname may not be resolvable but IP address probably will be
    host = experiment_utils._get_ip_address()
    port = server_sock.getsockname()[1]
    addr = (host,port)

    def _listen(self, sock):
      CONNECTIONS = []
      CONNECTIONS.append(sock)

      while not self.done:
        read_socks, write_socks, err_socks = select.select(CONNECTIONS, [], [], 60)
        for sock in read_socks:
          if sock == server_sock:
            client_sock, client_addr = sock.accept()
            CONNECTIONS.append(client_sock)
            logging.debug("client connected from {0}".format(client_addr))
          else:
            try:
              msg = self.receive(sock)
              self._handle_message(sock, msg)
            except Exception as e:
              logging.debug(e)
              sock.close()
              CONNECTIONS.remove(sock)

      server_sock.close()

    t = threading.Thread(target=_listen, args=(self, server_sock))
    t.daemon = True
    t.start()

    return addr

  def stop(self):
    """Stop the Server's socket listener."""
    self.done = True


class Client(MessageSocket):
  """Client to register and await node reservations.

  Args:
      :server_addr: a tuple of (host, port) pointing to the Server.
  """
  sock = None                   #: socket to server TCP connection
  server_addr = None            #: address of server

  def __init__(self, server_addr):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect(server_addr)
    self.server_addr = server_addr
    logging.info("connected to server at {0}".format(server_addr))

  def _request(self, msg_type, msg_data=None):
    """Helper function to wrap msg w/ msg_type."""
    msg = {}
    msg['type'] = msg_type
    if msg_data or ((msg_data == True) or (msg_data == False)):
      msg['data'] = msg_data

    done = False
    tries = 0
    while not done and tries < MAX_RETRIES:
      try:
        MessageSocket.send(self, self.sock, msg)
        done = True
      except socket.error as e:
        tries += 1
        if tries >= MAX_RETRIES:
          raise
        print("Socket error: {}".format(e))
        self.sock.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.server_addr)

    logging.debug("sent: {0}".format(msg))
    resp = MessageSocket.receive(self, self.sock)
    logging.debug("received: {0}".format(resp))
    return resp

  def close(self):
    """Close the client socket."""
    self.sock.close()

  def register(self, reservation):
      """
      Register ``reservation`` with server.

      Args:
          reservation:

      Returns:

      """
      resp = self._request('REG', reservation)
      return resp

  def register_worker_finished(self):
      """
      Register ``worker as finished`` with server.

      Returns:

      """
      resp = self._request('REG_DONE')
      return resp

  def await_all_workers_finished(self):
      """
      Poll until all reservations completed, then return cluster_info.

      Returns:

      """
      done = False
      while not done:
          done = self._request('QUERY_DONE')
          time.sleep(5)
      return True

  def get_reservations(self):
      """
      Get current list of reservations.

      Returns:

      """
      cluster_info = self._request('QINFO')
      return cluster_info

  def await_reservations(self):
      """Poll until all reservations completed, then return cluster_info."""
      done = False
      while not done:
          done = self._request('QUERY')
          time.sleep(1)
      reservations = self.get_reservations()
      return reservations

  def request_stop(self):
      """Request server stop."""
      resp = self._request('STOP')
      return resp
