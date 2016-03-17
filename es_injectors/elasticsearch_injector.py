#!/usr/bin/python

import socket, threading, argparse, logging, os, sys, time
import logging
from logging.handlers import RotatingFileHandler
from elasticsearch import Elasticsearch
from elasticsearch import helpers

VERSION = "0.0.1"

HOST = '0.0.0.0'   # Symbolic name, meaning all available interfaces
DEFAULT_PORT = 8888 # Arbitrary non-privileged port
LOG_PATH = os.path.expanduser('~/es_injector/injector.log')

INDEX_NAME = 'test-metrics'

class OpenTsdbParser:
  """A parser building metrics from opentsdb syntax

     :param time_unit: Configure the format of the timestamp received by
                       opentsdb to ajust the timestamp for es (s or ms)"""

  def __init__(self, time_unit='ms'):
    self.time_unit = time_unit
    self.logger = logging.getLogger('OpenTsdbParser')

  def parse(self, metric, logging_prefix=''):
    """Returns None if the parsing is invalid, the json document otherwise

      :param logging_prefix: A prefix to use for error logging messages
    """
    elements = metric.split(' ')

    if elements[0] != "put":
      self.logger.warning(logging_prefix + "Invalid opentsdb put line: '" + metric + "'")
      return None
    elements = elements[1:]

    if len(elements) < 3:
      self.logger.warning(logging_prefix + "Incorrect metric received: '" + metric + "'")
      return None

    doc = {}
    metric_name = elements[0].replace('.', '-')
    doc[metric_name] = elements[1]
    doc['timestamp'] = elements[2]
    if self.time_unit == 's':
      doc['timestamp'] += '000'
    for tag in elements[3:]:
      split_tag = tag.split('=')
      if len(split_tag) != 2:
        self.logger.warning(logging_prefix + 'Invalid tag: ' + tag + " in: '"+ metric + "'")
        return None

      doc[split_tag[0]] = split_tag[1]

    return (metric_name, doc)

class ElasticsearchSender:

  def __init__(self, parser, es, index, buffer_size = 5000, max_delay = 60, time_unit='ms'):
    """An elasticsearch injector for data respecting the following format:

    metric_name metric_value timestamp(in `time_unit`) [key=value, [key=value]]

    It injects into elasticsearch considering it must send the date as
    `epoch_millis`. Thus, if `time_unit` == 's', it will add 3 trailling zeros.

    :param es: An Elasticsearch instance
    :param buffer_size: The buffer size before using the bulk elastic API.
                        `flush()` is called after (buffer_size + 1) messages.
    :param max_delay: A `flush()` will be done when receiving a valid data if
                      the last flush has been done for more than `max_delay` (seconds)
    :param time_unit:

    """
    self.parser = parser
    self.es = es
    self.index = index
    self.buffer_size = buffer_size
    self.max_delay = max_delay
    self.time_unit = time_unit

    self.buffer = []
    self.last_flush = time.time()
    self.lock = threading.RLock()

    self.logger = logging.getLogger('ElasticsearchSender')

  def push(self, metrics, logging_prefix=''):
    """
    :param metrics: An iterable of string, each repreasenting a metric data
    """
    """A list of strings representing metrics"""
    docs = list()
    for metric in metrics:
      line = self.parser.parse(metric, logging_prefix=logging_prefix)

      if line is None:
        continue

      self.lock.acquire()
      self.buffer.append({'_index': self.index,
                          '_type': line[0],
                          '_source': line[1]})

      current_time = time.time()
      if len(self.buffer) > self.buffer_size or (current_time - self.last_flush) > 60:
        self.flush()
        self.last_flush = current_time
      self.lock.release()

  def flush(self):
    self.lock.acquire()
    logging.critical('Buffer:' + str(len(self.buffer)))
    nb_success, errors = helpers.bulk(self.es, self.buffer, chunk_size = 500, raise_on_error = False, raise_on_exception = False)
    del self.buffer[:]
    self.lock.release()

    self.logger.info((nb_success, errors))

class ClientThread(threading.Thread):
  """This thread will listen to a socket and send to the `injector` all
     lines received for processing. It uses socket.recv() to read data from the
     socket, and maintains an internal buffer to deal with incomplete lines.

     `injector` must implement push(lines) and flush()
  """
  def __init__(self, clientsocket, ip, port, injector):
    """
    :param clientsocket: A socket from which data will be received
    :param ip: The ip of the client (only used for logging)
    :param port: The port of the client socket (only used for logging)
    :param injector: an object having ``push(string list)`` and ``flush()``
                     defined
    """
    threading.Thread.__init__(self, name='Client: ' + str(ip) + ':' + str(port))
    self.setDaemon(True)
    self.clientsocket = clientsocket
    self.ip = ip
    self.port = port
    self.injector = injector

    self.logger = logging.getLogger('ClientThread')

    self.logger.info("[+] New thread for " + str(self.ip) + ':' + str(self.port))

  def run(self):
    remainer = ''
    try:
      while True:
        data = self.clientsocket.recv(1024)

        #print('Received: ' + data)
        if not data:
          self.clientsocket.close()
          self.logger.info('[-] Connection closed by ' + str(self.ip) + ':' + str(self.port))
          return
        #self.logger.debug('Received: ' + data)

        end_with_new_line = data.endswith('\n')
        #print('Endwish:' + str(end_with_new_line))
        lines = data.split('\n')

        if end_with_new_line:
          # When ending with a new line, the last element of lines is the empty string ''
          lines = lines[:-1]
          if remainer == '':
            self.injector.push(lines[:], logging_prefix='['+str(self.ip)+':'+str(self.port)+']')
          else:
            end = lines.pop(0)
            remainer += end
            self.injector.push([remainer])
            remainer = ''
            self.injector.push(lines)
        else:
          end = lines.pop(0)
          remainer += end
          if len(lines) > 0:
            self.injector.push([remainer])
            self.injector.push(lines[:-1])
            remainer = lines[-1]
    finally:
      self.clientsocket.close()

class AggregatorServer(threading.Thread):

  def __init__(self, bind_host, bind_port, injector):
    """
    :param clientsocket: A socket from which data will be received
    :param ip: The ip of the client (only used for logging)
    :param port: The port of the client socket (only used for logging)
    :param injector: an object having ``push(string list)`` and ``flush()``
                     defined
    """
    threading.Thread.__init__(self, name='AggregatorServer: '+bind_host + ':' + str(bind_port))
    self.setDaemon(True)
    self.host = bind_host
    self.port = bind_port
    self.injector = injector
    self.logger = logging.getLogger('AggregatorServer')

  def run(self):

    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #= True # Prevent 'cannot bind to address' errors on restart
      serversocket.bind((self.host, self.port)) #socket.gethostname()
      self.logger.info('Socket bind completed: ' + socket.gethostname() + ':' + str(self.port))
    except socket.error as msg:
      self.logger.critical('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
      self.logger.critical('Exiting...')
      sys.exit(1)

    serversocket.listen(10)
    self.logger.info('Socket now listening to ' + str(self.port))

    try:
      while True:
        (clientsocket, (ip, port)) = serversocket.accept()
        new_thread = ClientThread(clientsocket, ip, port, self.injector)
        new_thread.setDaemon(True)
        new_thread.start()
    finally:
      serversocket.close()
      self.injector.flush()
      self.logger.info('Close socket, flush buffer and quit')
      #sys.exit(0)

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--port", default=DEFAULT_PORT, type=int, help='Port on which to listen (default:' + str(DEFAULT_PORT) + ')')
  args = parser.parse_args()


  log_dir = os.path.dirname(LOG_PATH)
  if not os.path.isdir(log_dir):
    print('Creating log directory: ' + log_dir)
    os.makedirs(log_dir)

  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
  handler = RotatingFileHandler(LOG_PATH, maxBytes=4*1024, backupCount=5)
  handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s'))
  #logging.getLogger().handlers = []
  #logging.getLogger().addHandler(handler)

  logging.info('\n')
  logging.info('Starting es_injector')
  tracer = logging.getLogger('elasticsearch.trace')
  tracer.setLevel(logging.INFO)
  tracer.addHandler(logging.FileHandler(os.path.join(log_dir, 'es_trace.log')))

  parser = OpenTsdbParser()

  es = Elasticsearch(['localhost'],
                     sniff_on_start=True,
                     sniff_on_connection_fail=True,
                     sniffer_timeout=60*5,
                     maxsize=10)
  es_injector = ElasticsearchSender(parser, es, INDEX_NAME)

  server = AggregatorServer(HOST, args.port, es_injector)
  #server.setDaemon(True)
  #server.start()

  server.run()

