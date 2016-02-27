#!/usr/bin/python

import socket, threading, argparse, logging, os, sys, time
import logging
from logging.handlers import RotatingFileHandler
from elasticsearch import Elasticsearch
from elasticsearch import helpers

VERSION = "0.0.1"

HOST = '0.0.0.0'   # Symbolic name, meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port
LOG_PATH = os.path.expanduser('~/es_injector/injector.log')

INDEX_NAME = 'test-metrics'

class ElasticsearchSender:

  def __init__(self, es, opentsdb_put = False, buffer_size = 500, max_delay = 60, time_unit='ms'):
    """An elasticsearch injector for data respecting the following format:

    metric_name metric_value timestamp(in `time_unit`) [key=value, [key=value]]

    It injects into elasticsearch considering it must send the date as
    `epoch_millis`. Thus, if `time_unit` == 's', it will add 3 trailling zeros.

    :param es: An Elasticsearch instance
    :param buffer_size: The buffer size before using the bulk elastic API
    :param max_delay: A `flush()` will be done when receiving a valid data if
                      the last flush has been done for more than `max_delay` (seconds)
    :param time_unit:

    """
    self.es = es
    self.opentsdb_put = opentsdb_put
    self.buffer = []
    self.buffer_size = buffer_size
    self.max_delay = max_delay
    self.time_unit = time_unit
    self.last_flush = time.time()

  def push(self, metrics, logging_prefix=''):
    """
    :param metrics: An iterable of string, each repreasenting a metric data
    :param logging_prefix: A prefix to use for error logging messages
    """
    """A list of strings representing metrics"""
    docs = list()
    for metric in metrics:
      do_break = False

      if self.opentsdb_put:
        if metric[0] != "put":
          logging.warning(logging_prefix + 'Invalid opentsdb put line: ' + metric)
          continue
        metric = metric[1:]

      elements = metric.split(' ')
      if len(elements) < 3:
        logging.warning(logging_prefix + 'Incorrect metric received: ' + metric)
        continue
      else:
        doc = {}
        metric_name = elements[0].replace('.', '-')
        doc[metric_name] = elements[1]
        doc['timestamp'] = elements[2]
        if self.time_unit == 's':
          doc['timestamp'] += '000'
        for tag in elements[3:]:
          split_tag = tag.split('=')
          if len(split_tag) != 2:
            logging.warning('Invalid tag: ' + tag + ' in: ' + metric)
            do_break = True
            break
          else:
            doc[split_tag[0]] = split_tag[1]

        if do_break:
          break

        self.buffer.append({'_index': INDEX_NAME,
                            '_type': metric_name,
                            '_source': doc})

        current_time = time.time()
        if len(self.buffer) > self.buffer_size or (current_time - self.last_flush) > 60:
          flush()
          self.last_flush = current_time

  def flush(self):
    nb_success, errors = helpers.bulk(es, self.buffer, chunk_size = 500, raise_on_error = False, raise_on_exception = False)
    del self.buffer[:]
    logging.info((nb_success, errors))

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
    threading.Thread.__init__(self)
    self.injectorsocket = clientsocket
    self.ip = ip
    self.port = port
    self.injector = injector
    logging.info("[+] New thread for " + str(self.ip) + ':' + str(self.port))

  def run(self):
    remainer = ''
    while True:
      data = self.injectorsocket.recv(1024)
      print('Received: ' + data)
      if not data:
        self.injectorsocket.close()
        logging.info('[-] Connection closed by ' + str(self.ip) + ':' + str(self.port))
        return
      logging.debug('Received: ' + data)

      end_with_new_line = data.endswith('\n')
      print('Endwish:' + str(end_with_new_line))
      lines = data.split('\n')

      if end_with_new_line:
        if remainer == '':
          self.injector.push(lines[:-1], logging_prefix='['+str(self.ip)+':'+str(self.port)+']')
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
          self.injector.push[lines[:-1]]
          remainer = lines[-1]

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--opentsdb_format", action='store_true', help="Consider lines to be introduced by the 'put' keyword")
  parser.parse_args()


  log_dir = os.path.dirname(LOG_PATH)
  if not os.path.isdir(log_dir):
    print('Creating log directory: ' + log_dir)
    os.makedirs(log_dir)

  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
  handler = RotatingFileHandler(LOG_PATH, maxBytes=4*1024, backupCount=5)
  handler.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s'))
  logging.getLogger().handlers = []
  logging.getLogger().addHandler(handler)

  logging.info('\n')
  logging.info('Starting es_injector')
  tracer = logging.getLogger('elasticsearch.trace')
  tracer.setLevel(logging.INFO)
  tracer.addHandler(logging.FileHandler(os.path.join(log_dir, 'es_trace.log')))

  serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  try:
    serversocket.bind((HOST, PORT)) #socket.gethostname()
    logging.info('Socket bind completed: ' + socket.gethostname() + ':' + str(PORT))
  except socket.error as msg:
    logging.critical('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
    logging.critical('Exiting...')
    sys.exit(1)

  serversocket.listen(5)
  logging.info('Socket now listening to ' + str(PORT))

  es = Elasticsearch(['localhost'],
                     sniff_on_start=True,
                     sniff_on_connection_fail=True,
                     sniffer_timeout=60*5,
                     maxsize=10)
  es_sender = ElasticsearchSender(es, opentsdb_put = parser.opentsdb_format)

  try:
    while True:
      (clientsocket, (ip, port)) = serversocket.accept()
      new_thread = ClientThread(clientsocket, ip, port, es_sender)
      new_thread.setDaemon(True)
      new_thread.start()
  finally:
    serversocket.close()
    es_sender.flush()
    logging.info('Close socket, flush buffer and quit')
    sys.exit(0)