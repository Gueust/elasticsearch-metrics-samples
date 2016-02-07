#!/usr/bin/python

import socket, threading, argparse, logging, os, sys
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

  def __init__(self, es, buffer_size = 500, max_delay = 60):
    self.es = es
    self.buffer = []
    self.buffer_size = buffer_size
    self.max_delay = max_delay

  def push(self, metrics):
    """A list of strings representing metrics"""
    docs = list()
    print(metrics)
    for metric in metrics:
      elements = metric.split(' ')
      if len(elements) < 3:
        logging.warning('Incorrect metric received: ' + metric)
        continue

      else:
        print(elements)
        doc = {}
        doc[elements[0]] = elements[1]
        doc['timestamp'] = elements[2]
        for tag in elements[3:]:
          split_tag = tag.split('=')
          if len(split_tag) != 2:
            logging.warning('Invalid tag: ' + tag + ' in: ' + metric)
          else:
            doc[split_tag[0]] = split_tag[1]

        self.buffer.append({'_index': INDEX_NAME,
                            '_type': elements[0],
                            '_source': doc})
        print(self.buffer[-1])
        if len(self.buffer) > self.buffer_size:
          flush()

  def flush(self):
    nb_success, errors = helpers.bulk(es, self.buffer, chunk_size = 500, raise_on_error = False, raise_on_exception = False)
    logging.info((nb_success, errors))

class ClientThread(threading.Thread):

  def __init__(self, clientsocket, ip, port, es_client):
    threading.Thread.__init__(self)
    self.clientsocket = clientsocket
    self.ip = ip
    self.port = port
    self.es_client = es_client
    logging.info("[+] New thread for " + str(self.ip) + ':' + str(self.port))

  def run(self):
    remainer = ''
    while True:
      data = self.clientsocket.recv(1024)
      print('Received: ' + data)
      if not data:
        self.clientsocket.close()
        logging.info('[-] Connected closed by ' + str(self.ip) + ':' + str(self.port))
        return
      logging.debug('Received: ' + data)

      end_with_new_line = data.endswith('\n')
      print('Endwish:' + str(end_with_new_line))
      lines = data.split('\n')

      if end_with_new_line:
        if remainer == '':
          self.es_client.push(lines[:-1])
        else:
          end = lines.pop(0)
          remainer += end
          self.es_client.push([remainer])
          remainer = ''
          self.es_client.push(lines)
      else:
        end = lines.pop(0)
        remainer += end
        if len(lines) > 0:
          self.es_client.push([remainer])
          self.es_client.push[lines[:-1]]
          remainer = lines[-1]

def main(args):

  threads = []
  for i in range(10):
    t = Worker()
    threads.append(t)
    t.start()

  while len(threads) > 0:
    try:
      # Join all threads using a timeout so it doesn't block
      # Filter out threads which have been joined or are None
      threads = [t.join(1) for t in threads if t is not None and t.isAlive()]
    except KeyboardInterrupt:
      print "Ctrl-c received! Sending kill to threads..."
      for t in threads:
          t.kill_received = True


if __name__ == '__main__':

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
  es_sender = ElasticsearchSender(es)

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