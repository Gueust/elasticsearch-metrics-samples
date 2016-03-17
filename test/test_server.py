import unittest, logging, time
from es_injectors import elasticsearch_injector as es
from elasticsearch.helpers.test import get_test_client, ElasticsearchTestCase as BaseTestCase
import random, threading, socket


metric_index = 'es_injector_test'
bind_port = 8888
host = '0.0.0.0'

#TEST_ES_SERVER
es_client = get_test_client()

parser = es.OpenTsdbParser()
es_injector = es.ElasticsearchSender(parser, es_client, metric_index)

server = es.AggregatorServer(host, bind_port, es_injector)
server.start()

class Client(threading.Thread):

  def __init__(self, host, port, client_id, iterations):

    threading.Thread.__init__(self, name='Client')
    self.setDaemon(True)

    self.port = port
    self.client_id = client_id
    self.iterations = iterations

  def run(self):

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', self.port))

    for i in range(0, self.iterations):
      t = int(time.time())
      value = random.random()
      client_socket.sendall( ('put metric' + str(self.client_id) + ' ' + str(value) + ' ' + str(t) + ' host=me cluster=cluster1\n').encode() )

    client_socket.close()

class TestAggregatorServer(unittest.TestCase):

  def _delete_index(self, index):
    if es_client.indices.exists(index):
      es_client.indices.delete(index)

  def _create_index(self, index):
    if not es_client.indices.exists(index):
      es_client.indices.create(index)

  def setUp(self):
    #self._logger_handler.reset() # So each test is independent
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARN)
    logging.getLogger('elasticsearch').setLevel(logging.WARN)

   # logging.getLogger().setLevel(logging.WARN)
    self._create_index(metric_index)

  def tearDown(self):
    self._delete_index(metric_index)

  def test_many_injections(self):

    # We make sure the server had time to start
    time.sleep(1)

    clients = list()
    nb_clients = 20
    iterations_per_client = 100
    for i in range(0, nb_clients):
      client = Client(host, bind_port, i, iterations_per_client)
      clients.append(client)
      client.start()

    for client in clients:
      client.join()

    # We need the server client threads to executed the code associated with
    # the closing of the socket
    time.sleep(1)

    es_injector.flush()

    es_client.indices.flush(index=metric_index, wait_if_ongoing=True)

    self.assertTrue(es_client.indices.exists(metric_index))

    #print(es_client.count(index=metric_index))
    index_count = es_client.count(index=metric_index)
    should_count = iterations_per_client * nb_clients

    self.assertEqual(index_count['count'], should_count, \
      'The number of documents ('+ str(index_count['count']) + ') should be ' + str(should_count))

  def test_version(self):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(0.5)
    client_socket.connect(('localhost', bind_port))
    client_socket.sendall( ('version\n').encode())

    result = ''
    while True:
      data = client_socket.recv(1024).decode()
      result += data
      if '\n' in data or len(data) == 0:
        break

    client_socket.close()

    self.assertTrue(data.endswith('\n'))
    data = data[:-1]
    self.assertEqual(data, es.VERSION)

if __name__ == "__main__":
  unittest.main(verbosity=2)