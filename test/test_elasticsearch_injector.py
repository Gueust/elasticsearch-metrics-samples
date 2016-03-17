#!/usr/bin/python3

import unittest, logging, time
from es_injectors import elasticsearch_injector as es
from test.mocks import MockLoggingHandler

class TestOpenTsdbParser(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    super(TestOpenTsdbParser, cls).setUpClass()
    # Assuming you follow Python's logging module's documentation's
    # recommendation about naming your module's logs after the module's
    # __name__,the following getLogger call should fetch the same logger
    # you use in the foo module
    logger = logging.getLogger()
    cls._logger_handler = MockLoggingHandler(level='DEBUG')
    logger.addHandler(cls._logger_handler)
    cls.logger_messages = cls._logger_handler.messages

  def setUp(self):
    super(TestOpenTsdbParser, self).setUp()
    self._logger_handler.reset() # So each test is independent

  def test_parse(self):
    """Test parsing within push() function"""
    parser = es.OpenTsdbParser()

    self._logger_handler.reset()
    parser.parse('one')
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Invalid opentsdb put line:'),
                    msg=self._logger_handler)

    self._logger_handler.reset()
    parser.parse('put one')
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Incorrect metric received:'),
                    msg=self._logger_handler)

    self._logger_handler.reset()
    parser.parse('put one two')
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Incorrect metric received:'),
                    msg=self._logger_handler)

    self._logger_handler.reset()

    parser.parse('put one two three four')
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Invalid tag:'),
                    msg=self._logger_handler)
    self._logger_handler.reset()

    # Monday 8 February 2016, 21:16:00 (UTC+0100)
    t = 1454962560
    doc = parser.parse('put metric1 42.42 ' + str(t) + ' host=machine1 cluster=cluster1')
    self.assertEqual(doc, \
          ('metric1', {'timestamp': '1454962560',
           'host': 'machine1',
           'metric1': '42.42',
           'cluster': 'cluster1'}), msg=self._logger_handler)

    # Test '.' remplaced by '-'
    t = 1454962560
    doc = parser.parse('put metric.1 42.42 ' + str(t) + ' host=machine1 cluster=cluster1')
    self.assertEqual(doc, \
          ('metric-1', {'timestamp': '1454962560',
           'host': 'machine1',
           'metric-1': '42.42',
           'cluster': 'cluster1'}), msg=self._logger_handler)



from elasticsearch.helpers.test import get_test_client, ElasticsearchTestCase as BaseTestCase


class TestElasticsearchSender(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    super(TestElasticsearchSender, cls).setUpClass()
    # Assuming you follow Python's logging module's documentation's
    # recommendation about naming your module's logs after the module's
    # __name__,the following getLogger call should fetch the same logger
    # you use in the foo module
    logger = logging.getLogger()
    cls._logger_handler = MockLoggingHandler(level='DEBUG')
    logger.addHandler(cls._logger_handler)
    cls.logger_messages = cls._logger_handler.messages

  def setUp(self):
    super(TestElasticsearchSender, self).setUp()
    self._logger_handler.reset() # So each test is independent
    self.es_client = get_test_client() #TEST_ES_SERVER

  def test_push_and_flush(self):
    parser = es.OpenTsdbParser()
    es_injector = es.ElasticsearchSender(parser,self.es_client, 'bogus_index')

    metrics = ['one', 'put one', 'put one two', 'put one two three four', \
              'put metric1 42.42 ' + str(1454962560) + ' host=machine1 cluster=cluster1']

    es_injector.push(metrics)
    self.assertTrue(len(es_injector.buffer) == 1)
    es_injector.flush()
    self.assertTrue(len(es_injector.buffer) == 0)

    # Test it is injected in the correct index

  def test_push_buffer_size(self):
    parser = es.OpenTsdbParser()
    es_injector = es.ElasticsearchSender(parser, self.es_client, 'bogus_index', buffer_size = 10)

    metrics = ['put metric1 42.42 1454962560 host=machine1 cluster=cluster1' for i in range(0, 11)]
    es_injector.push(metrics)
    length = len(es_injector.buffer)
    self.assertTrue(length == 0, 'The buffer should be empty: ' + str(length))



import random, threading, socket

class TestAggregatorServer(unittest.TestCase):

  metric_index = 'es_injector_test'
  bind_port = 8888
  host = '0.0.0.0'

  class Client(threading.Thread):

    def __init__(self, host, port, client_id, iterations):

      threading.Thread.__init__(self, name='Client')
      self.setDaemon(True)

      self.host = host
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

  def _delete_index(self, index):
    if self.es_client.indices.exists(index):
      self.es_client.indices.delete(index)

  def _create_index(self, index):
    if not self.es_client.indices.exists(index):
      self.es_client.indices.create(index)

  def setUp(self):
    #self._logger_handler.reset() # So each test is independent
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARN)
    logging.getLogger('elasticsearch').setLevel(logging.WARN)
    self.es_client = get_test_client() #TEST_ES_SERVER
   # logging.getLogger().setLevel(logging.WARN)
    self._create_index(self.metric_index)

  def tearDown(self):
    self._delete_index(self.metric_index)

  def test_many_injections(self):

    parser = es.OpenTsdbParser()
    es_injector = es.ElasticsearchSender(parser, self.es_client, self.metric_index)

    server = es.AggregatorServer(self.host, self.bind_port, es_injector)
    server.start()
    # We make sure the server had time to start
    time.sleep(1)

    clients = list()
    nb_clients = 20
    iterations_per_client = 100
    for i in range(0, nb_clients):
      client = self.Client(self.host, self.bind_port, i, iterations_per_client)
      clients.append(client)
      client.start()

    for client in clients:
      client.join()

    # We need the server client threads to executed the code associated with
    # the closing of the socket
    time.sleep(1)

    es_injector.flush()

    self.es_client.indices.flush(index=self.metric_index, wait_if_ongoing=True)

    self.assertTrue(self.es_client.indices.exists(self.metric_index))

    #print(self.es_client.count(index=self.metric_index))
    index_count = self.es_client.count(index=self.metric_index)
    should_count = iterations_per_client * nb_clients

    self.assertEqual(index_count['count'], should_count, \
      'The number of documents ('+ str(index_count['count']) + ') should be ' + str(should_count))


if __name__ == "__main__":
  unittest.main()