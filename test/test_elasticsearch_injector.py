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
    es_injector = es.ElasticsearchSender(self.es_client, parser)

    metrics = ['one', 'put one', 'put one two', 'put one two three four', \
              'put metric1 42.42 ' + str(1454962560) + ' host=machine1 cluster=cluster1']

    es_injector.push(metrics)
    self.assertTrue(len(es_injector.buffer) == 1)
    es_injector.flush()
    self.assertTrue(len(es_injector.buffer) == 0)
  def test_push_buffer_size(self):
    parser = es.OpenTsdbParser()
    es_injector = es.ElasticsearchSender(self.es_client, parser, buffer_size = 10)

    metrics = ['put metric1 42.42 1454962560 host=machine1 cluster=cluster1' for i in range(0, 11)]
    es_injector.push(metrics)
    length = len(es_injector.buffer)
    self.assertTrue(length == 0, 'The buffer should be empty: ' + str(length))

if __name__ == "__main__":
  unittest.main()