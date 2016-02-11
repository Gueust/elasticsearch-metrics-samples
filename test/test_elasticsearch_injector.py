#!/usr/bin/python3

import unittest, logging, time
from es_injectors import elasticsearch_injector as es
from test.mocks import MockLoggingHandler

class TestElasticsearchSender(unittest.TestCase):
  """Unit tests for the ElasticsearchSender class"""

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

  def test_push(self):
    """Test googlemaps local_search()."""
    sender = es.ElasticsearchSender(None)

    sender.push(['one'])
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Incorrect metric received:'),
                    msg=self._logger_handler)
    self._logger_handler.reset()
    sender.push(['one two'])
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Incorrect metric received:'),
                    msg=self._logger_handler)
    self._logger_handler.reset()

    sender.push(['one two three four'])
    self.assertTrue(self._logger_handler.messages['warning'][0].startswith('Invalid tag:'),
                    msg=self._logger_handler)
    self._logger_handler.reset()

    # Monday 8 February 2016, 21:16:00 (UTC+0100)
    t = 1454962560
    sender.push(['metric1 42.42 ' + str(t) + ' host=machine1 cluster=cluster1'])
    self.assertTrue(len(sender.buffer) == 1)
    doc = sender.buffer[0]
    self.assertEquals(doc, \
      {'_type': 'metric1',
       '_source':
          {'timestamp': '1454962560',
           'host': 'machine1',
           'metric1': '42.42',
           'cluster': 'cluster1'},
       '_index': 'test-metrics'})

if __name__ == "__main__":
  unittest.main()