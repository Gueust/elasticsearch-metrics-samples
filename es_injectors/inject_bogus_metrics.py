#!/usr/bin/python3
from datetime import timedelta, date, datetime
import random, time, collections, copy, argparse, sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def daterange(start_date, end_date, time_delta=timedelta(hours=4)):
  current_date = start_date
  while current_date < end_date:
    yield current_date
    current_date += time_delta

metrics_data = {
  "cpu": ["host", "cluster", "os"],
  "used_memory": ["fqdn", "cluster"]
  }

NB_METRICS = 1
metric_names = ['metric_'+str(i) for i in range(0,NB_METRICS)]
hosts = ['fqdn_'+str(i) for i in range(0,NB_METRICS)]


number_tags = 3
tags = {}
for metric in metric_names:
  tags[metric] = [metric  + '_tag_'+str(i)for i in range(0, number_tags)]

def next_tags_positions(tags, tags_positions, nb_tag_values):
  """Returns true if there is a next position, in which case it modifies
     the given array to reflect the next tags_positions"""
  i = len(tags) - 1
  while i >= 0:
    if tags_positions[i] != nb_tag_values - 1:
      tags_positions[i] += 1
      return True
    elif i == 0:
      return False
    elif tags_positions[i-1] != nb_tag_values - 1:
      # We increment the previous number, and reset all others thereafter
      tags_positions[i-1] += 1
      for j in range(i, len(tags)):
        tags_positions[j] = 0
      return True

    i -= 1;

def generate_doc(index, metric_names, metric_tags, start_date, end_date, print_doc=False):
  """Generator of documents to index"""

  for metric in metric_names:

    tags = metric_tags[metric]
    # We need to generate all the possibility for the tags for the metric
    tags_positions = [0]* len(tags)
    while True:
      doc = {}
      doc['_type'] = metric
      doc['_index'] = index
      for i in range(0, len(tags_positions)):
        doc[tags[i]] = tags[i] + '_value' + str(tags_positions[i])

      a = random.uniform(1, 5)
      b = random.uniform(5, 14)

      for single_date in daterange(start_date, end_date):
        date = int(time.mktime(single_date.timetuple())) * 1000
        # We need to duplicate the document for every new value
        doc = copy.deepcopy(doc)
        doc[metric] = random.uniform(min(a,b), max(a,b))
        doc['timestamp'] = date
        if print_doc:
          print(doc)
        yield doc

      if not next_tags_positions(tags, tags_positions, 2):
        break

if __name__ == "__main__":
  import unittest


  parser = argparse.ArgumentParser()
  parser.add_argument("--test", '-t', action="store_true", help="Run tests")
  args = parser.parse_args()

  if not args.test:
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=1)

    es = Elasticsearch()
    print(helpers.bulk(es, generate_doc('test-metrics', metric_names, tags, start_date, end_date, True)))

    sys.exit(0)


  class TestStringMethods(unittest.TestCase):

    def test_upper(self):
      end_date = datetime.utcnow()
      start_date = end_date - timedelta(minutes=50)


      docs = []
      for doc in generate_doc('test-metrics', ["metric_1"], {"metric_1": ["tag1_1", "tag1_2", "tag1_3"]}, start_date, end_date):
        # We remove the timestamp which changes at every execution
        del doc['timestamp']
        if 'metric_1' in doc:
          del doc['metric_1']
        if 'metric_2' in doc:
          del doc['metric_2']
        docs.append(doc)

      correct_result = \
      [
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value0', 'tag1_2': 'tag1_2_value0', 'tag1_3': 'tag1_3_value0'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value0', 'tag1_2': 'tag1_2_value0', 'tag1_3': 'tag1_3_value1'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value0', 'tag1_2': 'tag1_2_value1', 'tag1_3': 'tag1_3_value0'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value0', 'tag1_2': 'tag1_2_value1', 'tag1_3': 'tag1_3_value1'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value1', 'tag1_2': 'tag1_2_value0', 'tag1_3': 'tag1_3_value0'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value1', 'tag1_2': 'tag1_2_value0', 'tag1_3': 'tag1_3_value1'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value1', 'tag1_2': 'tag1_2_value1', 'tag1_3': 'tag1_3_value0'},
        {'_type': 'metric_1', '_index': 'test-metrics', 'tag1_1': 'tag1_1_value1', 'tag1_2': 'tag1_2_value1', 'tag1_3': 'tag1_3_value1'}
      ]
      self.maxDiff = None
      self.assertEqual(docs, correct_result)

  # We need to clear the arguments, since they will be interpreted by unittest
  sys.argv[1:] = []
  unittest.main()