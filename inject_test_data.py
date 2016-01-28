#!/usr/bin/python3
from datetime import timedelta, date, datetime
import random, time
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def daterange(start_date, end_date):
  current_date = start_date
  while current_date < end_date:
    yield current_date
    current_date += timedelta(hours=1)



metric_names = ['metric_'+str(i) for i in range(0,5)]
hosts = ['fqdn_'+str(i) for i in range(0,5)]

number_tags = 3
tags = {}
for metric in metric_names:
  tags[metric] = [metric  + '_tag_'+str(i)for i in range(0, number_tags)]

def generate_doc(index, metric_names, tags, start_date, end_date):
  """Generator of documents to index"""
  for metric in metric_names:
    doc = {}
    doc['_type'] = metric
    doc['_index'] = index
    for tag in tags[metric]:
      for tag_value in [tag+'v1', tag+'v2']:

        doc[tag] = tag_value
        a = random.uniform(1, 5)
        b = random.uniform(5, 14)

        for single_date in daterange(start_date, end_date):
          date = int(time.mktime(single_date.timetuple())) * 1000
          doc[metric] = random.uniform(min(a,b), max(a,b))
          doc['timestamp'] = date
          print(doc)
          yield doc



end_date = datetime.utcnow()
start_date = end_date - timedelta(days=7)


es = Elasticsearch()

print(helpers.bulk(es, generate_doc('test-metrics', metric_names, tags, start_date, end_date)))

#for (metric_name, metric_doc) in :
#  res = es.index(index="test-index", doc_type=metric_name, body=metric_doc)
#  print(res)

#res = es.get(index="test-index", doc_type='tweet', id=1)
#print(res['_source'])#

#es.indices.refresh(index="test-index")#

#res = es.search(index="test-index", body={"query": {"match_all": {}}})
#print("Got %d Hits:" % res['hits']['total'])
#for hit in res['hits']['hits']:
#    print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
