.. Elasticsearch metrics tools documentation master file, created by
   sphinx-quickstart on Sun Feb  7 00:09:38 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Elasticsearch metrics tools's documentation!
=======================================================

Contents:

.. toctree::
   :maxdepth: 2


This projects provides all necessary tools to store your metrics (as timeseries) in elasticsearch.
One can be using Grafana to display them or any other tools allowing querying metrics in elastic.

A storage format is enforced to ensure some features when querying, but should be compatible with
most use cases.

What is a metric data?
-----------------------

A metric data is supposed to have:
 - a name, to know to which metric it belongs,
 - a value,
 - a timestamp,
 - a list of tags, being (key, value) pairs to represent different dimensions of the metric.

The value will always be considered to be numeric, but the present project does not suppose so, and
is compatible with other type of data.

Here is for instance a json representing a metric data:

.. code-block:: json

  {
    "response_time": 100,
    "timestamp": 1442165810,
    "service": "mysite.com",
    "http_method": "GET"
  }


The Elasticsearch template
--------------------------

To be able to get autocompletion of tags name for metrics, all the data for a given metric is
stored within a single **_type** within elasticsearch.

Here is a template, inspired by
`this blog <https://www.elastic.co/blog/elasticsearch-as-a-time-series-data-store>`_
that will aims for minimal disk and memory footprint:

.. code-block:: json

  {
    "template": "test-metrics*",
    "settings": {
      "index": {
        "refresh_interval": "60s"
      }
    },
    "mappings": {
      "_default_": {
        "dynamic_templates": [
          {
            "strings": {
              "match": "*",
              "match_mapping_type": "string",
              "mapping":   { "type": "string", "doc_values": true, "index": "not_analyzed" }
            }
          }
        ],
        "_all":            { "enabled": false },
        "_source":         { "enabled": false },
        "properties": {
          "timestamp":    { "type": "date",    "doc_values": true}
        }
      }
    }
  }

Inject your data
----------------

This project provides some scripts to aggregate your metrics and inject them using the
elasticsearch bulk API.

The **elasticsearch_injector.py** script is a TCP server that will listen for metrics,
and inject them using the elasticsearch bulk API.

The expected format for the data received by the aggregator is::
  metric_name value timestamp [key=value [key=value]]

For instance::

..

You can of course inject your data using directly the elasticsearch API.
Howerver, do not forget that:
 - document keys must not containg a dot ('.') in elasticsearch 2,
 - your metric name, in addition to be the key value for the metric,
   must also be the _type of your document.


Compact your data
-----------------


Delete your data
----------------


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`