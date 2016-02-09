
Elasticsearch based Metrics storage
===================================

This projects provides Python tools to use Elasticsearch as a metric database, in particular using Grafana for visualization. It is Python 2 and 3 compatible.

It contains:
  * a metrics aggregator server that injects metrics into
elasticsearch. It is compatible with the
`OpenTsdb tcollector <https://github.com/OpenTSDB/tcollector>`_ data collection
framework ;
  * a generator of bogus metrics for tests purposes.

The goal is to supply more tools to be able to compact and downsample metrics
as they become older.

Compatible with:
 * Python >= 2.7
 * Python 3

Documentation
-------------

The documentation is available within the `docs/` directory.

Dependencies
------------
When using Python 2:
    pip install elasticsearch

When using Python 3:
    pip3 install elasticsearch

For documentation(todo):
    sudo easy_install sphinx