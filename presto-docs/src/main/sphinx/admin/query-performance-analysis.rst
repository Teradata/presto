==========================
Query Performance Analysis
==========================

Presto provides several commands and utilities to help with performance
analysis of your queries:

* The standard :doc:`../sql/explain` command displays both logical and
  distributed query plans.
* The :doc:`../sql/explain-analyze` command provides detailed execution-time metrics
  such as a number of input and output rows at each stage and aggregated CPU
  time.
* The :doc:`web-interface` offers both a cluster-wide overview of all
  queries as well as a detailed view for each query. It includes the overall
  query metrics, stage-level progress and the task-level details. The ``Live Plan``
  view displays the graphical representation of a distributed query plan with
  the execution metrics updated while a query is running.

The collected information can help with identifying the slow parts of a query
and the means to address the performance challenges (for example, manually
reordering joins).