=================
Release 0.179-t
=================

Presto 0.179-t is equivalent to Presto release 0.179, with some additional features and patches.

Cost-Based Optimizer
--------------------
* Support cost-based join reordering. See :doc:`../optimizer/reorder-joins`.
* Replace the ``distributed_joins`` property with the ``join_distribution_type`` session property or ``join-distribution-type`` config property.  Options are ``AUTOMATIC``, ``REPARTITIONED``, and ``REPLICATED``.
* Replace the ``reorder_joins`` property with the ``join_reordering_strategy`` session property or ``optimizer.join-reordering-strategy`` config property.  Options are ``NONE``, ``ELIMINATE_CROSS_JOINS``, and ``COST_BASED``.
* Determine join distribution type based on statistics when ``join_distribution_type`` is set to ``AUTOMATIC``.
* Show estimated statistics for each stage of the query plan in ``EXPLAIN`` results.

Spill to Disk
--------------
* Improve spill-to-disk support for aggregations.
* Support spilling to disk for inner joins that exceed available memory. This can be enabled with the ``beta.spill-enabled`` configuration flag.
* Verify that spilled data is not corrupted while un-spilling. 
* Remove the ``experimental.operator-memory-limit-before-spill`` config property and the ``operator_memory_limit_before_spill`` session property.
* Allow configuring the amount of memory that can be used for merging
  spilled aggregation data from disk using the ``experimental.aggregation-operator-unspill-memory-limit`` config property
  or the ``aggregation_operator_unspill_memory_limit`` session property.
* Only spill to disk for aggregations when the cluster runs out of memory.
* Add spilled data size to ``EXPLAIN ANALYZE``.
* Add spilled data size to the Presto CLI when run with the ``--debug`` flag.
* Add spilled data size to the Presto WebUI on the Query Details and Live Plan pages.

General Changes
----------------
* Support prepared statements that are longer than 4K bytes.
* Improve the performance of joins with only non-equality conditions by using
  a nested loops join instead of a hash join.
* Add ``VERBOSE`` option for ``EXPLAIN ANALYZE`` that provides additional low-level details about query performance.
* Add per-task distribution information to the output of ``EXPLAIN ANALYZE``.
* Enable more join predicates to be pushed down to the source tables.
* Add :link: TPCDS Connector. This connector provides a set of schemas to support the TPC Benchmark™ DS (TPC-DS). It exposes table and column statistics for schemas ``tiny`` and ``sf1``.
* Avoid potentially expensive computation on coordinator by offloading certain plan fragments to worker nodes.
* Make ``DECIMAL`` the default literal for non-integral numbers.
* Add support for sorting data on all workers of the cluster, instead of just one node. This feature is disabled by default and can be enabled with the config property ``experimental.distributed-sort`` and the session property ``distributed_sort``. 
* Improve performance for selective filters by merging pages with few output rows. This feature can be disabled by setting the config property ``experimental.filter-and-project-min-output-page-row-count`` or the session property ``filter_and_project_min_output_page_row_count`` to 0. The default value is 256.

Bug Fixes
---------
* Fix query failure for ``CHAR`` functions :func:`trim`, :func:`rtrim`, and
  :func:`substr` when the return value would have trailing spaces under
  ``VARCHAR`` semantics.
* Fix incorrect results when performing comparisons between values of approximate
  data types (``REAL``, ``DOUBLE``) and columns of certain exact numeric types
  (``INTEGER``, ``BIGINT``, ``DECIMAL``).
* Fix failure in ``EXPLAIN`` for tables partitioned on a ``TIMESTAMP`` column
* Fix execution of several window functions on array and map types. Without this patch, some window functions taking array or map types (e.g. approx_percentile) were failing.
* Fix incorrect empty results for tables filtered on ``CHAR(x)``, ``DECIMAL``, ``DATE``, or ``TIMESTAMP`` partition columns.
* Fix incorrect results when ``optimizer.optimize-metadata-queries`` is enabled for queries involving aggregation over ``TopN`` and ``Filter``.
* Skip unknown costs in ``EXPLAIN`` output.
* Fix query failure when ``ORDER BY`` expressions reference columns that are used in the ``GROUP BY`` clause by their fully-qualified name.
* Handle ``GROUPING`` when aggregation expressions require implicit coercions.

Security Changes
----------------
* Add Kerberos principal matching rules to file-based system access control.
* Support secure internal communication between Presto nodes via HTTPS.
* Support secure internal communication between Presto nodes via LDAP.
* Support secure internal communication between Presto nodes via Kerberos.
* ``ROLE`` support for the Hive connector, including ``CREATE ROLE``,
  ``DROP ROLE``, ``GRANT ROLE``, ``REVOKE ROLE``, ``SET ROLE``, ``SHOW CURRENT ROLES``,
  ``SHOW ROLES`` and ``SHOW ROLE GRANTS`` commands.

Hive Changes
------------
* Allow partitions without files for bucketed tables (via ``hive.empty-bucketed-partitions.enabled``)
* Allow multiple files per bucket for bucketed tables (via ``hive.multi-file-bucketing.enabled``). There must be one or more files per bucket. File names must match the Hive naming convention.
* Ignore partition bucketing if table is not bucketed. This allows dropping the bucketing from table metadata but leaving it for old partitions.
* Fix potential native memory leak when writing tables using RCFile.
* Fix query failure when computing statistics on an unpartitioned table in CDH 5.11.
* Add a configuration option ``hive.create-non-managed-table-enabled`` that can disable creating external Hive tables (default value is ``true``).
* Support role management for the Hive connector.

TPC-H Changes
-------------
* Add column statistics for schemas ``tiny`` and ``sf1``.

CLI Changes
-----------
* Fix an issue that would sometimes prevent queries from being cancelled when
  exiting from the pager.

SPI Changes
-----------
* Fix regression that broke serialization of SchemaTableName.

Data Types
----------
The Teradata distribution of Presto fixes the semantics of the ``TIMESTAMP`` and ``TIME``
types to align with the SQL standard. See the following sections for details.

**TIMESTAMP semantic changes**

Previously, the ``TIMESTAMP`` type described an instance in time in the Presto session's time zone.
Now, Presto treats ``TIMESTAMP`` values as a set of the following fields representing wall time:

 * ``YEAR OF ERA``
 * ``MONTH OF YEAR``
 * ``DAY OF MONTH``
 * ``HOUR OF DAY``
 * ``MINUTE OF HOUR``
 * ``SECOND OF MINUTE`` - as decimal with precision 3

For that reason, a ``TIMESTAMP`` value is not linked with the session time zone in any way until a time zone is needed explicitly,
such as when casting to a ``TIMESTAMP WITH TIME ZONE`` or ``TIME WITH TIME ZONE``.
In those cases, the time zone offset of the session time zone is applied, as specified in the SQL standard.

For various compatibility reasons, when casting from date/time type without a time zone to one with a time zone, a fixed time zone
is used as opposed to the named one that may be set for the session.

eg. with ``-Duser.timezone="Asia/Kathmandu"`` on CLI

 * Query: ``SELECT CAST(TIMESTAMP '2000-01-01 10:00' AS TIMESTAMP WITH TIME ZONE);``
 * Previous result: ``2000-01-01 10:00:00.000 Asia/Kathmandu``
 * Current result: ``2000-01-01 10:00:00.000 +05:45``

**TIME semantic changes**

The ``TIME`` type was changed similarly to the ``TIMESTAMP`` type.

**TIME WITH TIME ZONE semantic changes**

Due to compatibility requirements, having ``TIME WITH TIME ZONE`` completely aligned with the SQL standard was not possible yet.
For that reason, when calculating the time zone offset for ``TIME WITH TIME ZONE``, the Teradata distribution of Presto uses
the session's start date and time.

This can be seen in queries using ``TIME WITH TIME ZONE`` in a time zone that has had time zone policy changes or uses DST.
eg. With session start time on 1 March 2017

 * Query: ``SELECT TIME '10:00:00 Asia/Kathmandu' AT TIME ZONE 'UTC'``
 * Previous result: ``04:30:00.000 UTC``
 * Current result: ``04:15:00.000 UTC``

**Time-related bug fixes**

 * The ``current_time`` and ``localtime`` functions were fixed to return the correct value for non-UTC timezones.

