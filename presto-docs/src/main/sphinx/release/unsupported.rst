=============
Release Notes
=============

Presto 101t is equivalent to Presto release 0.101. Below are documented unsupported
features and known limitations.

Unsupported Features
--------------------

The following features from Presto 0.101 may work but are not officially supported:

 * Installing presto via the tar ball
 * Running presto via the launcher script
 * Queue configuration
 * The following connectors:

    * Cassandra
    * Kafka
    * PostgreSQL
    * MySQL
    * Hive-hadoop1
    * Hive-cdh4

 * ROW data type

 * Developer guide, including:
    * SPI
    * Pluggable functions

Known Limitations
-----------------

 * The SQL keyword ``end`` is used as a column name in ``system.runtime.queries``, so in order to query from that column, ``end`` must be wrapped in quotes.
 * ``DECIMAL`` and ``NUMERIC`` datatypes are not supported
 * ``FULL OUTER JOIN`` is not supported
 * ``NATURAL JOIN`` is not supported
 * Scalar subqueries are not supported -- e.g. ``WHERE x = (SELECT y FROM ...)``
 * Correlated subqueries are not supported
 * Non-equi joins are only supported for inner join -- e.g. ``"n_name" < "p_name"``
 * ``EXISTS``, ``EXCEPT``, and ``INTERSECT`` are not supported.

Hive Datatype Notes
-------------------
There are a few caveats for using the Hive connector, in particular related
to datatype support.

Columns with the following types are not visible in a table:

 * ``DECIMAL``
 * ``DECIMAL`` with parentheses
 * ``CHAR``

Some types are only partially supported.

 * ``VARCHAR`` is not supported in ORC and RCBinary file formats.

Additionally, Presto does not map Hive data types 1-to-1.

 * All integral types are mapped to ``BIGINT``
 * ``FLOAT`` and ``DOUBLE`` are mapped to ``DOUBLE``
 * ``STRING`` and ``VARCHAR`` are mapped to ``VARCHAR``

Usually, this should not be a problem, but the mapping could be visible if
column values are passed to Hive UDFs, or through slight differences in mathematical
functions.

The granularity of the ``TIMESTAMP`` datatype is to milliseconds in Presto, while
Hive supports microseconds

``TIMESTAMP`` values that do not have an explicit timezone are parsed according to Hive's
``hive.time-zone`` property in the Hive catalog definition. When results are printed to the
console, the Presto client's JVM timezone is used.

``TIMESTAMP``, ``DATE`` and ``BINARY`` datatypes are not supported for the Parquet file type.