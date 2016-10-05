===============
Release 0.127-t
===============

Presto 0.127-t is equivalent to Presto release 0.127, with some additional features.

General Fixes
-------------

* Remove buggy optimization to prune redundant projections because it produced wrong results.

Datatypes
---------

* Experimental support for the decimal datatype

RPM Fixes
---------
* The presto rpm now works with all versions of rpm after and including 4.6.  And has a separate rpm
  for rpm versions before 4.6
* Fix rpm upgrade

Unsupported Functionality
-------------------------

Some functionality from Presto 0.127 may work but are not officially supported by Teradata.

* The installation method as documented on `prestodb.io <https://prestodb.io/docs/0.127/installation/deployment.html>`_.
* Web Connector for Tableau
* The following connectors:

  * Cassandra
  * Kafka
  * Redis
  * Hive-hadoop1
  * Hive-cdh4
* Developing Plugins

Beta Features
-------------
Decimal support is currently in Beta stage.

SQL/DML/DDL Limitations
-----------------------

 * The SQL keyword ``end`` is used as a column name in ``system.runtime.queries``, so in order to query from that column, ``end`` must be wrapped in quotes
 * ``NATURAL JOIN`` is not supported
 * Scalar subqueries are not supported -- e.g. ``WHERE x = (SELECT y FROM ...)``
 * Correlated subqueries are not supported
 * Non-equi joins are only supported for inner join -- e.g. ``"n_name" < "p_name"``
 * ``EXISTS``, ``EXCEPT``, and ``INTERSECT`` are not supported
 * ``ROLLUP``, ``CUBE``, ``GROUPING SETS`` are not supported
 * ``LIMIT ALL`` and ``OFFSET`` are not supported

Hive Connector Limitations
--------------------------

Teradata supports data stored in the following formats:

 * text files
 * ORC
 * RCFILE
 * PARQUET

Data types not supported
^^^^^^^^^^^^^^^^^^^^^^^^
Columns with the following types are not visible in a table through Presto:

 * ``DECIMAL``
 * ``DECIMAL`` with parentheses
 * ``CHAR``
 * ``VARCHAR`` is not supported in ORC and RCBinary file formats.

Hive to Presto data type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Presto does not map Hive data types 1-to-1:

 * All integral types are mapped to ``BIGINT``
 * ``FLOAT`` and ``DOUBLE`` are mapped to ``DOUBLE``
 * ``STRING`` and ``VARCHAR`` are mapped to ``VARCHAR``

Generally it does not have much effect but may be visible if column values are
passed to Hive UDFs, or through slight differences in mathematical operations.

Also due to mapping ``FLOAT`` values to ``DOUBLE`` result may a bit confusing to
user. If a Hive data file contains ``FLOAT`` column with value ``123.345`` it is presented
as double to user which string representation is ``123.34500122070312``.

TIMESTAMP limitations
^^^^^^^^^^^^^^^^^^^^^
Granularity of the ``TIMESTAMP`` datatype is to milliseconds in Presto, while
Hive supports microseconds

``TIMESTAMP`` values in tables are parsed according to the server's timezone. If this is not what you want, you must start Presto with UTC timezone.
To do this, set JVM timezone to UTC: ``-Duser.timezone=UTC`` and also add property in Hive connector properties file: ``hive.time-zone=UTC``.

Presto's method for declaring timestamps with/with out timezone is not sql standard. In Presto, both are declared using the word ``TIMESTAMP``, e.g. ``TIMESTAMP '2003-12-10 10:32:02.1212'`` or ``TIMESTAMP '2003-12-10 10:32:02.1212 UTC'``. The timestamp is determined to be with or without timezone depending on whether you include a time zone at the end of the timestamp. In other systems, timestamps are explicitly declared as ``TIMESTAMP WITH TIME ZONE`` or ``TIMESTAMP WITHOUT TIME ZONE`` (with ``TIMESTAMP`` being an alias for one of them). In these systems, if you declare a ``TIMESTAMP WITHOUT TIMEZONE``, and your string has a timezone at the end, it is silently ignored. If you declare a ``TIMESTAMP WITH TIME ZONE`` and no time zone is included, the string is interpreted in the user time zone.

INSERT INTO ... VALUES limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This mostly works, but the data types must be exact, i.e. must use ``2.0`` for ``double``, ``cast('2015-1-1' as date)`` for ``date``, and you must supply a value for every column.

INSERT INTO ... SELECT limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``INSERT INTO`` works but there is a problem if Hive tables have a schema for which Presto only interprets some of the columns (lets say Hive have 10 columns but presto exposes only 5 of them).
In such case when you insert into this table through Presto the generated file on HDFS will not match Hive table schema (it will contain only 5 columns) and therefore will not be readable (neither through Presto nor Hive). 
Also, if called through JDBC, executeUpdate does not return count of rows inserted.

Hive Parquet Issues
^^^^^^^^^^^^^^^^^^^
Although TEXT, ORC, RC behaved consistently, PARQUET support in Hive imposes more limitations. 

 * ``TIMESTAMP``, ``DATE``, ``BINARY`` datatypes are not supported
 * Although ``FLOAT`` column was mapped to ``DOUBLE`` through Presto the value for ``123.345`` was exposed as ``DOUBLE 123.345`` in Presto.


PostgreSQL and MySQL Connectors Limitations
===========================================

Known Bugs
^^^^^^^^^^
PostgreSQL connector ``describe table`` reports ``Table has no supported column types`` inappropriately.
`https://github.com/facebook/presto/issues/4082 <https://github.com/facebook/presto/issues/4082>`_ 

Security
^^^^^^^^
Presto connects to MySQL and PostgreSQL using the credentials specified in the properties file.  The credentials are used to authenticate the users while establishing the connection.  Presto runs queries as the "presto" service user and does not pass down user information to MySQL or PostgreSQL connectors.   

Datatypes
^^^^^^^^^
PostgreSQL and MySQL each support a wide variety of datatypes (PostgreSQL datatypes, MySQL datatypes).  Many of these types are not supported in Presto.  Table columns that are defined using an unsupported type are not visible to Presto users.  These columns are not shown when ``describe table`` or ``select *`` SQL statements are executed.

CREATE TABLE
^^^^^^^^^^^^
``CREATE TABLE (...)`` does not work, but ``CREATE TABLE AS SELECT`` does.

INSERT INTO
^^^^^^^^^^^
``INSERT INTO`` is not supported

DROP TABLE
^^^^^^^^^^
``DROP TABLE`` is not supported.

Limited SQL push-down
^^^^^^^^^^^^^^^^^^^^^
Presto does not "push-down" aggregate calculations to PostgreSQL or MySQL.  This means that when a user executes a simple query such as ``SELECT COUNT(*) FROM lineitem`` the entire table will be retrieved and the aggregate calculated by Presto.  If the table is large or the network slow, this may take a very long time.

MySQL Catalogs
^^^^^^^^^^^^^^
MySQL catalog names are mapped to Presto schema names.


Teradata ODBC Driver
--------------------

When querying columns of MAP data type, the table must be specified as schema.name or catalog.schema.name.  
Here are two examples of queries that work correctly:

.. code-block:: none

		Executing: select c_map['b'], c_map['c'] from hive.default.repro
		_col0|_col1|
		None|24|
		4|None|
		
		
		Executing: select c_map['b'], c_map['c'] from default.repro
		_col0|_col1|
		None|24|
		4|None|


If only the table name is specified, then all values in the MAP column will be returned:

.. code-block:: none

		Executing: select c_map['b'], c_map['c'] from repro
		'b'|'c'|
		{"a":2,"b":4}|{"a":2,"b":4}|
		{"c":24,"d":44}|{"c":24,"d":44}|
