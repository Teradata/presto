==========================================
Unsupported Features and Known Limitations
==========================================

Presto 148t is equivalent to Presto release 0.148, with some additional features. Below
are documented unsupported features and known limitations.

Unsupported Functionality
=========================

Some functionality from Presto 0.148 may work but is not officially supported by Teradata.

* The installation method as documented on `prestodb.io <https://prestodb.io/docs/0.148/installation/deployment.html>`_.
* Web Connector for Tableau
* The following connectors:
  * Cassandra Connector
  * Kafka Connector
  * Local File Connector
  * MongoDB Connector
  * Redis Connector
* Developing Plugins

SQL/DML/DDL Limitations
=======================

 * The SQL keyword ``end`` is used as a column name in ``system.runtime.queries``, so in order to query from that column, ``end`` must be wrapped in quotes
 * ``NATURAL JOIN`` is not supported
 * ``EXISTS`` and ``EXCEPT`` are not supported
 * ``LIMIT ALL`` and ``OFFSET`` are not supported
 * Correlated Subqueries

Hive Connector Limitations
==========================

Teradata supports data stored in the following formats:

 * Text files
 * ORC
 * RCFILE
 * PARQUET

TIMESTAMP limitations
^^^^^^^^^^^^^^^^^^^^^
Presto supports a granularity of milliseconds for the ``TIMESTAMP`` datatype, while Hive
supports microseconds.

``TIMESTAMP`` values in tables are parsed according to the server's timezone. If this is not what you want, you must
start Presto in the UTC timezone. To do this, set the JVM timezone to UTC: ``-Duser.timezone=UTC`` and also add the
following property in  the Hive connector properties file: ``hive.time-zone=UTC``.

Presto's method for declaring timestamps with/with out timezone is not sql standard. In Presto, both are declared using
the word ``TIMESTAMP``, e.g. ``TIMESTAMP '2003-12-10 10:32:02.1212'`` or ``TIMESTAMP '2003-12-10 10:32:02.1212 UTC'``.
The timestamp is determined to be with or without timezone depending on whether you include a time zone at the end of
the timestamp. In other systems, timestamps are explicitly declared as ``TIMESTAMP WITH TIME ZONE`` or
``TIMESTAMP WITHOUT TIME ZONE`` (with ``TIMESTAMP`` being an alias for one of them). In these systems, if you declare a
``TIMESTAMP WITHOUT TIMEZONE``, and your string has a timezone at the end, it is silently ignored. If you declare a
``TIMESTAMP WITH TIME ZONE`` and no time zone is included, the string is interpreted in the user time zone.

INSERT INTO ... VALUES limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The data types must be exact, i.e. must use ``2.0`` for ``double``, ``cast('2015-1-1' as date)`` for ``date``, and you must supply a value for every column.

INSERT INTO ... SELECT limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
INSERT INTO creates unreadable data (unreadable both by Hive and Presto) if a Hive table has a schema for which Presto
only interprets some of the columns (e.g. due to unsupported data types).  This is because the generated file on HDFS
will not match the Hive table schema.

If called through JDBC, executeUpdate does not return the count of rows inserted.

Hive Parquet Issues
^^^^^^^^^^^^^^^^^^^
PARQUET support in Hive imposes more limitations than the other file types.

 * ``DATE`` and ``BINARY`` datatypes are not supported


PostgreSQL and MySQL Connectors Limitations
===========================================

Known Bugs
^^^^^^^^^^
| PostgreSQL connector ``describe table`` reports ``Table has no supported column types`` inappropriately.
| `https://github.com/facebook/presto/issues/4082 <https://github.com/facebook/presto/issues/4082>`_ 

Security
^^^^^^^^
Presto connects to MySQL and PostgreSQL using the credentials specified in the properties file.  The credentials are
used to authenticate the users while establishing the connection.  Presto runs queries as the "presto" service user and
does not pass down user information to MySQL or PostgreSQL connectors.

Datatypes
^^^^^^^^^
PostgreSQL and MySQL each support a wide variety of datatypes (PostgreSQL datatypes, MySQL datatypes).  Many of these
types are not supported in Presto.  Table columns that are defined using an unsupported type are not visible to Presto
users.  These columns are not shown when ``describe table`` or ``select *`` SQL statements are executed.

CREATE TABLE
^^^^^^^^^^^^
| ``CREATE TABLE (...)`` does not work, but ``CREATE TABLE AS SELECT`` does.

INSERT INTO
^^^^^^^^^^^
| ``INSERT INTO`` is not supported

DROP TABLE
^^^^^^^^^^
| ``DROP TABLE`` is not supported.

Limited SQL push-down
^^^^^^^^^^^^^^^^^^^^^
Presto does not "push-down" aggregate calculations to PostgreSQL or MySQL.  This means that when a user executes a
simple query such as ``SELECT COUNT(*) FROM lineitem`` the entire table will be retrieved and the aggregate calculated
by Presto.  If the table is large or the network slow, this may take a very long time.

MySQL Catalogs
^^^^^^^^^^^^^^
MySQL catalog names are mapped to Presto schema names.


Teradata JDBC Driver
====================
The Teradata JDBC driver does not support batch queries.

