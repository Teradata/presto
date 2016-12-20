=================
Release 0.157.1-t
=================

Presto 0.157.1-t is equivalent to Presto release 0.157.1, with some additional features and patches.

SQL
---
* Support correlated scalar aggregation subqueries
* Support correlated scalar subqueries
* Additional prepared statement syntax: DESCRIBE INPUT and DESCRIBE OUTPUT

Data Types
----------
* Native functions for Varchar(x) and Char(x)
* Native Decimal functions

Security
--------
* Secured cluster communication over HTTPS
* LDAP Authentication

    LDAP Authentication functionality in 0.157.1-t is same as 0.152.1-t, but the server properties have been renamed. The new properties and the only ones supported now are:


========================================== ================================================= =================================================
Property                                   Example usage in Active Directory                 Example usage in OpenLDAP
========================================== ================================================= =================================================
``http-server.authentication.type``        LDAP                                              LDAP
``authentication.ldap.url``                ldaps://ldapserver:636                            ldaps://ldapserver:636
``authentication.ldap.user-bind-pattern``  ${USER}@domain.com                                uid=${USER},ou=Asia,dc=domain,dc=com
``authentication.ldap.user-base-dn``       ou=Asia,dc=domain,dc=com                          ou=Asia,dc=domain,dc=com
``authentication.ldap.group-auth-pattern`` (&(objectClass=person)(sAMAccountName=${USER})    (&(objectClass=inetOrgPerson)(uid=${USER})
                                           (memberof=cn=group,ou=America,dc=domain,dc=com))  (memberof=cn=group,ou=America,dc=domain,dc=com))
========================================== ================================================= =================================================

* SHOW GRANTS support for Hive connector

Performance
-----------
* Automatically choose between repartitioned and replicated based on Hive table statistics. Must have ``join-distribution-type`` set to ``automatic``.
* Avoid cross joins if possible by reordering joins. Must have ``reorder-joins`` set to ``true``.
* Optimize DictionaryBlock.copyPositions()
* Faster Decimal implementation
* Move certain filters in the WHERE clauses to be executed as part of the INNER JOIN rather than a filter after the join. For example: ``SELECT * FROM t t1 JOIN t t2 ON t1.a = t2.a WHERE (t1.b+t2.b)*5 > 100000000``
* Code generation for joins with filters
* Merge non-identical windows (for the same `partition by` and `order by` but different frame)
* Optimize execution order of Windowing functions to minimize the number of times data is repartitioned. For example:

   ``wfunc1() OVER (PARTITION BY A ORDER BY B)``

   ``wfunc2() OVER (PARTITION BY C ORDER BY D)`` 

   ``wfunc3() OVER (PARTITION BY A ORDER BY B)``

   If the execution order is as written in the query, it results in partitioning the data 3 times.
   If we rearrange to execute wfunc1 -> wfunc3 -> wfunc2 then data is only partitioned twice.
   Must have ``optimizer.reorder-windows`` set to ``true``.
* Add support for running regular expression functions using the more efficent re2j-td library by setting the session variable ``regex_library`` to RE2J.  The memory footprint can be adjusted by setting ``re2j_dfa_states_limit``. Additionally, the number of times the re2j library falls back from its DFA algorithm to the NFA algorithm (due to hitting the states limit) before immediately starting with the NFA algorithm can be set with the ``re2j_dfa_retries`` session variable.

Scale
-----
Beta - Spilling to disk for aggregations and avoid the memory limits within Presto. Must have ``beta.spill-enabled`` set to ``true``.
  

Teradata JDBC Driver
--------------------
The Teradata JDBC driver does not support batch queries.

Connectors
----------
* TPC-DS connector was added
* Teradata has added official support for the Cassandra connector.

Presto Admin
------------
Removed Sudo requirement for Presto Admin. See Presto Admin upgrade documentation.

Documentation
-------------
* Query Optimizer
* Query Performance Analysis
* Presto Tuning
* Installation of Presto via Presto Admin

----


Bugs Fixed
----------
* Fix issue “Hive table is corrupt. It is declared as being bucketed, but the files do not match the bucketing declaration. The number of files in the directory (1) does not match the declared.” by fixing support for Hive bucketed tables. See option hive.multi-file-bucketing.enabled in the Presto Hive connector documentation.
* Allow empty partitions for clustered hive tables
* Fix issue “low must be less than or equal to high” that can occur with ORC and Character data.
* Fix incorrect stream property derivations from GroupIdNode
* Remove broken %w specifier for MySQL date functions
* RPM to include installation of the Memory connector.
* Fix query failures for connectors that produce non-remotely accessible splits (0.158)
* Fix non-linear performance issue when parsing certain SQL expressions. (0.158)

----
  
  
Unsupported Functionality
-------------------------

Some functionality from Presto 0.157.1 may work but is not officially supported by Teradata.

* The installation method as documented on `prestodb.io <https://prestodb.io/docs/0.157/installation/deployment.html>`_.
* Web Connector for Tableau
* The following connectors:

  * Accumulo Connector
  * Kafka Connector
  * Local File Connector
  * MongoDB Connector
  * Redis Connector

    
----


SQL/DML/DDL Limitations
-----------------------

 * The SQL keyword ``end`` is used as a column name in ``system.runtime.queries``, so in order to query from that column, ``end`` must be wrapped in quotes
 * ``NATURAL JOIN`` is not supported
 * ``LIMIT ALL`` and ``OFFSET`` are not supported

  
----

   
Hive Connector Limitations
--------------------------

**File Formats**

Teradata supports data stored in the following formats:

 * Text files
 * ORC
 * RCFILE
 * PARQUET


**TIMESTAMP limitations**

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

**INSERT INTO ... VALUES limitations**

The data types must be exact, i.e. must use ``2.0`` for ``double``, ``cast('2015-1-1' as date)`` for ``date``, and you must supply a value for every column.

**INSERT INTO ... SELECT limitations**

INSERT INTO creates unreadable data (unreadable both by Hive and Presto) if a Hive table has a schema for which Presto
only interprets some of the columns (e.g. due to unsupported data types).  This is because the generated file on HDFS
will not match the Hive table schema.

If called through JDBC, executeUpdate does not return the count of rows inserted.

**Hive Parquet Issues**

PARQUET support in Hive imposes more limitations than the other file types.

``DATE`` and ``BINARY`` datatypes are not supported


----


PostgreSQL and MySQL Connectors Limitations
-------------------------------------------

**Known Bugs**
PostgreSQL connector ``describe table`` reports ``Table has no supported column types`` inappropriately.
`https://github.com/facebook/presto/issues/4082 <https://github.com/facebook/presto/issues/4082>`_ 

**Security**

Presto connects to MySQL and PostgreSQL using the credentials specified in the properties file.  The credentials are
used to authenticate the users while establishing the connection.  Presto runs queries as the "presto" service user and
does not pass down user information to MySQL or PostgreSQL connectors.

**Datatypes**

PostgreSQL and MySQL each support a wide variety of datatypes (PostgreSQL datatypes, MySQL datatypes).  Many of these
types are not supported in Presto.  Table columns that are defined using an unsupported type are not visible to Presto
users.  These columns are not shown when ``describe table`` or ``select *`` SQL statements are executed.

**CREATE TABLE**

``CREATE TABLE (...)`` does not work, but ``CREATE TABLE AS SELECT`` does.

**DROP TABLE**

``DROP TABLE`` is not supported.

**Limited SQL push-down**

Presto does not "push-down" aggregate calculations to PostgreSQL or MySQL.  This means that when a user executes a
simple query such as ``SELECT COUNT(*) FROM lineitem`` the entire table will be retrieved and the aggregate calculated
by Presto.  If the table is large or the network slow, this may take a very long time.

**MySQL Catalogs**

MySQL catalog names are mapped to Presto schema names.

----


=====================
Release 0.157.1-t.0.1
=====================

The following has been added to 0.157.1-t.0.1:

* Set `join_distribution_type` default to `repartitioned`
* Tuning and minor improvements for when `join_distribution_type` is `automatic`
* Documentation improvements
* Fix wrong results for nested except
* Fix principal error in Hive Kerberos Authentication


=====================
Release 0.157.1-t.0.3
=====================

The following has been added to 0.157.1-t.0.3:

* Fix incorrect result that may result as part of Decimal data type coercion
* Additional documentation
