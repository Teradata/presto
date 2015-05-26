==============
Hive Connector
==============

The Hive connector allows querying data stored in a Hive
data warehouse. Hive is a combination of three components:

* Data files in varying formats that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database such as MySQL and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Configuration
-------------

Presto includes Hive connectors for multiple versions of Hadoop:

* ``hive-hadoop1``: Apache Hadoop 1.x
* ``hive-hadoop2``: Apache Hadoop 2.x
* ``hive-cdh4``: Cloudera CDH 4
* ``hive-cdh5``: Cloudera CDH 5

Create ``/etc/presto/catalog/hive.properties`` with the following contents
to mount the ``hive-cdh4`` connector as the ``hive`` catalog,
replacing ``hive-cdh4`` with the proper connector for your version
of Hadoop and ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive Clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``/etc/presto/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.

HDFS Configuration
^^^^^^^^^^^^^^^^^^

Presto configures the HDFS client automatically for most setups and
does not require any configuration files. In some rare cases, such
as when using federated HDFS, it may be necessary to specify additional
HDFS client options in order to access your HDFS cluster. To do so, add
the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if absolutely necessary.
We also recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

HDFS Permissions
^^^^^^^^^^^^^^^^
Before running any ``CREATE TABLE`` or ``CREATE TABLE ... AS`` statements
for Hive tables in Presto, you need to check that the operating system user
running the Presto server has access to the Hive warehouse directory on HDFS. The Hive warehouse
directory is specified by the configuration variable ``hive.metastore.warehouse.dir``
in ``hive-site.xml``, and the default value is ``/user/hive/warehouse``. If that
is not the case, either add the following to ``jvm.config`` on all of the nodes:
``-DHADOOP_USER_NAME=USER``, where ``USER`` is an operating system user that has proper
permissions for the Hive warehouse directory, or start the Presto server as a user with
similar permissions. The ``hive`` user generally works as ``USER``, since Hive is often
started with the ``hive`` user. If you run into HDFS permissions problems on
``CREATE TABLE ... AS``, remove ``/tmp/presto-*`` on HDFS, fix the user as described
above, then restart all of the Presto servers.


Configuration Properties
------------------------

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.metastore.uri``                             The URI(s) of the Hive metastore to connect to using the
                                                   Thrift protocol. If multiple URIs are provided, the first
                                                   URI is used by default and the rest of the URIs are
                                                   fallback metastores. This property is required.
                                                   Example: ``thrift://192.0.2.3:9083`` or
                                                   ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``

``hive.config.resources``                          An optional comma-separated list of HDFS
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.
                                                   Example: ``/etc/hdfs-site.xml``

``hive.storage-format``                            The default file format used when creating new tables.       ``RCBINARY``

``hive.force-local-scheduling``                    Force splits to be scheduled on the same node as the Hadoop  ``false``
                                                   DataNode process serving the split data.  This is useful for
                                                   installations where Presto is collocated with every
                                                   DataNode.

``hive.allow-drop-table``                          Allow the Hive connector to drop tables.                     ``false``

``hive.allow-rename-table``                        Allow the Hive connector to rename tables.                   ``false``

``hive.respect-table-format``                      Should new partitions be written using the existing table    ``true``
                                                   format or the default Presto format?

``hive.immutable-partitions``                      Can new data be inserted into existing partitions?           ``false``

``hive.max-partitions-per-writers``                Maximum number of partitions per writer.                     100
================================================== ============================================================ ==========

Querying Hive Tables
--------------------

The following table is an example Hive table from the `Hive Tutorial`_.
It can be created in Hive (not in Presto) using the following
Hive ``CREATE TABLE`` command:

.. _Hive Tutorial: https://cwiki.apache.org/confluence/display/Hive/Tutorial#Tutorial-UsageandExamples

.. code-block:: none

    hive> CREATE TABLE page_view (
        >   viewTime INT,
        >   userid BIGINT,
        >   page_url STRING,
        >   referrer_url STRING,
        >   ip STRING COMMENT 'IP Address of the User')
        > COMMENT 'This is the page view table'
        > PARTITIONED BY (dt STRING, country STRING)
        > STORED AS SEQUENCEFILE;
    OK
    Time taken: 3.644 seconds

Assuming that this table was created in the ``web`` schema in
Hive, this table can be described in Presto::

    DESCRIBE hive.web.page_view;

.. code-block:: none

        Column    |  Type   | Null | Partition Key |        Comment
    --------------+---------+------+---------------+------------------------
     viewtime     | bigint  | true | false         |
     userid       | bigint  | true | false         |
     page_url     | varchar | true | false         |
     referrer_url | varchar | true | false         |
     ip           | varchar | true | false         | IP Address of the User
     dt           | varchar | true | true          |
     country      | varchar | true | true          |
    (7 rows)

This table can then be queried in Presto::

    SELECT * FROM hive.web.page_view;

Character data types
--------------------

Hive supports three character data types:
 - ``STRING``
 - ``CHAR(n)``
 - ``VARCHAR(n)``

Currently columns for all those data types are exposed in presto as unparametrized ``VARCHAR`` type.
This implies semantic inconsistencies for columns defined as ``CHAR(x)`` between Hive and Presto.

Following example documents basic semantic differences:

**Create table in Hive**

.. code-block:: none

    hive> create table string_test (c char(5), v varchar(5), s string) stored as orc;
    hive> insert into string_test values ('ala', 'ala', 'ala'), ('ala ', 'ala ', 'ala ');


**Query the table in Hive**

.. code-block:: none

    hive> select concat('x', c, 'x'), concat('x', v, 'x'), concat('x', s, 'x'), length(c), length(v), length(s) from string_test;
    OK
    xalax	xalax	 xalax	 3	3	3
    xalax	xala x	 xala x	 3	4	4

**Query the table in Presto**

.. code-block:: none

    presto:default> select concat('x',c,'x'), concat('x', v, 'x'), concat('x', s, 'x'), length(c), length(v), length(s) from string_test;
      _col0  | _col1  | _col2  | _col3 | _col4 | _col5
    ---------+--------+--------+-------+-------+-------
     xala  x | xalax  | xalax  |     5 |     3 |     3
     xala  x | xala x | xala x |     5 |     4 |     4

Also for ``CHAR(x)`` datatype padding whitespace should not be taken into consideration during comparisons.
So ``'ala  '`` should be equal to ``'ala        '``. This is currently not the case in Presto.


**Note:** Ultimately Presto presto will implement native ``CHAR(x)`` data type. It will follow ANSI SQL semantics which differs from
 Hive's. This will cause backward incompatibilities of queries using Hive's ``CHAR(x)`` columns.


Custom Storage Handlers
-----------------------

Hive tables can use custom storage handlers to support alternative data formats.
To query from Hive tables that use custom storage handlers, you will need the
JARs containing the storage handler classes.  Copy the storage handler JARs to
the connector plugin directory on all nodes, restart the presto servers, and
then query the table as you would any other Hive table.  You can copy the
jar across the cluster using presto-admin's ``plugin add_jar`` command and
restart servers by using the ``server restart`` command.

For example, if the plugin directory is located at
``/usr/lib/presto/lib/plugin``, and you want to use the ``hive-hadoop2``
connector to query from a table that uses a storage handler available
in ``/tmp/my-classes.jar``:

1. Copy ``my-classes.jar`` into ``/usr/lib/presto/lib/plugin/hive-hadoop2``
   on all nodes of the cluster.
   ::

        sudo ./presto-admin plugin add_jar /tmp/my-classes.jar hive-hadoop2


2. Restart your presto-servers::

        sudo ./presto-admin server restart


Then you can query from the table as you would any other Hive table in Presto.

