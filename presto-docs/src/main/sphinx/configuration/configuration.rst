==================
Configuring Presto
==================

Configuration Files
-------------------

Create a ``/etc/presto`` directory.
This will hold the following configuration:

* Node Properties: environmental configuration specific to each node
* JVM Config: command line options for the Java Virtual Machine
* Config Properties: configuration for the Presto server
* Catalog Properties: configuration for :doc:`/connector` (data sources)

.. _presto_node_properties:

Node Properties
^^^^^^^^^^^^^^^

The node properties file, ``/etc/presto/node.properties``, contains configuration
specific to each node. A *node* is a single installed instance of Presto
on a machine. This file is typically created by the deployment system when
Presto is first installed. The following is a minimal ``node.properties``:

.. code-block:: none

    node.environment=production
    node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
    node.data-dir=/var/presto/data

The above properties are described below:

* ``node.environment``:
  The name of the environment. All Presto nodes in a cluster must
  have the same environment name.

* ``node.id``:
  The unique identifier for this installation of Presto. This must be
  unique for every node. This identifier should remain consistent across
  reboots or upgrades of Presto. If running multiple installations of
  Presto on a single machine (i.e. multiple nodes on the same machine),
  each installation must have a unique identifier.

* ``node.data-dir``:
  The location (filesystem path) of the data directory. Presto will store
  logs and other data here.

.. _presto_jvm_config:

JVM Config
^^^^^^^^^^

The JVM config file, ``/etc/presto/jvm.config``, contains a list of command line
options used for launching the Java Virtual Machine. The format of the file
is a list of options, one per line. These options are not interpreted by
the shell, so options containing spaces or other special characters should
not be quoted (as demonstrated by the ``OnOutOfMemoryError`` option in the
example below).

The following provides a good starting point for creating ``jvm.config``:

.. code-block:: none

    -server
    -Xmx16G
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+UseGCOverheadLimit
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+HeapDumpOnOutOfMemoryError
    -XX:OnOutOfMemoryError=kill -9 %p

Because an ``OutOfMemoryError`` will typically leave the JVM in an
inconsistent state, we write a heap dump (for debugging) and forcibly
terminate the process when this occurs.


.. _config_properties:

Config Properties
^^^^^^^^^^^^^^^^^

The config properties file, ``/etc/presto/config.properties``, contains the
configuration for the Presto server. Every Presto server can function
as both a coordinator and a worker, but dedicating a single machine
to only perform coordination work provides the best performance on
larger clusters.

The following is a minimal configuration for the coordinator:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

And this is a minimal configuration for the workers:

.. code-block:: none

    coordinator=false
    http-server.http.port=8080
    query.max-memory=50GB
    query.max-memory-per-node=1GB
    discovery.uri=http://example.net:8080

Alternatively, if you are setting up a single machine for testing that
will function as both a coordinator and worker, use this configuration:

.. code-block:: none

    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    query.max-memory=5GB
    query.max-memory-per-node=1GB
    discovery-server.enabled=true
    discovery.uri=http://example.net:8080

These properties require some explanation:

* ``coordinator``:
  Allow this Presto instance to function as a coordinator
  (accept queries from clients and manage query execution).

* ``node-scheduler.include-coordinator``:
  Allow scheduling work on the coordinator.
  For larger clusters, processing work on the coordinator
  can impact query performance because the machine's resources are not
  available for the critical task of scheduling, managing and monitoring
  query execution.

* ``http-server.http.port``:
  Specifies the port for the HTTP server. Presto uses HTTP for all
  communication, internal and external.

* ``query.max-memory=50GB``:
  The maximum amount of distributed memory that a query may use.

* ``query.max-memory-per-node=1GB``:
  The maximum amount of memory that a query may use on any one machine.

* ``discovery-server.enabled``:
  Presto uses the Discovery service to find all the nodes in the cluster.
  Every Presto instance will register itself with the Discovery service
  on startup. In order to simplify deployment and avoid running an additional
  service, the Presto coordinator can run an embedded version of the
  Discovery service. It shares the HTTP server with Presto and thus uses
  the same port.

* ``discovery.uri``:
  The URI to the Discovery server. Because we have enabled the embedded
  version of Discovery in the Presto coordinator, this should be the
  URI of the Presto coordinator. Replace ``example.net:8080`` to match
  the host and port of the Presto coordinator. This URI must not end
  in a slash.

Log Levels
^^^^^^^^^^

The optional log levels file, ``/etc/presto/log.properties``, allows setting the
minimum log level for named logger hierarchies. Every logger has a name,
which is typically the fully qualified name of the class that uses the logger.
Loggers have a hierarchy based on the dots in the name (like Java packages).
For example, consider the following log levels file:

.. code-block:: none

    com.facebook.presto=INFO

This would set the minimum level to ``INFO`` for both
``com.facebook.presto.server`` and ``com.facebook.presto.hive``.
The default minimum level is ``INFO``
(thus the above example does not actually change anything).
There are four levels: ``DEBUG``, ``INFO``, ``WARN`` and ``ERROR``.

Catalog Properties
^^^^^^^^^^^^^^^^^^

Presto accesses data via *connectors*, which are mounted in catalogs.
The connector provides all of the schemas and tables inside of the catalog.
For example, the Hive connector maps each Hive database to a schema,
so if the Hive connector is mounted as the ``hive`` catalog, and Hive
contains a table ``clicks`` in database ``web``, that table would be accessed
in Presto as ``hive.web.clicks``.

Catalogs are registered by creating a catalog properties file
in the ``/etc/presto/catalog`` directory.
For example, create ``/etc/presto/catalog/jmx.properties`` with the following
contents to mount the ``jmx`` connector as the ``jmx`` catalog:

.. code-block:: none

    connector.name=jmx

See :doc:`/connector` for more information about configuring connectors.

