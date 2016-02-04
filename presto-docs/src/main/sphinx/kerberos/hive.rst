=====================
Hive Kerberos Support
=====================

The :doc:`/connector/hive` supports connecting to an instance of Hive running on
a Hadoop cluster that has Kerberos authentication enabled. There are two
services on the Hadoop cluster that can be configured to require Kerberos
authentication:

* The Hive metastore Thrift service
* The Hadoop Distributed File System (HDFS)

Access to these services by the Hive connector is configured in the properties
file that contains the general Hive connector configuration.

Hive Metastore Thrift Service
-----------------------------

In a Kerberized Hadoop cluster, access to the Hive metastore Thrift service is
done using the Simple Authentication and Security Layer (SASL). Kerberos
authentication for the metastore is configured in the connector's properties
file using the following properties:

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.metastore.sasl.enabled``                    When set to ``true`` the Hive connector will connect to the  ``false``
                                                   Hive metastore Thrift service using SASL and authenticate
                                                   with Kerberos. When using the default value of ``false``,
                                                   no Kerberos authentication is disabled and no other
                                                   properties need to be configured.

``hive.metastore.principal``                       The Kerberos principal of the Hive metastore. The Presto     
                                                   coordinator will use this to authenticate the Hive
                                                   metastore.

``hive.metastore.presto.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore.

``hive.metastore.presto.keytab``                   The path to the keytab file that contains a key for the
                                                   principal specified by hive.metastore.presto.principal.

================================================== ============================================================ ==========

Example Configuration
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.metastore.sasl.enabled=true
    hive.metastore.principal=hive/_HOST@EXAMPLE.COM
    hive.metastore.presto.principal=hive/_HOST@EXAMPLE.COM
    hive.metastore.presto.keytab=/etc/presto/hive.keytab
    
When SASL is enabled for the Hive metastore Thrift service, Presto will connect
as the principal specified by the property ``hive.metastore.presto.principal``.
Presto will authenticate using the keytab specified by
``hive.metastore.presto.keytab``, and will verify that the identity of the
metastore matches ``hive.metastore.principal``.  The special value _HOST is
replaced at runtime with the fully qualified domain name of the host Presto is
running on.

Hive HDFS Access
----------------

In a Kerberized Hadoop cluster, access to the Hadoop Distributed File System
(HDFS) is authenticated using Kerberos. Presto can operate in several modes,
which are selected using the ``hive.hdfs.authentication.type`` property. The
list of properties that are involved in HDFS authentication is given below;
a complete discussion of the modes follows.

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.hdfs.authentication.type``                  One of ``SIMPLE``, ``SIMPLE_IMPERSONATION``, ``KERBEROS``,   ``SIMPLE``
                                                   or ``KERBEROS_IMPERSONATION``. See below for a description
                                                   of each.

``hive.hdfs.presto.principal``                     When ``hive.hdfs.authentication.type`` is set to
                                                   ``KERBEROS`` or ``KERBEROS_IMPERSONATION``, the Kerberos
                                                   principal that Presto will use when connecting to HDFS.
                                                   When using ``KERBEROS_IMPERSONATION``, this principal must
                                                   be allowed to impersonate the users who connect to Presto.

``hive.hdfs.presto.keytab``                        The path to the keytab file that contains a key for the
                                                   principal specified by hive.hdfs.presto.principal.

================================================== ============================================================ ==========

.. _kerberos-hive-simple:

``SIMPLE``
^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=SIMPLE

When using ``SIMPLE`` authentication, Presto accesses HDFS as the user the
Presto process is running as. This is the default authentication type, and no
additional properties need to be set. Kerberos is not used.

.. _kerberos-hive-simple-impersonation:

``SIMPLE_IMPERSONATION``
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=SIMPLE_IMPERSONATION

When using ``SIMPLE_IMPERSONATION`` authentication, Presto accesses HDFS as the
user who owns the Presto session. The user Presto is running as must be able to
impersonate this user, as discussed in the section
:ref:`configuring-hadoop-impersonation`. Kerberos is not used.

.. _kerberos-hive-kerberos:

``KERBEROS``
^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS
    hive.hdfs.presto.principal=hdfs@EXAMPLE.COM
    hive.hdfs.presto.keytab=/etc/presto/hdfs.keytab

When using ``KERBEROS`` authentication, Presto accesses HDFS as the principal
specified by the ``hive.hdfs.presto.principal`` property. Presto authenticates
this principal using the keytab specified by the ``hive.hdfs.presto.keytab``
keytab.

.. _kerberos-hive-kerberos-impersonation:

``KERBEROS_IMPERSONATION``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS_IMPERSONATION
    hive.hdfs.presto.principal=hdfs@EXAMPLE.COM
    hive.hdfs.presto.keytab=/etc/presto/hdfs.keytab

When using ``KERBEROS_IMPERSONATION`` authentication, Presto accesses HDFS as
the principal who owns the Presto session. The principal specified by the
``hive.hdfs.presto.principal`` property must be able to impersonate this user,
as discussed in the section :ref:`configuring-hadoop-impersonation`. Presto
authenticates ``hive.hdfs.presto.principal`` using the keytab specified by the
``hive.hdfs.presto.keytab`` keytab.

.. _configuring-hadoop-impersonation:

Configuring Impersonation in Hadoop
-----------------------------------

In order to use :ref:`kerberos-hive-simple-impersonation` or
:ref:`kerberos-hive-kerberos-impersonation`, the Hadoop cluster must be
configured to allow the user or principal that Presto is running as to
impersonate the users who log in to Presto. Impersonation in Hadoop is
configured in the file :file:`core-site.xml`. A complete description of the
configuration options can be found in the `Hadoop documentation
<https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations>`_.
