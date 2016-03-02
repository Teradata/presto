===========================
Hive Security Configuration
===========================

When set up without specific configuration options, the :doc:`/connector/hive`
can connect to a Hadoop cluster that has been configured with ``simple``
authentication. All queries are executed as the user who runs the Presto
process, regardless of the end user.

The Hive connector provides addional security options to support Hadoop
clusters that have been configured to user Kerberos for greater security.


when accessing HDFS, Presto can impersonate the end user who is running the
query.

Kerberos Support
================

In order to use the Hive connector with a Hadoop cluster that uses ``kerberos``
authentication, you will need to configure the connector to work with two
services on the Hadoop cluster:

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
``hive.metastore.authentication.type``             One of ``SIMPLE`` or ``SASL``. When set to ``SASL`` the Hive ``SIMPLE``
                                                   connector will connect to the Hive metastore Thrift service
                                                   using SASL and authenticate with Kerberos. When using the
                                                   default value of ``SIMPLE``, Kerberos authentication is
                                                   disabled and no other properties need to be configured.

``hive.metastore.principal``                       The Kerberos principal of the Hive metastore. The Presto     
                                                   coordinator will use this to authenticate the Hive
                                                   metastore.

``hive.metastore.presto.principal``                The Kerberos principal that Presto will use when connecting
                                                   to the Hive metastore.

``hive.metastore.presto.keytab``                   The path to the keytab file that contains a key for the
                                                   principal specified by hive.metastore.presto.principal.

================================================== ============================================================ ==========

``SIMPLE`` authentication
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.metastore.authentication.type=SIMPLE

``SIMPLE`` authentication is the default authentication mechanism for the Hive
metastore. When using ``SIMPLE`` authentication, Presto connects to an
unsecured Hive metastore. Kerberos is not used.

``SASL`` authentication
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.metastore.authentication.type=SASL
    hive.metastore.principal=hive/_HOST@EXAMPLE.COM
    hive.metastore.presto.principal=presto@EXAMPLE.COM
    hive.metastore.presto.keytab=/etc/presto/hive.keytab
    
When SASL is enabled for the Hive metastore Thrift service, Presto will connect
as the principal specified by the property ``hive.metastore.presto.principal``.
Presto will authenticate using the keytab specified by
``hive.metastore.presto.keytab``, and will verify that the identity of the
metastore matches ``hive.metastore.principal``. The special value _HOST is
replaced at runtime with the fully qualified domain name of the host Presto is
running on.

Keytab files must be distributed to every node in the cluster.

:ref:`Additional information on keytab files.<hive-security-additional-keytab>`

Hive HDFS Authentication
------------------------

In a Kerberized Hadoop cluster, access to the Hadoop Distributed File System
(HDFS) is authenticated using Kerberos. Presto can operate in several modes,
which are selected using the ``hive.hdfs.authentication.type`` property. The
list of properties that are involved in HDFS authentication is given below;
a complete discussion of the modes follows.

================================================== ============================================================ ==========
Property Name                                      Description                                                  Default
================================================== ============================================================ ==========
``hive.hdfs.authentication.type``                  One of ``SIMPLE`` or ``KERBEROS``. When set to ``KERBEROS``, ``SIMPLE``
                                                   the Hive connector authenticates to HDFS using Kerberos.
                                                   When using the default value of ``SIMPLE``, Kerberos
                                                   authentication is disabled and no other properties need to
                                                   be configured.

``hive.hdfs.presto.principal``                     When ``hive.hdfs.authentication.type`` is set to
                                                   ``KERBEROS``, the Kerberos principal that Presto will use
                                                   when connecting to HDFS.

``hive.hdfs.presto.keytab``                        The path to the keytab file that contains a key for the
                                                   principal specified by hive.hdfs.presto.principal.

================================================== ============================================================ ==========

.. _hive-security-simple:

``SIMPLE``
^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=SIMPLE

When using ``SIMPLE`` authentication, Presto accesses HDFS as the user the
Presto process is running as. This is the default authentication type, and no
additional properties need to be set. Kerberos is not used.

.. _hive-security-kerberos:

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

Keytab files must be distributed to every node in the cluster.

:ref:`Additional information on keytab files.<hive-security-additional-keytab>`

End user impersonation
======================

Impersonation Accessing HDFS
----------------------------

Presto can impersonate the end user who is running a query. In the case of a
user running a query from the command line interface, this is the value of the
``--user`` option. Impersonating the end user can provide additional security
when accessing HDFS. 

.. _hive-security-simple-impersonation:

``SIMPLE`` authentication with HDFS impersonation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=SIMPLE
    hive.hdfs.impersonation=true

When using ``SIMPLE`` authentication with impersonation, Presto impersonates
the user who is running the query when accessing HDFS. The user Presto is
running as must be allowed to impersonate this user, as discussed in the
section :ref:`configuring-hadoop-impersonation`.

.. _hive-security-kerberos-impersonation:

``KERBEROS`` authentication with HDFS impersonation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none

    hive.hdfs.authentication.type=KERBEROS
    hive.hdfs.impersonation=true
    hive.hdfs.presto.principal=presto@EXAMPLE.COM
    hive.hdfs.presto.keytab=/etc/presto/hdfs.keytab

When using ``KERBEROS`` authentication with impersonation, Presto impersonates
the user who is running the query when accessing HDFS. The principal
specified by the ``hive.hdfs.presto.principal`` property must be allowed to
impersonate this user, as discussed in the section
:ref:`configuring-hadoop-impersonation`. Presto authenticates
``hive.hdfs.presto.principal`` using the keytab specified by the
``hive.hdfs.presto.keytab`` keytab.

.. _configuring-hadoop-impersonation:

Impersonation Accessing the Hive Metastore
------------------------------------------

Presto does not currently support impersonating the end user when accessing the
Hive metastore.

Configuring Impersonation in Hadoop
-----------------------------------

In order to use :ref:`hive-security-simple-impersonation` or
:ref:`hive-security-kerberos-impersonation`, the Hadoop cluster must be
configured to allow the user or principal that Presto is running as to
impersonate the users who log in to Presto. Impersonation in Hadoop is
configured in the file :file:`core-site.xml`. A complete description of the
configuration options can be found in the `Hadoop documentation
<https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations>`_.

.. _hive-security-additional-keytab:

Additional Information About Keytab Files
=========================================

Keytab files contain encryption keys that are used to authenticate principals
to the Kerberos Key Distribution Center. These encryption keys must be stored
securely; you should take the same precautions to protect them that you would
to protect ssh private keys.

In particular, access to keytab files should be limited to the accounts that
actually need to use them to authenticate. In practice, this is the user that
the presto process runs as. The ownership and permissions on keytab files
should be set to prevent other users from reading or modifying the files.

Keytab files need to be distributed to every node in the cluster. Under common
deployment situations, the hive configuration will be the same on all nodes.
This means that the keytab needs to be in the same location on every node.

You should ensure that the keytab files have the correct permissions on every
node after distributing them.
