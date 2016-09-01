=====================
Software Requirements
=====================

**Operating System**

* RHEL 6.x (Red Hat Enterprise Linux)
* CentOS equivalent to RHEL (Community ENTerprise Operating System)
* SLES 11 SP3 (SUSE Linux Enterprise Server) **Teradata Support for SLES on Teradata Hadoop Appliances only**
  
**Hadoop Distributions**

* CDH (Cloudera Distribution Including Apache Hadoop) 5.x
* HDP (Hortonworks Data Platform) 2.x
* IBM BigInsights for Apache Hadoop 4.1

**Java**

* Oracle Java JRE 1.8 Update 60+ (64-bit)

**Python**

* Python 2.6.x OR
* Python 2.7.x

**SSH Configuration**

* Passwordless SSH from the node running ``presto-admin`` to the nodes where Presto will be installed OR
* Ability to SSH with a password from the node running ``presto-admin`` to the nodes where Presto will be installed

For more on SSH configuration, see :ref:`ssh-configuration-label`.

**Other Configuration**

* Sudo privileges on both the node running ``presto-admin`` and the nodes where Presto will be installed are required for a non-root presto-admin user.
* The number of open file descriptors and user processes for the ``presto`` user should be set to the following values in ``/etc/security/limits.conf``.

.. code-block:: none

    presto soft nofile 32768
    presto hard nofile 65536

    presto soft nproc 32768
    presto hard nproc 65536

If you see this error "java.lang.OutOfMemoryError: unable to create new native thread" then install the :doc:`/connector/jmx` and run the following query to verify the limit for open file descriptors for your system.

.. code-block:: sql

    SELECT openfiledescriptorcount, maxfiledescriptorcount FROM jmx.current."java.lang:type=operatingsystem"
