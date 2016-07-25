======================================
Installing a Presto VM Sandbox for CDH
======================================

The following link is to a VirtualBox sandbox image that can be used to experiment with the Presto distribution.

| Download **presto-0.141t-demo-cdh.ova** from https://www.teradata.com/presto

The credentials for the VM are:

    |  user: **presto**
    |  password: **presto**

The image is based on CentOS 6.7 and contains the following components:

    * Presto is installed in ``/usr/lib/presto``. It is set up with Hive, TPCH and JMX connectors. Logs are located in ``/var/log/presto``.
    * ``presto-admin`` is installed in ``/opt/prestoadmin``. You can run presto-admin by executing: ``/opt/prestoadmin/presto-admin``.
    * For documentation of the Presto release embedded in the VM see: :doc:`Presto Documentation <../index>`.

In addition, the image contains:

    * The Cloudera Hadoop distribution, version 5.4, running in pseudo-distributed mode.
    * Hive (set up to use YARN).
    * Hive Metastore.
    * Zookeeper (used by Hive).
    * MySQL (used by Hive Metastore).

The following sample tables are loaded to HDFS and are visible from Hive:

    * TPC-H nation table.
    * TPC-H region table.

The ``presto-cli`` executable JAR can be found in ``/home/presto`` and should be used to execute Presto queries.
Please wait a few moments after the image boots for the Presto service to start.


Usage example: ::

    [presto@presto-demo-cdh ~]# java -jar /home/presto/presto-cli.jar --catalog hive --schema default
    show tables;
     Table
    --------
     nation
     region
    (2 rows)

    presto:default> select * from nation;
    ...

JDBC Driver
===========

You can also find the Presto JDBC JAR on the VM (``/home/presto/presto-jdbc.jar``). It can be used
to connect to the Presto server running in the VM from your Java application.

Hadoop Services Startup
=======================

After the VM boots, some Hadoop services may still not be started. Starting them
takes a couple of minutes depending on the host machine.

VM Networking
=============

By default, the VM is configured to use NAT networking and will have an IP of 10.0.2.15 in VirtualBox.
For some systems with local firewalls or connected to VPN this may result in being unable to reach the presto
and various hadoop web UI pages.

To resolve this, enable port forwarding via the VirtualBox UI by navigating to your VM -> settings -> Network -> Advanced.
Click on the 'Port Forwarding' button and configure the ports you would like to forward from your local machine to the VM.
For example, 127.0.0.1:50070 -> 10.0.2.15:50070 will enable access to the hadoop name node web UI.
