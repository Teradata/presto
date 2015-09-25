=========================
Readme for HDP VM Sandbox
=========================

This is a VirtualBox sandbox image that can be used to experiment with the Presto distribution.
The credentials for the VM are:

    |  user: **presto**
    |  password: **presto**

The image is based on CentOS 6.6 and contains the following components:

    * Presto is installed in ``/usr/lib/presto``. It is set up with Hive, TPCH and JMX connectors. Logs are located in ``/var/log/presto``.
    * ``presto-admin`` is installed in ``/opt/prestoadmin``. You can run presto-admin by executing: ``/opt/prestoadmin/presto-admin``.
    * For documentation of the Presto release embedded in the VM see: :doc:`Presto Documentation <../index>`.

In addition, the image contains:

    * The Hortonworks Hadoop distribution, version 2.3, running in pseudo distributed mode.
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

    [presto@presto-demo-hdp ~]# java -jar /home/presto/presto-cli.jar --catalog hive
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
takes a couple of minutes depending on the host machine. You can monitor the startup progress
of these services through the Ambari UI, which is accessible on port 8081 at `<http://[guest-ip-address]:8081/>`_.
In order to access Ambari from the host machine, you will need to change the network adaptor for the VM from NAT to the
Bridged Adaptor. The credentials are admin/admin.
