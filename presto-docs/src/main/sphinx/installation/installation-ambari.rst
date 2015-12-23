**********************************
Automated Installation with Ambari
**********************************

Presto can be installed using Apache Ambari. To install Presto using Ambari you need to have the `Presto Ambari Integration package <https:www.teradata.com/presto>`_.

.. contents:: Getting Started

Requirements for Integration
----------------------------
1. You must have Ambari installed and thus transitively fulfill `Ambari's requirements <http://docs.hortonworks.com/HDPDocuments/Ambari-2.1.2.1/bk_Installing_HDP_AMB/content/_meet_minimum_system_requirements.html>`_.
2. Oracle Java JDK 1.8 (64-bit). Note that when installing Ambari you will be prompted to pick a JDK. You can tell Ambari to download Oracle JDK 1.8 or point it to an existing installation. Presto picks up whatever JDK Ambari was installed with so it is imperative that Ambari is running on Oracle JDK 1.8.
3. Disable `requiretty`. On RHEL 6.x this can be done by editing the `/etc/sudoers` file and commenting out `Defaults    requiretty`.
4. Install `wget` on all nodes that will run a Presto component.

Adding the Presto service
-------------------------
This section and all others that follow walk you through the integration steps needed to get Presto working with Ambari. By default, this integration code installs the latest Teradata Presto release (0.127t).

To integrate the Presto service with Ambari, follow the steps outlines below:

* Assuming HDP 2.3 was installed with Ambari, create the following directory on the node where the ``ambari-server`` is running:

.. code-block:: bash

	$ mkdir /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO
	$ cd /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO

* Place the integration files within the newly created PRESTO directory by uploading the integration archive to your cluster and extracting it like so:

.. code-block:: bash

	$ tar -xvf /path/to/integration/package/ambari-presto-0.1.0.tar.gz -C /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO
	$ mv /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO/ambari-presto-0.1.0/* /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO
	$ rm -rf /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO/ambari-presto-0.1.0

* Finally, make all integration files executable and restart the Ambari server:

.. code-block:: bash

	$ chmod -R +x /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO/*
	$ ambari-server restart

* Once the server has restarted, point your browser to it and on the main Ambari Web UI page click the ``Add Service`` button and follow the on screen wizard to add Presto. The following sections provide more details on the options and choices you will make when adding Presto.

Supported Topologies
--------------------
The following two screens will allow you to assign the Presto processes among the nodes in your cluster. Once you pick a topology for Presto and finish the installation process it is impossible to modify that topology.

Presto is composed of a coordinator and worker processes. The same code runs all nodes because the same Presto server RPM is installed for both workers and coordinator. It is the configuration on each node that determines how a particular node will behave. Presto can run in pseudo-distributed mode, where a single Presto process on one node acts as both coordinator and worker, or in distributed mode, where the Presto coordinator runs on one node and the Presto workers run on other nodes.

The client component of Presto is the ``presto-cli`` executable JAR. You should place it on all nodes where you expect to access the Presto server via this command line utility. The ``presto-cli`` executable JAR does not need to be co-located with either a worker or a coordinator, it can be installed on its own. Once installed, the CLI can be found at ``/usr/lib/presto/bin/presto-cli``.

**Do not place a worker on the same node as a coordinator**. Such an attempt will fail the installation because the integration software will attempt to install the RPM twice. In order to schedule work on the Presto coordinator, effectively turning the process into a dual worker/coordinator, please enable the ``node-scheduler.include-coordinator`` toggle available in the configuration screen.

Pseudo-distributed
^^^^^^^^^^^^^^^^^^
Pick a node for the Presto coordinator and **do not assign any Presto workers**. On the configuration screen that follows, you must also enable ``node-scheduler.include-coordinator`` by clicking the toggle.

Distributed
^^^^^^^^^^^

Pick a node for the Presto coordinator and assign as many Presto workers to nodes as you'd like. Feel free to also place the client component on any node. Remember to not place a worker on the same node as a coordinator.

.. _configuring-presto-label:

Configuring Presto
------------------
The one configuration property that does not have a default and requires input is ``discovery.uri``. The expected value is ``http://<FQDN-of-node-hosting-coordinator>:8081``. Note that it is http and not https and that the port is 8081. If you change the value of ``http-server.http.port``, make sure to also change it in ``disovery.uri``.

Some of the most popular properties are displayed in the Settings tab (open by default). In the Advanced tab, set custom properties by opening up the correct drop down and specifying a key and a value. Note that specifying a property that Presto does not recognize will cause the installation to finish with errors as some or all servers fail to start.

Change the Presto configuration after installation by selecting the Presto service followed by the Configs tab. After changing a configuration option, make sure to restart Presto for the changes to take effect.

Adding and removing connectors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To add a connector modify the ``connectors.to.add`` property, whose format is the following: 

``{'connector1': ['key1=value1', 'key2=value2', etc.], 'connector2': ['key3=value3', 'key4=value4'], etc.}``. 

Note the single quotes around the whole property and around each individual element. This property only adds connectors and will not delete connectors. Thus, if you add connector1, save the configuration, restart Presto, then specify {} for this property, connector1 will not be deleted.

To delete a connector modify the ``connectors.to.delete`` property, whose format is the following: 

``'['connector1', 'connector2', etc.]'``. 

Again, note the single quotes around the whole property and around each element. The above value will delete connectors ``connector1`` and ``connector2``. Note that the ``tpch`` connector cannot be deleted because it is used to smoketest Presto after it starts. The impact that the presence of the ``tpch`` connector has on the system is negligible.

Known issues
============

* For some older versions of Presto, when attempting to ``CREATE TABLE`` or ``CREATE TABLE AS`` using the Hive connector, you may run into the following error:

.. code-block:: none

   Query 20151120_203243_00003_68gdx failed: java.security.AccessControlException: Permission denied: user=hive, access=WRITE, inode="/apps/hive/warehouse/nation":hdfs:hdfs:drwxr-xr-x
		at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:319)
		at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:219)
		at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:190)
		at org.apache.hadoop.hdfs.server.namenode.FSDirectory.checkPermission(FSDirectory.java:1771)

To work around the issue, edit your ``jvm.config`` settings by adding the following property ``-DHADOOP_USER_NAME=hive``. This problem affects Presto ``0.115t`` but does not affect ``0.127t``. After saving your edit to ``jvm.config``, don't forget to restart all Presto components in order for the changes to take effect.

* If you decide to deploy an older version of Presto, you may have to adjust some setting manually. Please see :ref:`configuring-presto-label` for an explanation of how to add custom settings. For example, the ``task.max-memory`` setting was deprecated in ``0.127t`` but is valid in ``0.115t``. Therefore, if you're installing ``0.115t`` and would like to change ``task.max-memory`` to something other than its default, add it as a custom property.

* On the Presto service home page, if you click on 'Presto workers', you will get an incorrect list of workers. This is a known issue and has been fixed in Ambari 2.2.0.
