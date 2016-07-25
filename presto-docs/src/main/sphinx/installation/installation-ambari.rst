***********************************
Automated Installation on a Cluster
***********************************

You can use Apache Ambari to install Presto. To install Presto using Ambari, you must have the `Presto Ambari Integration package <https:www.teradata.com/presto>`_.

The following procedures **do not** describe how to use Ambari to install Presto on YARN-based clusters.
For those procedures, see: :doc:`Automated Installation with Ambari 2.1 with YARN integration <installation-yarn-automated>`.

.. contents:: Installing Presto with Ambari

Pre-Requisites
--------------

1. Red Hat Enterprise Linux 6.x (64-bit) or CentOS equivalent.
2. You must have Ambari installed and thus transitively fulfill `Ambari's requirements <http://docs.hortonworks.com/HDPDocuments/Ambari-2.1.2.1/bk_Installing_HDP_AMB/content/_meet_minimum_system_requirements.html>`_.
3. Oracle Java JDK 1.8 (64-bit). Note that when installing Ambari you will be prompted to 
   select a JDK. You can tell Ambari to download Oracle JDK 1.8 or point Ambari to an 
   existing installation. Presto picks up the JDK Ambari was installed with so it is 
   imperative that Ambari is running on Oracle JDK 1.8.
4. Disable ``requiretty``. On RHEL 6.x this can be done by editing the ``/etc/sudoers`` 
   file and commenting out ``Defaults    requiretty``.
5. Install ``wget`` on all nodes that will run a Presto component.

-----

Adding the Presto Service
-------------------------

This section and the following sections walk you through the integration steps needed to 
get Presto working with Ambari. By default, this integration code installs the latest 
Teradata Presto release.

To integrate the Presto service with Ambari:

1. Assuming HDP 2.3 was installed with Ambari, create the following directory on the node 
   where the ``ambari-server`` is running:

   .. code-block:: bash

	   $ mkdir /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO
	   $ cd /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO

2. Place the integration files within the newly created PRESTO directory by uploading the 
   integration archive to your cluster and extracting it as follows:

   .. code-block:: bash

	   $ tar -xvf /path/to/integration/package/ambari-presto-0.1.0.tar.gz -C /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO
	   $ mv /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO/ambari-presto-0.1.0/* /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO
	   $ rm -rf /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO/ambari-presto-0.1.0

3. Make all integration files executable and restart the Ambari server:

   .. code-block:: bash

	   $ chmod -R +x /var/lib/ambari-server/resources/stacks/HDP/2.3/services/PRESTO/*
	   $ ambari-server restart

4. After the server restarts, point your browser to it.

5. On the main Ambari Web UI page, click the ``Add Service`` button and follow the 
   on screen wizard to add Presto. 
   
   The following sections provide more details about the options and choices you 
   need to make when adding Presto to the cluster.

-----

Assigning Presto Processes to Nodes in a Cluster
------------------------------------------------

Presto is composed of a coordinator and worker processes. The same code runs all nodes 
because the same Presto server RPM is installed for both workers and coordinator. The  
configuration on each node determines how a particular node behaves. 

Presto can run in distributed mode or psuedo-distributed mode. 

* In distributed mode, the Presto coordinator runs on one node and the Presto workers 
  run on other nodes. Distributed mode is recommended.

* In pseudo-distributed mode, a single Presto process on one node acts as both coordinator 
  and worker. Pseudo-distributed mode is not recommended for production nodes.

When you select a topology for Presto and finish the installation procedures as described 
below, **you cannot modify that topology**.

The client component of Presto is the ``presto-cli`` executable JAR. During or after 
installation, place the client component of Presto on any node all nodes where you expect 
to access the Presto server with this command line utility. The ``presto-cli`` executable 
JAR does not need to be co-located with either a worker or a coordinator. It can be installed 
on its own. The CLI can be found at ``/usr/lib/presto/bin/presto-cli``.

After you add the Presto Service as described in the previous section, two screens on the 
main Ambari WebUI page allow you to assign the Presto processes among the nodes in your cluster.

1. Assign the Presto processes among the nodes in your cluster:

   a. Select a node for the Presto coordinator.
   b. Assign as many Presto workers to nodes as you need.
      **Do not place a worker on the same node as a coordinator.** Doing so causes 
      the installation to fail because the integration software will attempt to 
      install the RPM twice. 

2. Place the client component on all nodes where you expect to access the Presto server 
   with this command line utility.

3. If you would like to schedule work on the Presto coordinator, effectively turning the 
   process into a dual worker/coordinator, enable the ``node-scheduler.include-coordinator`` 
   toggle available in the configuration screen.

-----

.. _configuring-presto-label:

Configuring Presto
------------------

The one configuration property that does not have a default and requires input is 
``discovery.uri``. The expected value is ``http://<FQDN-of-node-hosting-coordinator>:8081``. 
Note that it is ``http`` and not ``https`` and that the port is ``8081``. If you change the 
value of ``http-server.http.port``, make sure you also change it in ``disovery.uri``.

Some of the most popular properties are displayed in the Settings tab (open by default). In 
the Advanced tab, set custom properties by opening up the correct drop down and specifying a 
key and a value. Note that specifying a property that Presto does not recognize will cause 
the installation to finish with errors because some or all servers fail to start.

You can change the Presto configuration after installation by doing the following:

1. In Ambari, select the Presto service.
2. Select the Configs tab. 
3. Change a configuration option.
4. Restart Presto for the changes to take effect.

If you are running a version of Ambari that is older than 2.1 (version less than 2.1), 
you must omit the memory suffix (GB) when setting the following memory-related configurations: 

* ``query.max-memory-per-node``
* ``query.max-memory``

For these two properties, the memory suffix is automatically added by the integration software. 
For all other memory-related configurations that you add as custom properties, you must 
include the memory suffix when specifying the value.

Adding and Removing Connectors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a connector, modify the ``connectors.to.add`` property, whose format is the following:

``{'connector1': ['key1=value1', 'key2=value2', etc.], 'connector2': ['key3=value3', 'key4=value4'], etc.}``

Note the single quotes around each individual element. This property only adds connectors and 
does not delete connectors. If you add connector1, save the configuration, restart Presto, and 
then specify {} for this property, connector1 will not be deleted. If you specify incorrect values 
in your connector settings, for example, setting the ``hive.metastore.uri`` in the Hive connector 
to point to an invalid hostname, then Presto will fail to start.

For example, to add the Hive and Kafka connectors, set the ``connectors.to.add`` property to:

.. code-block:: none

  {
      'hive': ['connector.name=hive-cdh4', 'hive.metastore.uri=thrift://example.net:9083'],
      'kafka': ['connector.name=kafka', 'kafka.table-names=table1,table2', 'kafka.nodes=host1:port,host2:port']
  }

To delete a connector, modify the ``connectors.to.delete`` property, whose format is the following: 

``['connector1', 'connector2', etc.]`` 

Again, note the single quotes around each element. The above value will delete connectors ``connector1`` and 
``connector2``. Note that the ``tpch`` connector cannot be deleted because it is used to smoketest Presto 
after it starts. The presence of the ``tpch`` connector has negligible impact on the system.

For example, to delete the Hive and Kafka connectors, set the ``connectors.to.delete`` property to:

 ``['hive', 'kafka']``

-----

Known Issues
============

* For some older versions of Presto, when using the Hive connector to ``CREATE TABLE`` or 
  ``CREATE TABLE AS``, you may run into the following error:

  .. code-block:: none

     Query 20151120_203243_00003_68gdx failed: java.security.AccessControlException: Permission denied: user=hive, access=WRITE, inode="/apps/hive/warehouse/nation":hdfs:hdfs:drwxr-xr-x
  		at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:319)
		at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:219)
		at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission(FSPermissionChecker.java:190)
		at org.apache.hadoop.hdfs.server.namenode.FSDirectory.checkPermission(FSDirectory.java:1771)

  This problem affects Presto ``0.115t`` but does not affect ``0.127t``. 

  To work around the issue, edit your ``jvm.config`` settings by doing the following:

  1. Add the following property:

     ``-DHADOOP_USER_NAME=hive``

  2. Save your edit to ``jvm.config``.

  3. Restart all Presto components for the changes to take effect.

* If you decide to deploy an older version of Presto, you may have to adjust some settings manually. 
  See :ref:`configuring-presto-label` for an explanation of how to add custom settings. For example, 
  the ``task.max-memory`` setting was deprecated in ``0.127t``, but is valid in ``0.115t``. If you're installing 
  ``0.115t`` and would like to change ``task.max-memory`` to something other than its default, add it as a 
  custom property.

* On the Presto service home page, if you click 'Presto workers', you will get an incorrect list of workers. 
  This is a known issue and has been fixed in Ambari 2.2.0.

* If the installation of Presto fails with the error message ``Python script has been killed due to timeout 
  after waiting 1800 secs``, then the ``wget`` for either the Presto RPM or ``presto-cli`` JAR has timed out. 
  To increase the timeout, increase the ``agent.package.install.task.timeout`` setting by doing the following:

  1. On the Ambari server host, in ``/etc/ambari-server/conf/ambari.properties``, edit the setting. 

  2. Restart the Ambari server for the change to take effect. 

  3. To resume the installation, do one of the following:

     a. Click the Retry button in the installation wizard.
     b. Finish the wizard and then install all Presto components individually by navigating to the relevant host 
        and selecting Re-install. 

  The components can be installed manually in any order, but when starting the components, start the Presto 
  coordinator last. 

  If the installation keeps timing out, we suggest downloading the RPM and JAR outside the installation process, 
  uploading them somewhere on your network, and editing ``package/scripts/download.ini`` with the new URLs.

* At the moment, upgrading Presto from Ambari is not possible. Even though Ambari provides the capability to 
  upgrade software, upgrading has not been implemented in the integration. If many users request this feature,
  we can add it to a future release. (If you'd like to see this feature, let us know by commenting on 
  `this issue <https://github.com/prestodb/ambari-presto-service/issues/17>`_).
 
  To upgrade Presto without the native upgrade integratio,n you must manually uninstall Presto, and then install 
  the new version.
