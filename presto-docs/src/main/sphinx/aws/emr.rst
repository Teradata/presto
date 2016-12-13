=================
Presto on AWS EMR
=================

We support deploying Presto on EMR version 4.3.0 or greater. For instructions
on how to run and install presto-admin on EMR refer to the EMR specific notes
in the installation and configuration sections of the presto-admin.

Resizing an EMR cluster using presto-admin
==========================================

Only the number of EMR core (slave) nodes can be resized. Migration of the
Presto coordinator to a different EMR master node is not supported. The
steps below assume a working installation of presto-admin.

Adding a node
-------------

1. Using the AWS console or the AWS CLI tool, resize your EMR cluster by
adding a core node. The AWS documentation on this can be found
`here <http://docs.aws.amazon.com/ElasticMapReduce/latest/ManagementGuide/emr-manage-resize.html>`_.

2. Add the FQDN of the new node to the presto-admin topology file.
By default this file is located at ``~/.prestoadmin/config.json``.

3. To install Presto on the new node, run the following two presto-admin
commands. The first one is only needed if Java 8 is not already installed on your
new node:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem package install -H new_node_fqdn /path/to/jdk8.rpm
    /path/to/presto-admin -i /path/to/your.pem server install -H new_node_fqdn /path/to/presto-server.rpm

4. In order to take advantage of the extra resources of the new node, you
may need to edit some of the Presto memory settings (for example
``query.max.memory``). For more information on configuring Presto using
presto-admin please refer to the Configuration section of the presto-admin.

5. If you made configuration changes, make sure to deploy those changes to
all nodes:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem configuration deploy

6. If you did not make any configuration changes in step 4 then just start
Presto on the new node:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem server start -H new_node_fqdn

If you did make configuration changes in step 4 then all nodes need to be restarted
(note that you don't need to also start the new node because it is now part
of the topology and it will be started as part of the restart):

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem server restart

7. To verify that the new node was correctly started run:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem server status

Removing a node
---------------

1. Stop Presto on the worker node you would like to remove:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem server stop -H node_to_remove_fqdn

2. Uninstall Presto from the worker node:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem server uninstall -H node_to_remove_fqdn

3. Remove the FQDN of the node you would like to remove from the presto-admin
topology file. By default this file is located at ``~/.prestoadmin/
config.json``.

4. To adjust your Presto installation to the decrease in resources you
may need to edit some of the Presto memory settings (for example
``query.max.memory``). For more information on configuring Presto using
presto-admin please refer to the Configuration section of the presto-admin.

5. If you made configuration changes, make sure to deploy those changes to
all nodes and then restarting them by running:

.. code-block:: bash

    /path/to/presto-admin -i /path/to/your.pem configuration deploy
    /path/to/presto-admin -i /path/to/your.pem server restart

6. Terminate the node using the AWS console or the AWS CLI tool.

Troubleshooting
===============

* EMR deploys hive 1.0.0 version which has a bug with ALTER TABLE..RENAME TO command. When running this command in Presto using the Hive connector, you may run into the following:

.. code-block:: none

    presto:default> ALTER TABLE test_table RENAME TO new_test_table;
    
    Query 20160322_155117_00002_ai777 failed: InvalidOperationException(message:Unable to access old location hdfs://node:8020/user/hive/warehouse/test_table for table default.test_table)
    
You can workaround this issue by changing the ownership of the ``/user/hive/warehouse`` directory to ``hive:hadoop`` as shown below:

.. code-block:: none

    hadoop dfs -chown hive:hadoop -R /user/hive/warehouse/test_table
