====================================================
Presto Configuration Options for YARN-Based Clusters
====================================================

If you are using Ambari to install the Presto App package you can update
the configuration properties from the Ambari Slider View UI. If you are 
using Slider to install Presto on YARN manually, you must edit the 
configuration files manually.

The ``appConfig.json`` and ``resources-[singlenode|mutlinode].json`` files
are the two major configuration files you need to configure before you
can get Presto running on YARN. Sample configuration options files for the 
Presto App package are available in the repository in the following directory:

``presto-yarn-package/src/main/resources``

The "default" values listed for the sections
`appConfig.json <#appconfig-json>`__ and `resources.json <#resources-json>`__ are
from ``presto-yarn-package/src/main/resources/appConfig.json`` and
``presto-yarn-package/src/main/resources/resources-multinode.json``
files respectively. These default values will be auto-populated on the Slider
View UI for installation using `Ambari <#installation-using-ambari-slider-view>`__ Slider View.
But you can modify the properties on the UI as per your requirements.

For manual installation using Slider, copy the 
``presto-yarn-package/src/main/resources/appConfig.json`` and
``presto-yarn-package/src/main/resources/resources-[singlenode|multinode].json``
to a local file at a location where you are planning to run Slider. Name
them as ``appConfig.json`` and ``resources.json``. Update these sample
json files with whatever configurations you want to have for Presto. If
you are ok with the default values in the sample file you can just use
them as-is.

Follow the steps here and configure the presto-yarn configuration files
to match your cluster requirements. Optional ones are marked (optional).
Please do not change any variables other than the ones listed below.

appConfig.json
~~~~~~~~~~~~~~

1. ``site.global.app_user`` (default - ``yarn``): This is the user which
   will be launching the YARN application for Presto. So all the Slider
   commands (using ``bin/slider`` script) will be run as this user. Make
   sure that you have a HDFS home directory created for the
   ``app_user``. Eg: for user ``yarn`` create ``/user/yarn`` with
   ``yarn`` user as an owner.

::

    hdfs dfs -mkdir -p /user/yarn
    hdfs dfs -chown yarn:yarn /user/yarn

``Note``: For operations involving Hive connector in Presto, especially
INSERT, ALTER TABLE etc, it may require that the user running Presto has
access to HDFS directories like Hive warehouse directories. So make sure
that the ``app_user`` you set has appropriate access permissions to
those HDFS directories. For eg: ``/apps/hive/warehouse`` is usually
where Presto user will need access for various DML operations involving
Hive connector and is owned by ``hdfs`` in most cases. In that case, one
way to fix the permission issue is to set ``site.global.app_user`` to
user ``hdfs`` and also create ``/user/hdfs`` directory in HDFS if not
already there (as above). You will also need to run any slider
scripts(bin/slider) as user ``hdfs`` in this case.

2. ``site.global.user_group`` (default - ``hadoop``): The group owning
   the application.

3. ``site.global.data_dir`` (default - ``/var/lib/presto/data``): This will
   be the data directory used by Presto. This directory configured should 
   be pre-created on all nodes and must be owned by user ``yarn``, 
   otherwise slider will fail to start Presto with permission errors.

::

    mkdir -p /var/lib/presto/data
    chown -R yarn:hadoop /var/lib/presto/data


4. ``site.global.config_dir`` (default - ``/var/lib/presto/etc``): The
   configuration directory on the cluster where the Presto config files
   node.properties, jvm.config, config.properties and connector
   configuration files are deployed. These files will have configuration
   values created from templates
   ``presto-yarn-package/package/templates/*.j2`` and other relevant
   ``appConfig.json`` parameters.

5. ``site.global.singlenode`` (default - ``true``): If set to true, the
   node used act as both coordinator and worker (singlenode mode). For
   multi-node set up, this should be set to false.

6. ``site.global.presto_query_max_memory`` (default - ``50GB``): This
   will be used as ``query.max-memory`` in Presto's config.properties
   file.

7. ``site.global.presto_query_max_memory_per_node`` (default - ``1GB``):
   This will be used as ``query.max-memory-per-node`` in Presto's
   config.properties file.

8. ``site.global.presto_server_port`` (default - ``8080``): Presto
   server's http port.

9. ``site.global.catalog`` (optional) (default - configures ``tpch``
   connector): This property is used to configure connectors for Presto.
   The value of this should match the properties you would normally add
   in a ``connector.properties`` file for Presto in a non-YARN based installation.
   
   It should be of the format (note the single quotes around
   each value) - {'connector1' : ['key1=value1', 'key2=value2'..],
   'connector2' : ['key1=value1', 'key2=value2'..]..}. This will create
   files connector1.properties, connector2.properties for Presto with
   entries key1=value1 etc. 
   For example to have hive.properties for CDH Hive:

::

        "site.global.catalog": "{'hive': ['connector.name=hive-cdh5', 'hive.metastore.uri=thrift://${NN_HOST}:9083'], 'tpch': ['connector.name=tpch']}"

``Note``: The ``NN_HOST`` used in ``hive.metastore.uri`` is a variable
for your HDFS Namenode and this expects that your hive metastore is up
and running on your HDFS Namenode host. You do not have to replace that
with your actual Namenode hostname. This variable will be substituted
with your Namenode hostname during runtime. If you have hive metastore
running elsewhere make sure you update ``NN_HOST`` with the appropriate
hostname.

10. ``site.global.jvm_args`` (default - as in example below): This
    configures Presto ``jvm.config`` file and default heapsize is
    ``1GB``. Since Presto needs the ``jvm.config`` format to be a list of
    options, one per line, this property must be a String representation
    of list of strings. Each entry of this list will be a new line in
    your jvm.config. For example the configuration should look like:

::

        "site.global.jvm_args": "['-server', '-Xmx1024M', '-XX:+UseG1GC', '-XX:G1HeapRegionSize=32M', '-XX:+UseGCOverheadLimit', '-XX:+ExplicitGCInvokesConcurrent', '-XX:+HeapDumpOnOutOfMemoryError', '-XX:OnOutOfMemoryError=kill -9 %p']",

11. ``site.global.additional_node_properties`` and
    ``site.global.additional_config_properties`` (optional) (default -
    None): Presto launched via Slider will use ``config.properties`` and
    ``node.properties`` created from templates
    ``presto-yarn-package/package/templates/config.properties*.j2`` and
    ``presto-yarn-package/package/target/node.properties.j2``
    respectively. If you want to add any additional properties to these
    configuration files, add ``site.global.additional_config_properties``
    and ``site.global.additional_node_properties`` to your
    ``appConfig.json``. The value of these has to be a string
    representation of an array of entries (key=value) that has to go to
    the ``.properties`` file. Eg:

::

        "site.global.additional_config_properties": "['task.max-worker-threads=50', 'distributed-joins-enabled=true']"

12. ``site.global.plugin`` (optional) (default - None): This allows you
    to add any additional jars you want to copy to plugin
    ``presto-server-<version>/plugin/<connector>`` directory in addition
    to what is already available there. It should be of the format
    {'connector1' : ['jar1', 'jar2'..], 'connector2' : ['jar3',
    'jar4'..]..}. This will copy jar1, jar2 to Presto plugin directory at
    plugin/connector1 directory and jar3, jar4 at plugin/connector2
    directory. Make sure you have the plugin jars you want to add to
    Presto available at
    ``presto-yarn-package/src/main/slider/package/plugins/`` prior to
    building the presto-yarn app package and thus the app package built
    ``presto-yarn-package-<version>-<presto-version>.zip`` will have the
    jars under ``package/plugins`` directory.

::

        "site.global.plugin": "{'ml': ['presto-ml-${presto.version}.jar']}",

13. ``java_home`` (default - ``/usr/lib/jvm/java``): Presto requires Java
    1.8. So make jdk8 the default java or add it to ``java_home`` here

14. Variables in ``appConfig.json`` like ``${COORDINATOR_HOST}``,
    ``${AGENT_WORK_ROOT}`` etc. do not need any substitution and will be
    appropriately configured during runtime.

resources.json
~~~~~~~~~~~~~~

The configuration here can be added either globally (for COORDINATOR and
WORKER) or for each component. Refer `configuration <#advanced-configuration>`__
section for further details.

1. ``yarn.vcores`` (default - ``1``): By default this is set globally.

2. ``yarn.component.instances`` (default - ``1`` for COORDINATOR and
   ``3`` for WORKER): The multinode
   ``presto-yarn-package/src/main/resources/resources-multinode.json``
   sample file is now configured for a 4 node cluster where there will
   be 1 coordinator and 3 workers with strict placement policy, meaning,
   there will be one component instance running on every node
   irrespective of failure history. If there are insufficient number of
   nodemanager nodes in your cluster to accomodate the number of workers
   requested, the application launch will fail. The number of workers
   could be ``number of nodemanagers in your cluster - 1``, with 1 node
   reserved for the coordinator, if you want Presto to be on all YARN
   nodes.
   If you want to deploy Presto on a single node
   (``site.global.singlenode`` set to true), make sure you set 1 for the
   COORDINATOR and just not add the WORKER component section (Refer
   ``presto-yarn-package/src/main/resources/resources-singlenode.json``).
   You can also just set ``yarn.component.instances`` to 0 for WORKER in
   this case.

3. ``yarn.memory`` (default - ``1500MB``): The heapsize defined as -Xmx
   of ``site.global.jvm_args`` in ``appConfig.json``, is used by the
   Presto JVM itself. Slider suggests that the value of ``yarn.memory``
   must be bigger than this heapsize. The value of ``yarn.memory`` MUST
   be bigger than the heap size allocated to any JVM and Slider suggests
   using atleast 50% more appears to work, though some experimentation
   will be needed.

4. ``yarn.label.expression`` (optional) (default - ``coordinator`` for
   COORDINATOR and ``worker`` for WORKER\`\`): See `label <#using-yarn-label>`__
   section for details.

Now you are ready to deploy Presto on YARN either manually or by using 
Ambari.
