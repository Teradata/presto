======================================================
Advanced Configuration Options for YARN-Based Clusters
======================================================

The following sections explain a few advanced configuration options.

Configuring Memory, CPU, and YARN CGroups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Memory and CPU related configuration properties must be modified as per
your cluster configuration and requirements.

1. Memory

``yarn.memory`` in ``resources.json`` declares the amount of memory to
ask for in YARN containers. It should be defined for each component,
COORDINATOR and WORKER based on the expected memory consumption,
measured in MB. A YARN cluster is usually configured with a minimum
container allocation, set in ``yarn-site.xml`` by the configuration
parameter ``yarn.scheduler.minimum-allocation-mb``. It will also have a
maximum size set in ``yarn.scheduler.maximum-allocation-mb``. Asking for
more than this will result in the request being rejected.

The heapsize defined as -Xmx of ``site.global.jvm_args`` in
``appConfig.json``, is used by the Presto JVM itself. Slider suggests
that the value of ``yarn.memory`` must be bigger than this heapsize. The
value of ``yarn.memory`` MUST be bigger than the heap size allocated to
any JVM and Slider suggests using atleast 50% more appears to work,
though some experimentation will be needed.

In addition, set other memory specific properties
``presto_query_max_memory`` and ``presto_query_max_memory_per_node`` in
``appConfig.json`` as you would set the properties ``query.max-memory``
and ``query.max-memory-per-node`` in Presto's config.properties.

2. CPU

Slider also supports configuring the YARN virtual cores to use for the
process which can be defined per component. ``yarn.vcores`` declares the
number of "virtual cores" to request. Ask for more vcores if your
process needs more CPU time.

See
http://slider.incubator.apache.org/docs/configuration/resources.html#core
for more details.

3. CGroups in YARN

If you are using CPU scheduling (using the DominantResourceCalculator),
you should also use CGroups to constrain and manage CPU processes.
CGroups compliments CPU scheduling by providing CPU resource isolation.
With CGroups strict enforcement turned on, each CPU process gets only
the resources it asks for. This way, we can guarantee that containers
hosting Presto services is assigned with a percentage of CPU. If you
have another process that needs to run on a node that also requires CPU
resources, you can lower the percentage of CPU allocated to YARN to free
up resources for the other process.

See Hadoop documentation on how to configure CGroups in YARN:
https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/NodeManagerCgroups.html.
Once you have CGroups configured, Presto on YARN containers will be
configured in the CGroups hierarchy like any other YARN application
containers.

Slider can also define YARN queues to submit the application creation
request to, which can set the priority, resource limits and other values
of the application. But this configuration is global to Slider and
defined in ``conf/slider-client.xml``. You can define the queue name and
also the priority within the queue. All containers created in the Slider
cluster will share this same queue.

::

        <property>
          <name>slider.yarn.queue</name>
          <value>default</value>
        </property>

        <property>
          <name>slider.yarn.queue.priority</name>
          <value>1</value>
        </property>

Failure Policy
~~~~~~~~~~~~~~

Follow this section if you want to change the default Slider failure
policy. Yarn containers hosting Presto may fail due to some
misconfiguration in Presto or some other conflicts. The number of times
the component may fail within a failure window is defined in
``resources.json``.

The related properties are:

1. The duration of a failure window, a time period in which failures are
   counted. The related properties are
   ``yarn.container.failure.window.days``,
   ``yarn.container.failure.window.hours``,
   ``yarn.container.failure.window.minutes`` and should be set in the
   global section as it relates just to slider. The default value is
   ``yarn.container.failure.window.hours=6``. The initial window is
   measured from the start of the slider application master —once the
   duration of that window is exceeded, all failure counts are reset,
   and the window begins again.
2. The maximum number of failures of any component in this time period.
   ``yarn.container.failure.threshold`` is the property for this and in
   most cases, should be set proportional to the the number of instances
   of the component. For Presto clusters, where there will be one
   coordinator and some number of workers it is reasonable to have a
   failure threshold for workers more than that of coordinator. This is
   because a higher failure rate of worker nodes is to be expected if
   the cause of the failure is due to the underlying hardware. At the
   same time the threshold should be low enough to detect any Presto
   configuration issues causing the workers to fail rapidly and breach
   the threshold sooner.

These failure thresholds are all heuristics. When initially configuring
an application instance, low thresholds reduce the disruption caused by
components which are frequently failing due to configuration problems.
In a production application, large failure thresholds and/or shorter
windows ensures that the application is resilient to transient failures
of the underlying YARN cluster and hardware.

Based on the placement policy there are two more failure related
properties you can set.

1. The configuration property ``yarn.node.failure.threshold`` defines
   how "unreliable" a node must be before it is skipped for placement
   requests. This is only used for the default
   yarn.component.placement.policy where unreliable nodes are avoided.
2. ``yarn.placement.escalate.seconds`` is the timeout after which slider
   will escalate the request of pending containers to be launched on
   other nodes. For strict placement policy where the requested
   components are deployed on all nodes, this property is irrelevant.
   For other placement policies this property is relevant and the higher
   the cost of migrating a component instance from one host to another,
   the longer value of escalation timeout is recommended. Thus slider
   will wait longer before the component instance is escalated to be
   started on other nodes. During restart, for cases where redeploying
   the component instances on the same node as before is beneficial (due
   to locality of data or similar reasons), a higher escalation timeout
   is recommended.

Take a look here:
http://slider.incubator.apache.org/docs/configuration/resources.html#failurepolicy
for more details on failure policy.

Using YARN label
~~~~~~~~~~~~~~~~

This is an optional feature and is not required to run Presto in YARN.
To guarantee that a certain set of nodes are reserved for deploying
Presto or to configure a particular node for a component type we can
make use of YARN label expressions.

1. First assign the nodes/subset of nodes with appropriate labels. See
   http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.0/bk_yarn_resource_mgt/content/ch_node_labels.html
2. Then set the components in ``resource.json`` with
   ``yarn.label.expression`` to have labels to be used when allocating
   containers for Presto.
3. Create the application using
   ``bin/slider create .. --queue <queuename>``. ``queuename`` will be
   the queue defined in step one for the appropriate label.

If a label expression is specified for the slider-appmaster component
then it also becomes the default label expression for all component.
Sample ``resources.json`` may look like:

.. code-block:: none

        "COORDINATOR": {
          "yarn.role.priority": "1",
          "yarn.component.instances": "1",
          "yarn.component.placement.policy": "1",
          "yarn.label.expression":"coordinator"
        },
        "WORKER": {
          "yarn.role.priority": "2",
          "yarn.component.instances": "2",
          "yarn.component.placement.policy": "1",
          "yarn.label.expression":"worker"
        }

where coordinator and worker are the node labels created and configured
with a scheduler queue in YARN

