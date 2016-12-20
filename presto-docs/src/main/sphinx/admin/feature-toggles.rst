===============
Feature Toggles
===============

The default Presto settings should work well for most workloads. The following
information may help you if some of the features are not working correctly for you.
Though, if you think the behavior of a feature is unexpected, don't hesitate to file a bug report regardless if it's enabled or disabled by default.

Config Properties
-----------------

+---------------------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Name of property           | Default value | Description                                                                                                                                                |
+===========================+===============+=======================================================================================================+++==================================================+
|optimizer.reorder-windows  | false         |Allow reordering windows in order to put those with the same partitioning next to each other. This may sometimes allow minimizing number of repartitionings.|
+---------------------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|beta.spill-enabled         | false         |Try spilling memory to disk to avoid exceeding memory limits for the query. See :ref:`this section<spilling-toggle>` for a more detailed description.       |
+---------------------------+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+

.. _spilling-toggle:

Spilling to disks
-----------------

Spilling works by offloading memory to disk. This process can allow some queries with large memory
footprint to pass at the cost of slower execution times. Currently, spilling is supported only for
aggregations, so this property will not reduce memory usage required for joins, window functions and
sorting. The spiller can be configured to write simultaneously to multiple directories, which helps
to utilize multiple drives installed in the system.

Be aware that this is an experimental feature and should be used with care.

Currently, all queries with aggregations will slow down after enabling spilling. It is recommended
to use the spill session property to selectively turn on spilling only for queries that would run
out of memory otherwise.

For configuration and session properties, check :ref:`spilling properties <tuning-spilling>`.
