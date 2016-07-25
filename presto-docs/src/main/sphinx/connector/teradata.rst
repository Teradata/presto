==================
Teradata Connector
==================

Teradata provides a low latency high performing connector that
supports high concurrency and parallel processing between Teradata
and Presto. The Teradata connector (also known as Presto-To-Teradata QueryGrid)
can initiate a query from Presto to reach out to Teradata. The connector is
architected to be as efficient as possible, leveraging SQL pushdown, column pruning,
auto data conversion, and compression as well as optimized CPU usage.

To query tables that exist in Teradata from Presto all that is needed
is to configure the Teradata connector by adding a `teradata.properties`
file, then query from the **teradata** catalog. For example, if you have a sales table in Teradata
and want to get the average price for the state
of Massachusetts, execute the following query in Presto —

.. code-block:: none
   
   SELECT AVG(price)
   FROM teradata.sales.transactions
   WHERE state=’MA’;

It is also possible to join data from Teradata with different data sources
available in Presto.

The Presto-to-Teradata QueryGrid connector has simple, easy-to-use syntax,
enabling a business user to quickly and interactively query between
systems.

Teradata also provides a Teradata-to-Presto QueryGrid connector, to allow
querying data in Presto from Teradata.

Click `here <http://www.teradata.com/Resources/Datasheets/QueryGrid-and-Presto-Enabling-faster-more-scalable-interactive-querying-of-Hadoop/>`_ for more information about the Presto Teradata QueryGrid Connectors

Contact **presto@teradata.com** for more information to obtain an evaluation of the Presto Teradata QueryGrid Connectors.

