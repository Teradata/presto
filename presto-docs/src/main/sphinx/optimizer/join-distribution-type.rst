==================================
Repartitioned and Replicated Joins
==================================

Background
----------

Presto can perform two types of distributed joins: repartitioned and replicated.
In a repartitioned join, both inputs to a join get hash partitioned across the
nodes of the cluster. In a replicated join, one of the inputs is distributed to
all of the nodes on the cluster that have data from the other input.

Repartitioned joins are good for larger inputs, as they need less memory on each
node and allow Presto to handle larger joins overall. However, they can be much
slower than replicated joins because they can require more data to be transferred
over the network for tables that differ greatly in size.

Replicated joins can be much faster than repartitioned joins if the replicated
table is small enough to fit in memory and much smaller than the other input.
But, if the replicated input is too large, the query can run out of memory.

Choosing the Distribution Type in Presto
----------------------------------------

The choice between replicated and repartitioned joins is controlled by the
property ``join-distribution-type``. Its possible values are ``repartitioned``,
``replicated``, and ``automatic``. The default value is ``repartitioned``.
The property can also be set per session using the session property
``join_distribution_type``.

.. code-block:: sql

    SET SESSION join_distribution_type = 'automatic';

When ``join-distribution-type`` is set to ``repartitioned``, repartitioned
distribution is used. When it is set to ``replicated``, replicated distribution
is used. In ``replicated`` mode, the right input to the join is the one that
gets replicated.

When the property is set to ``automatic``, the optimizer will choose which type
of join to use based on the size of the join inputs. The smaller table from a
join will be replicated if the following two conditions hold:

    1. It is smaller than ``coefficient`` * ``query.max-memory-per-node``
       The coefficient defaults to  0.01. It can be set using the property
       ``small-table-coefficient`` or per session using the session property
       ``small_table_coefficient``.

       .. code-block:: sql

          SET SESSION small_table_coefficient = 0.05;

       The coefficient is there to ensure that queries do not exceed
       ``query.max-memory-per-node``, even when they contain multiple joins.
       The property should be set to prevent out of memory errors for the most
       complex queries while within that framework allowing replicated joins
       when optimal and possible.
    2. It is n times smaller than the other table in a join, where n is the
       number of nodes in the cluster.

If neither input meets these conditions, a repartitioned join is performed.
