********************************
Partitioned and Replicated Joins
********************************

Background
----------
Presto can perform two types of distributed joins: partitioned and replicated.

In a partitioned join, both inputs to a join get hash partitioned across the nodes of the cluster. This distribution type is good for larger inputs,
as it requires less memory on each node. However, it can be much slower than replicated joins because it can require more data to be transferred
over the network.

In a replicated join, the input on the right side is distributed to all of the nodes on the cluster containing data from the input on the left.
This can be much faster than partitioned joins if the right input is small enough (especially if it is much smaller than the left input),
but if it is too large, the query can run out of memory.

Optimizer 
---------
The choice between replicated and partitioned joins is controlled by the property ``join-distribution-type``. Its possible values are
``partitioned``, ``replicated``, and ``automatic``. The default value is ``partitioned``. The property can also be set per session using
the session property ``join_distribution_type``

.. note:: The choice between partitioned and replicated joins was previously controlled by the ``distributed-joins-enabled`` property, which defaulted to ``true``,
    meaning partitioned. That property is now deprecated. For compatibility with legacy configurations, we still look at ``distributed-joins-enabled`` if
    ``join-distribution-type`` is set to the default value of ``partitioned``. In that case, if ``distributed-joins-enabled`` is ``false``, we will perform a
    replicated join.

When ``join-distribution-type`` is set to ``partitioned``, partitioned distribution is used. When it is set to ``replicated``, replicated distribution is used.

When the property is set to ``automatic``, the optimizer will choose which type of join to use based on the size of the join inputs. If the size of either input input
is less than 10% of ``query.max-memory-per-node``, Presto will perform a replicate the smaller of the two tables. If neither input is small enough or there is
insufficient information to determine their size, a partitioned join is performed.
