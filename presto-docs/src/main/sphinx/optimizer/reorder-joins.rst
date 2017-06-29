===============
Join Reordering
===============

Background
----------

The order in which joins are executed in a query can have a significant impact
on the query's performance. The aspect of join ordering that has the largest
impact on performance is the size of the data being processed and passed over
the network. If a join is not a primary key-foreign key join, the data produced
can be much greater than the size of either table in the join-- up to
\|Table 1\| x \|Table 2\| for a cross join. If a join that produces a lot of
data is performed early in the execution, then subsequent stages will need to
process large amounts of data for longer than necessary, increasing the time and
resources needed for the query.

Join Reordering Options in Presto
---------------------------------
Presto's join reordering strategy can be set by the configuration property
``join-reordering-strategy`` or the session property ``join_reordering_strategy``.
The options are ``NONE``, ``ELIMINATE_CROSS_JOINS``, and ``COST_BASED``. The default
value is ``ELIMINATE_CROSS _JOINS``.

NONE
====

When the join reordering strategy is set to ``NONE``, Presto joins tables in
the order in which they are listed in a query. It is the responsibility of the
user to optimize the join order when writing queries in order to achieve better
performance and handle larger joins. It is often a good idea to join small tables
early in the plan, and leave larger fact tables until the end. One should also be
careful of introducing cross joins or join conditions that produce output that is
larger than the size of the input. Though such joins may sometimes be necessary or
optimal, when introduced unintentionally, they can dramatically reduce the performance
of the query.

ELIMINATE_CROSS_JOINS
=====================

When the join reordering strategy is set to ``ELIMINATE_CROSS_JOINS`` (the default),
the optimizer will search for cross joins in the query plan and try to eliminate them
by changing the join order. When reordering, Presto will try to preserve the original
join order as much as possible. If cross joins cannot be eliminated, the original join
order will be maintained. Note that this join reordering strategy does not use any statistics.
Therefore, Presto will try to eliminate any cross join it can, even if including the cross
joins would have resulted in a more optimal query plan. For example, it may be optimal to
perform a cross join of two small dimension tables before joining in the larger fact table.
However, Presto will nevertheless reorder the joins to remove the cross join.

Examples
^^^^^^^^
For the following query:

.. code-block:: sql

 SELECT * FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey;

In the original join order ``part, orders, lineitem``, Presto will first join
the ``part`` table with the ``orders`` table, for which there is no join
condition. When ``reorder-joins=true``, the join order will be changed to
``part, lineitem, orders`` to eliminate the cross join.

For the following query:

.. code-block:: sql

 SELECT * FROM part p, orders o, lineitem l, supplier s, nation n
 WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND l.suppkey = s.suppkey AND s.nationkey = n.nationkey;

The join order will change from ``part, orders, lineitem, supplier, nation`` to
``part, lineitem, orders, supplier, nation``.


COST_BASED
==========

When the join reordering strategy is set to ``COST_BASED``, Presto will use statistics
provided by the connectors to estimate the costs for different join orders (currently stats
are only available in the hive connector). It will then choose the join order with the lowest
computed cost. If statistics are not available or if for any other reason a cost could not be
computed, the ``ELIMINATE_CROSS_JOINS`` strategy is used instead. Enumerating all possible join
orders for many tables is computationally intensive, so joins are reordered in groups of ten at a time.
If there are more than ten joins the first ten will be reordered, and then the next ten will be reordered
indepently of the first.

.. NOTE::

    For best optimizer choices, enable automatic join type selection using the configuration property
    ``join-distribution-type=AUTOMATIC``
