*************
Reorder joins
*************

Cross joins elimination
-----------------------


When config property ``reorder-joins`` (or ``reorder_joins`` session property) is enabled optimizer will search for cross joins
in query plan and try to eliminate them by changing order of the joins. While doing so optimizer will try to preserve
original join order as much as possible. For this optimization optimizer does not use any statistics. It just assumes that
every cross join is unintended. If optimizer is not able to eliminate cross joins it will maintain original join order.

For example for the following query:

.. code-block:: sql

    SELECT * FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey;

Original join order ``part, orders, lineitem`` means that presto first have to join ``part`` table with ``orders`` table,
for which there is no join condition. ``reorder-joins`` enabled changes join order to ``part, lineitem, orders``
which eliminates this cross join.

For following query:

.. code-block:: sql

    SELECT * FROM part p, orders o, lineitem l, supplier s, nation n
    WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey AND l.suppkey = s.suppkey AND s.nationkey = n.nationkey;

Join order will change from ``part, orders, lineitem, supplier, nation`` to ``part, lineitem, orders, supplier, nation``.
Note: the same can be achieved by manually changing the order of tables in the query.
