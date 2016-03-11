===============
EXPLAIN ANALYZE
===============

Synopsis
--------

.. code-block:: none

    EXPLAIN ANALYZE statement

Description
-----------

Execute the statement, and show the distributed execution plan of the statement
along with the cost of each operation.

.. note::

    The stats may not be 100% accurate, so cost for some part of the query may be unknown

Examples
--------

.. code-block:: none

    presto:sf1> EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk;

                                              Query Plan
    -----------------------------------------------------------------------------------------------
    Fragment 1 [HASH]
        Cost: CPU 3.25ms, Input 4000 (148.45kB), Output 1000 (37.11kB)
        Output layout: [clerk, $hashvalue, count]
        Output partitioning: SINGLE []
        - Aggregate(FINAL)[clerk] => [clerk:varchar, $hashvalue:bigint, count:bigint]
                Cost: 75,00%, Output 1000 (37.11kB)
                count := "count"("count_8")
            - RemoteSource[2] => [clerk:varchar, $hashvalue:bigint, count_8:bigint]
                    Cost: 25,00%, Output 4000 (148.45kB)

    Fragment 2 [SOURCE]
        Cost: CPU 919.46ms, Input 150000 (4.15MB), Output 4000 (148.45kB)
        Output layout: [clerk, $hashvalue, count_8]
        Output partitioning: HASH [clerk]
        - Aggregate(PARTIAL)[clerk] => [clerk:varchar, $hashvalue:bigint, count_8:bigint]
                Cost: 6,33%, Output 4000 (148.45kB)
                count_8 := "count"(*)
            - ScanFilterAndProject[table = tpch:tpch:orders:sf0.1, originalConstraint = true] => [clerk:varchar, $hashvalue:bigint]
                    Cost: 93,67%, Input 150000 (0B), Output 150000 (4.15MB), Filtered: 0,00%
                    $hashvalue := "combine_hash"(0, COALESCE("$operator$hash_code"("clerk"), 0))
                    clerk := tpch:clerk

