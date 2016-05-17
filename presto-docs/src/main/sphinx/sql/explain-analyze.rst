===============
EXPLAIN ANALYZE
===============

Synopsis
--------

.. code-block:: none

    EXPLAIN ANALYZE statement

Description
-----------

Execute the statement and show the distributed execution plan of the statement
along with the cost of each operation.

.. note::

    The stats may not be entirely accurate, especially for queries that complete quickly.

Examples
--------

In the example below, you can see the CPU time spent in each stage, as well as the relative
cost of each operator in the stage. Note that the relative cost of the operators is based on
wall time, which may or may not be correlated to CPU time.

.. code-block:: none

    presto:sf1> EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk;

                                              Query Plan
    -----------------------------------------------------------------------------------------------
    Fragment 1 [SINGLE]
        Cost: CPU 1.56ms, Input: 1000 rows (8.79kB), Output: 1000 rows (8.79kB)
        Output layout: [count]
        Output partitioning: SINGLE []
        - Output[Query Plan] => [count:bigint]
                Cost: 0.00%, Output: 1000 rows (8.79kB)
                Query Plan := count
            - RemoteSource[2] => [count:bigint]
                    Cost: 100.00%, Output: 1000 rows (8.79kB)

    Fragment 2 [HASH]
        Cost: CPU 12.87ms, Input: 4000 rows (148.44kB), Output: 1000 rows (8.79kB)
        Output layout: [count]
        Output partitioning: SINGLE []
        - Project[] => [count:bigint]
                Cost: 5.26%, Input: 1000 rows (37.11kB), Output: 1000 rows (8.79kB), Filtered: 0.00%
            - Aggregate(FINAL)[clerk] => [clerk:varchar(15), $hashvalue:bigint, count:bigint]
                    Cost: 47.37%, Output: 1000 rows (37.11kB)
                    count := "count"("count_8")
                - RemoteSource[3] => [clerk:varchar(15), count_8:bigint, $hashvalue:bigint]
                        Cost: 47.37%, Output: 4000 rows (148.44kB)

    Fragment 3 [tpch:orders:1500000]
        Cost: CPU 13.29s, Input: 1500000 rows (41.48MB), Output: 4000 rows (148.44kB)
        Output layout: [clerk, count_8, $hashvalue_9]
        Output partitioning: HASH [clerk]
        - Aggregate(PARTIAL)[clerk] => [clerk:varchar(15), $hashvalue_9:bigint, count_8:bigint]
                Cost: 8.08%, Output: 4000 rows (148.44kB)
                count_8 := "count"(*)
            - ScanProject[table = tpch:tpch:orders:sf1.0, originalConstraint = true] => [clerk:varchar(15), $hashvalue_9:bigint]
                    Cost: 91.92%, Input: 1500000 rows (0B), Output: 1500000 rows (41.48MB), Filtered: 0.00%
                    $hashvalue_9 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("clerk"), 0))
                    clerk := tpch:clerk

