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

    presto:sf1> EXPLAIN ANALYZE SELECT count(*), clerk FROM orders WHERE orderdate > date '1995-01-01' GROUP BY clerk;

                                              Query Plan
    -----------------------------------------------------------------------------------------------
    Fragment 1 [SINGLE]
        Cost: CPU 7.07ms, Input: 1000 rows (8.79kB), Output: 1000 rows (8.79kB)
        Output layout: [count]
        Output partitioning: SINGLE []
        - Output[Query Plan] => [count:bigint]
                Cost: 0.00%, Output: 1000 rows (8.79kB)
                TaskOutputOperator := Drivers: 1, Input avg.: 1000.00 lines, Input std.dev.: 0.00%
                Query Plan := count
            - RemoteSource[2] => [count:bigint]
                    Cost: 100.00%, Output: 1000 rows (8.79kB)
                    ExchangeOperator := Drivers: 1, Input avg.: 1000.00 lines, Input std.dev.: 0.00%

    Fragment 2 [HASH]
        Cost: CPU 22.55ms, Input: 4000 rows (148.44kB), Output: 1000 rows (8.79kB)
        Output layout: [count]
        Output partitioning: SINGLE []
        - Project[] => [count:bigint]
                Cost: 4.11%, Input: 1000 rows (37.11kB), Output: 1000 rows (8.79kB), Filtered: 0.00%
                TaskOutputOperator := Drivers: 1, Input avg.: 1000.00 lines, Input std.dev.: 0.00%
                FilterAndProjectOperator := Drivers: 1, Input avg.: 1000.00 lines, Input std.dev.: 0.00%
            - Aggregate(FINAL)[clerk] => [clerk:varchar(15), $hashvalue:bigint, count:bigint]
                    Cost: 23.29%, Output: 1000 rows (37.11kB)
                    HashAggregationOperator := Drivers: 1, Input avg.: 4000.00 lines, Input std.dev.: 0.00%
                    HashAggregationOperator := Collisions avg.: 112.00 (345.16% est.), Collisions std.dev.: 0.00%
                    count := "count"("count_8")
                - RemoteSource[3] => [clerk:varchar(15), count_8:bigint, $hashvalue:bigint]
                        Cost: 72.60%, Output: 4000 rows (148.44kB)
                        ExchangeOperator := Drivers: 1, Input avg.: 4000.00 lines, Input std.dev.: 0.00%

    Fragment 3 [tpch:orders:1500000]
        Cost: CPU 20.58s, Input: 818058 rows (22.62MB), Output: 4000 rows (148.44kB)
        Output layout: [clerk, count_8, $hashvalue_9]
        Output partitioning: HASH [clerk]
        - Aggregate(PARTIAL)[clerk] => [clerk:varchar(15), $hashvalue_9:bigint, count_8:bigint]
                Cost: 5.37%, Output: 4000 rows (148.44kB)
                HashAggregationOperator := Drivers: 4, Input avg.: 204514.50 lines, Input std.dev.: 0.05%
                HashAggregationOperator := Collisions avg.: 5701.28 (17569.93% est.), Collisions std.dev.: 1.12%
                PartitionedOutputOperator := Drivers: 4, Input avg.: 1000.00 lines, Input std.dev.: 0.00%
                count_8 := "count"(*)
            - ScanFilterProject[table = tpch:tpch:orders:sf1.0, originalConstraint = ("orderdate" > "$literal$date"(BIGINT '9131')), filterPredicate = ("orderdate" > "$literal$date"(BIGINT '9131'))] => [cler
                    Cost: 94.63%, Input: 1500000 rows (0B), Output: 818058 rows (22.62MB), Filtered: 45.46%
                    ScanFilterAndProjectOperator := Drivers: 4, Input avg.: 375000.00 lines, Input std.dev.: 0.00%
                    $hashvalue_9 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("clerk"), 0))
                    orderdate := tpch:orderdate
                    clerk := tpch:clerk

