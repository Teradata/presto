=======
EXPLAIN
=======

Synopsis
--------

.. code-block:: none

    EXPLAIN [ ( option [, ...] ) ] statement

    where option can be one of:

        FORMAT { TEXT | GRAPHVIZ }
        TYPE { LOGICAL | DISTRIBUTED }

Description
-----------

Show the logical or distributed execution plan of a statement. Use ``TYPE DISTRIBUTED`` option
to display fragmented plan. Each plan fragment is executed by a single or multiple Presto nodes.
Fragments separation represent the data exchange between Presto nodes. Fragment type specifies
how the fragment is executed by Presto nodes and how the data is distributed between fragments:

  * ``SINGLE``: Fragment is executed by single node.

  * ``HASH``: Fragment is executed by a fixed number of Presto nodes. Fragment input data is
              distributed among nodes with a hash function.

  * ``ROUND_ROBIN``: Fragment is executed by a fixed number of Presto nodes. Fragment input data
                     is distributed among nodes with a round robin function.

  * ``BROADCAST``: Fragment is executed by a fixed number of Presto nodes. Fragment input data
                   is broadcasted to all nodes.

  * ``SOURCE``: Fragment is executed on nodes where input splits needs to be accessed from.

Examples
--------

Logical plan:

.. code-block:: none

    presto:tiny> EXPLAIN SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                                    Query Plan
    ----------------------------------------------------------------------------------------------------------
     - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
             _col1 := count
         - RemoteExchange[GATHER] => regionkey:bigint, count:bigint
             - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                     count := "count"("count_8")
                 - RemoteExchange[REPARTITION] => regionkey:bigint, count_8:bigint, $hashvalue:bigint
                     - Project[] => [regionkey:bigint, count_8:bigint, $hashvalue_9:bigint]
                             $hashvalue_9 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("regionkey"), 0))
                     - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                             count_8 := "count"(*)
                         - TableScan[tpch:tpch:nation:sf0.01, originalConstraint = true] => [regionkey:bigint]
                                 regionkey := tpch:regionkey

Distributed plan:

.. code-block:: none

    presto:tiny> EXPLAIN (TYPE DISTRIBUTED) SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                              Query Plan
    ----------------------------------------------------------------------------------------------
     Fragment 0 [SINGLE]
         Output layout: [regionkey, count]
         Output partitioning: SINGLE []
         - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
                 _col1 := count
             - RemoteSource[1] => [regionkey:bigint, count:bigint]

     Fragment 1 [HASH]
         Output layout: [regionkey, count]
         Output partitioning: SINGLE []
         - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                 count := "count"("count_8")
             - RemoteSource[2] => [regionkey:bigint, count_8:bigint, $hashvalue:bigint]

     Fragment 2 [SOURCE]
         Output layout: [regionkey, count_8, $hashvalue_9]
         Output partitioning: HASH [regionkey]
         - Project[] => [regionkey:bigint, count_8:bigint, $hashvalue_9:bigint]
                 $hashvalue_9 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("regionkey"), 0))
             - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                     count_8 := "count"(*)
                 - TableScan[tpch:tpch:nation:sf0.01, originalConstraint = true] => [regionkey:bigint]
                         regionkey := tpch:regionkey
