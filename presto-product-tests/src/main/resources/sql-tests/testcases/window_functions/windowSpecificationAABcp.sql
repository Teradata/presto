--- Test query with window functions in window specifications as follows:
--- window specification A
--- window specification A
--- window specification B, window function has constant parameters
-- database: presto; groups: window;
select
suppkey, orderkey, partkey,
sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row) sum_quantity_A,
sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row) sum_discount_A,
ntile(4) over (partition by partkey order by orderkey rows between UNBOUNDED preceding and CURRENT ROW) ntile_quantity_B

from tpch.tiny.lineitem where (partkey = 272 or partkey = 273) and suppkey > 50
