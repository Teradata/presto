-- database: presto; groups: window;
select orderkey, discount, dense_rank() over (order by discount) from tpch.tiny.lineitem where partkey = 272
