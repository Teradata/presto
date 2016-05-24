-- database: presto; groups: window;
select orderkey, discount, rank() over (order by discount) from tpch.tiny.lineitem where partkey = 272
