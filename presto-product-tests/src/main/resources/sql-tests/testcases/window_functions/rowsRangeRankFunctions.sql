-- database: presto; groups: window;
select orderkey, suppkey,
discount,
dense_rank() over (partition by suppkey order by discount desc) discount_drank,
extendedprice,
rank() over (partition by discount order by extendedprice range current row) extendedprice_rank

from tpch.tiny.lineitem where partkey = 272

