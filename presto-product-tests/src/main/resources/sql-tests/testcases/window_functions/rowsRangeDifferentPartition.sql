-- database: presto; groups: window;
select orderkey, suppkey,
discount,
round(avg(discount) over (partition by suppkey order by orderkey rows between current row and unbounded following), 5)  avg_discount,
extendedprice,
round(avg(extendedprice) over (partition by discount range current row), 5) avg_extendedprice

from tpch.tiny.lineitem where partkey = 272

