-- database: presto; tables: part; groups: group-by;
SELECT p_type, COUNT(*) FROM part GROUP BY p_type HAVING COUNT(*) > 1400 and AVG(p_retailprice) > 1000
