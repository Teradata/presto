-- database: presto; groups: decimal; tables: datatype
--!
select c_decimal from datatype where c_decimal < decimal 90.2
--!
-- delimiter: |; trimValues: true; ignoreOrder: true; types: DECIMAL
90.123456789|
0.123456789|
