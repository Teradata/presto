-- database: presto; groups: decimal; tables: datatype
--!
select c_decimal from datatype where c_decimal = decimal 12345678901234567890.123
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
12345678901234567890.123|
