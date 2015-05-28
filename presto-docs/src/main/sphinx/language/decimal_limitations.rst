
=============================
Decimal data type limitations
=============================

 #. Performance of decimal types longer than 17 digits may be not satisfactory.

 #. Automatic coercion is not supported for decimal column in INSERT flow.

 #. Currently only Hive connector supports exposing ``DECIMAL`` columns.
