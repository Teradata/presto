
=============================
Decimal data type limitations
=============================

 #. Currently only basic arithmetic operators and inequality operators are supported.

 #. Automatic coercions are only supported between ``DECIMAL`` types (where possible).
    Other supported casts must be specified explicitly by the user is SQL.

 #. Performance of decimal types longer than 17 digits may be not satisfactory.

 #. ``INSERT`` flow is not supported for ``DECIMAL`` columns.

 #. Currently only Hive connector supports exposing ``DECIMAL`` columns.
