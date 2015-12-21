==================
Teradata Additions
==================

The following are additions that Teradata has added to Facebook's 0.127 release of presto

General Fixes
-------------

* Remove buggy optimization to prune redundant projections because it produced wrong results.

Datatypes
---------

* Experimental support for the decimal datatype

RPM Fixes
---------
* The presto rpm now works with all versions of rpm after and including 4.6.  And has a separate rpm
  for rpm versions before 4.6
* Fix rpm upgrade
