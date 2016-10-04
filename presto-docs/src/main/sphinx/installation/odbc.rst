============
ODBC Drivers
============

Presto can be accessed using one of the Teradata ODBC
drivers. The ODBC drivers are available for Windows, Mac, and Linux. The
drivers are available for free download from
https://www.teradata.com/presto.

For detailed Presto ODBC documentation, please see the `Teradata
Presto ODBC Driver Install Guide
<http://teradata-presto.s3.amazonaws.com/odbc-1.1.3.1007/TeradataODBCDriverPrestoInstallGuide_1_1_3.pdf>`_


Version Compatibility
---------------------

============================ ==========================
Teradata Presto ODBC Version Compatible Presto Versions
============================ ==========================
1.1.4.1.1001                 0.152.1-t, 0.148-t.1.2

1.1.3.1007                   0.148-t.1.2

1.1.0.1004                   0.141-t

1.0.0.1001                   0.127-t, 0.115-t, 0.101-t
============================ ==========================

System Requirements
-------------------

============================= ==============================================
Operating System              Driver Manager
============================= ==============================================
OS X 10.9, OS X 10.10         iODBC 3.52.7 or later

Windows 32 and 64             Microsoft ODBC Driver Manager

RHEL 6.x, CentOS equivalent   iODBC 3.52.7 or later, unixODBC 2.3.0 or later
============================= ==============================================

Tableau Customizations
----------------------

**Windows Only**
The Teradata client distribution package comes with a Tableau Datasource Connection (TDC) file. The file is used to better the use of Presto and Tableau by customizing the SQL that Tableau sends to Presto via the driver. The TDC will not correct functionality, it will only inform Tableau of the best way to work with the Teradata Presto ODBC driver. The TDC file is included in the Presto Client Package download.

After installing the ODBC driver on Windows, you should copy the TDC file to the location Tableau will look for it:
``C:\Users\<USER_NAME>\Documents\My Tableau Repository\Datasources``

**Tableau 10**
Tableau 10 provides a named connector for Presto. For users on Tableau 10, the TDC file is not needed.
