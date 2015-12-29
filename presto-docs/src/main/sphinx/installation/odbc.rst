============
ODBC Drivers
============

Presto can be accessed from using one of the Teradata ODBC drivers. The ODBC
driver is available for Windows, Mac, and Linux. The drivers are available for
free download from Teradata.

ODBC for Mac
************

*Requires*

* OS X 10.9 or 10.10
* iODBC 3.52.7 or later

| Download :download:`odbc-documentation`
| Download :download:`odbc-mac`

----

ODBC for Windows
****************

*Requires*

* Microsoft ODBC Driver Manager

| Download :download:`odbc-documentation`
| Download (64 bit) :download:`odbc-windows-64`
| Download (32 bit) :download:`odbc-windows-32`

----

ODBC for Linux
**************
*Requires*

* RedHat Enterprise Linux 6.x or or CentOS equivalent
* iODBC 3.52.7 or later, unixODBC 2.3.0 or later

| Download :download:`odbc-documentation`
| Download (64) :download:`odbc-linux-64`
| Download (32) :download:`odbc-linux-32`

Tableau Customizations
**********************
**Windows Only**

The Teradata Presto ODBC driver is distributed with a Tableau Datasource Connection (TDC) file. The file is used to better the use of Presto and Tableau by customizing the SQL that Tableau sends to Presto via the driver. The TDC will not correct functionality, it will only inform Tableau of the best way to work with the Teradata Presto ODBC driver.

| After installation of the Teradata Presto ODBC driver, the TDC file is located in:
| ``C:\Program Files\Teradata Presto ODBC Driver\lib`` (64 bit)
| or
| ``C:\Program Files (x86)\Teradata Presto ODBC Driver\lib`` (32 bit)
 
| You should copy the TDC file to the location Tableau will look for it:
| ``C:\Users\<USER_NAME>\Documents\My Tableau Repository\Datasources``
