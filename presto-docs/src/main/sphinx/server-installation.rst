===========================
 Presto Server Installation
===========================

There are several ways to install and deploy Presto depending on your requirements and preference.

Automated Installation with Ambari 2.1
======================================
Install and manage Presto using `Apache Ambari`_.

:doc:`Installation Instructions <installation/installation-ambari>`

*Requires*:

* Ambari 2.1 (on Java 8)
* Oracle Java JDK 1.8 (64-bit)
* Red Hat Enterprise Linux 6.x (64-bit) or CentOS equivalent
* HDP 2.x
* Python 2.6 or 2.7
* Minimum 16GB RAM per node allocated for Presto

  .. _Apache Ambari: https://ambari.apache.org/


Automated Installation with Ambari 2.1 with YARN integration
============================================================
Install and manage Presto integrated with YARN using the `Apache Slider`_ view for `Apache Ambari`_.

:doc:`Installation Instructions <installation/installation-yarn>`

*Requires*:

* Oracle Java JDK 1.8 (64-bit)
* Red Hat Enterprise Linux 6.x (64-bit) or CentOS equivalent
* HDP 2.2 (Presto-YARN integration requires Hadoop 2.6 which is on HDP 2.2)
* Python 2.6 or 2.7
* Minimum 16GB RAM per node allocated for Presto

  .. _Apache Slider: https://slider.incubator.apache.org/
  .. _Apache Ambari: https://ambari.apache.org/


Manual Installation via Presto-Admin
====================================
Install and manage Presto manually using Presto-Admin.

:doc:`Installation Instructions <installation/installation-presto-admin>`

*Requires*:

* RedHat Enterprise Linux 6.x (64-bit) or CentOS equivalent
* Oracle Java JDK 1.8 (64-bit)
* CDH 5.x or HDP 2.x
* Python 2.6 or 2.7
* Minimum 16GB RAM per node allocated for Presto


Manual Installation with YARN integration
=========================================
Install and manage Presto integrated with YARN manually using `Apache Slider`_.

:doc:`Installation Instructions <installation/installation-yarn>`

*Requires*:

* RedHat Enterprise Linux 6.x (64-bit) or CentOS equivalent
* Oracle Java JDK 1.8 (64-bit)
* CDH 5.4 or HDP 2.2 (Presto-YARN integreation requires Hadoop 2.6 which is on CDH 5.4, HDP 2.2)
* Apache Slider 0.80.0
* Python 2.6 or 2.7
* Minimum 16GB RAM per node allocated for Presto

  .. _Apache Slider: https://slider.incubator.apache.org/
