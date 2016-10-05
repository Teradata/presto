=================================================================
 Presto Server Installation on an AWS EMR (Presto Admin and RPMs)
=================================================================

You can use Presto Admin and RPMs to install and deploy Presto on 
an Amazon Web Services (AWS) EMR.

System Requirements
*******************

* Hardware requirements:
 
  + Minimum 16GB RAM per node allocated for Presto

* Software requirements:

  + Red Hat Enterprise Linux 6.x (64-bit) or CentOS equivalent
  + Oracle Java JDK 1.8 (64-bit)  
  + Cluster: CDH 5.x or HDP 2.x
  + YARN-based cluster: CDH 5.4 or HDP 2.2; both require Hadoop 2.6
  + Python 2.6 or 2.7

Installation on an AWS EMR
**************************

Install and manage Presto manually on an AWS EMR in the Amazon cloud.

| Download Presto from https://www.teradata.com/presto
|
| Before installing Preston on an AWS EMR cluster, review the caveats summarized in the following document:
| :doc:`Setting up Presto Admin on an Amazon EMR Cluster <installation/presto-admin/emr>`
|
|       
| The following installation procedures contain notes or sections specific to installing Presto on an Amazon Web Services EMR cluster. These are the notes summarized in the above document.
| For a detailed explanation of all of the commands and their options, see
| :doc:`Comprehensive Presto Admin User Guide <installation/presto-admin/user-guide>`
|

.. toctree::
    :maxdepth: 1
	       
    Installing Presto Admin <installation/presto-admin/installation/presto-admin-installation.rst>
    Configuring Presto Admin <installation/presto-admin/installation/presto-admin-configuration.rst>
    Installing Java 8 <installation/presto-admin/installation/java-installation.rst>
    Installing the Presto Server <installation/presto-admin/installation/presto-server-installation.rst>
    Installing the Presto CLI <installation/presto-admin/installation/presto-cli-installation.rst>
    Adding a Connector <installation/presto-admin/installation/presto-connector-installation.rst>
    Configuring Presto <installation/presto-admin/installation/presto-configuration.rst>
    Troubleshooting <installation/presto-admin/installation/troubleshooting-installation.rst>




