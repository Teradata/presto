=====================
Software Requirements
=====================

**Operating System**

* RHEL 6.x (Red Hat Enterprise Linux)
* CentOS equivalent to RHEL (Community ENTerprise Operating System)
* SLES 11 SP3 (SUSE Linux Enterprise Server) **Teradata Support for SLES on Teradata Hadoop Appliances only**
  
**Hadoop distributions**

* CDH (Cloudera Distribution Including Apache Hadoop) 5.x
* HDP (Hortonworks Data Platform) 2.x
* IBM BigInsights for Apache Hadoop 4.1

**Java**

* Oracle Java 1.8 JRE (64-bit)

**Python**

* Python 2.6.x OR
* Python 2.7.x

**SSH Configuration**

* Passwordless SSH between the node running ``presto-admin`` and the nodes where Presto will be installed OR
* Ability to SSH with a password between the node running ``presto-admin`` and the nodes where Presto will be installed

**Other Configuration**

* Sudo privileges on both the node running ``presto-admin`` and the nodes where Presto will be installed
