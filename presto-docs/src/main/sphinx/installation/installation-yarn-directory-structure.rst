==============================================================
Presto Installation Directory Structure on YARN-Based Clusters
==============================================================

If you use Slider scripts or use Ambari slider view to set up Presto on
YARN, Presto is going to be installed using the Presto server tarball
(and not the rpm). Installation happens when the YARN application is
launched and you can find the Presto server installation directory under
the ``yarn.nodemanager.local-dirs`` on your YARN nodemanager nodes. If
for example, your ``yarn.nodemanager.local-dirs`` is
``/mnt/hadoop/nm-local-dirs`` and ``app_user`` is ``yarn``, you can find
Presto is installated under
``/mnt/hadoop-hdfs/nm-local-dir/usercache/yarn/appcache/application_<id>/container_<id>/app/install/presto-server-<version>``.
The first part of this path (till the container\_id) is called the
AGENT\_WORK\_ROOT in Slider and so in terms of that, Presto is available
under ``AGENT_WORK_ROOT/app/install/presto-server-<version>``.

Normally for a tarball installed Presto the catalog, plugin and lib
directories will be subdirectories under the main presto-server
installation directory. The same case here, the catalog directory is at
``AGENT_WORK_ROOT/app/install/presto-server-<version>/etc/catalog``,
plugin and lib directories are created under
``AGENT_WORK_ROOT/app/install/presto-server-<version>/plugin`` and
``AGENT_WORK_ROOT/app/install/presto-server-<version>/lib`` directories
respectively. The launcher scripts used to start the Presto Server will
be at ``AGENT_WORK_ROOT/app/install/presto-server-<version>/bin``
directory.

The Presto logs are available at locations based on your configuration
for data directory. If you have it configured at
``/var/lib/presto/data`` in ``appConfig.json`` then you will have Presto
logs at ``/var/lib/presto/data/var/log/``.
 