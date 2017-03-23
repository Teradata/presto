#!/usr/bin/env bash
MAPR_HOME="/opt/mapr"
CLDB_PORT=7222
${MAPR_HOME}/server/configure.sh -N hadoop-master -c -C hadoop-master:${CLDB_PORT} -HS hadoop-master
/docker/volumes/conf/docker/files/presto-launcher-wrapper.sh singlenode run
