#!/bin/bash

HIVE_JDBC_DOWNLOAD_URL='http://bdch-ftp.td.teradata.com:8081/nexus/service/local/artifact/maven/redirect?r=test-framework&g=org.apache.hive&a=hive-jdbc-all&v=1.0-SNAPSHOT&e=jar'
HIVE_JDBC_JAR_NAME='test-framework-hive-jdbc-all.jar'

if [ ! -f ${HIVE_JDBC_JAR_NAME} ]; then
    wget ${HIVE_JDBC_DOWNLOAD_URL} -O ${HIVE_JDBC_JAR_NAME}
else
    echo "File ${HIVE_JDBC_JAR_NAME} already downloaded"
fi