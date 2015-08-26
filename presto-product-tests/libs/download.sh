#!/bin/bash
set -e;

HIVE_JDBC_DOWNLOAD_URL='http://bdch-ftp.td.teradata.com:8081/nexus/service/local/artifact/maven/redirect?r=test-framework&g=org.apache.hive&a=hive-jdbc-all&v=1.0-SNAPSHOT&e=jar'
HIVE_JDBC_JAR_NAME='test-framework-hive-jdbc-all.jar'

# when jar doesn't exist yet, or the jar is just empty file (previous download was failed)
if [[ (! -f ${HIVE_JDBC_JAR_NAME}) || (! -s ${HIVE_JDBC_JAR_NAME})  ]]; then
    rm -rf ${HIVE_JDBC_JAR_NAME}
    echo "Downloading ${HIVE_JDBC_JAR_NAME} ..."
    wget ${HIVE_JDBC_DOWNLOAD_URL} -O ${HIVE_JDBC_JAR_NAME}
    if [ $? -ne 0 ]; then
        echo "Download ${HIVE_JDBC_JAR_NAME} failed."
        rm -rf ${HIVE_JDBC_JAR_NAME}
        exit 1
    else
        echo "${HIVE_JDBC_JAR_NAME} has been successfully downloaded."
    fi
else
    echo "File ${HIVE_JDBC_JAR_NAME} already downloaded"
fi
