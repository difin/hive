#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

set -x

DOWNLOAD_PATH="/custom-jars"

if [ "${USE_KERBEROS}" == "true" ]; then
	SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
	SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
	kinit -V -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}
	klist
fi

if [[ -z "${CUSTOM_JARS_URL}" ]]; then
	echo "CUSTOM_JARS_URL is not defined. Skipping jar download from url.."
else
	echo "CUSTOM_JARS_URL is set to ${CUSTOM_JARS_URL}. Downloading jar/tar to ${DOWNLOAD_PATH}.."
	cd ${DOWNLOAD_PATH}
	wget --no-verbose ${CUSTOM_JARS_URL}
fi

if [[ -z "${CUSTOM_JARS_PATH}" ]]; then
	echo "CUSTOM_JARS_PATH is not defined. Skipping jar download from path.."
else
	cd ${DOWNLOAD_PATH}
	${HADOOP_HDFS_HOME}/bin/hdfs dfs -test -d ${CUSTOM_JARS_PATH}
	if [ $? == 0 ]; then
		echo "CUSTOM_JARS_PATH is set to ${CUSTOM_JARS_PATH}. Copying jar/tar to ${DOWNLOAD_PATH}.."
		${HADOOP_HDFS_HOME}/bin/hdfs dfs -copyToLocal "${CUSTOM_JARS_PATH}"/* .
		ls -l .
		[ -d "./lib" ] && ls -l ./lib
	else
		echo "CUSTOM_JARS_PATH is set to ${CUSTOM_JARS_PATH} but the path does not exist."
	fi
fi

cd ${DOWNLOAD_PATH}
find . -type f -iname "*.tar.gz" -print0 -execdir tar xf {} \; -delete
find . -name "*.jar" -exec md5sum {} \;
