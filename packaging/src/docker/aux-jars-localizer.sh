#!/bin/bash

set -x

DOWNLOAD_PATH=/aux-jars/

if [ "${USE_KERBEROS}" == "true" ]; then
  SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
  SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
  kinit -V -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}
  klist
fi

download_and_copy() {
  AUX_PATH=$1
  if ${HADOOP_HDFS_HOME}/bin/hdfs dfs -test -d ${AUX_PATH}; then
    echo "Copying dir $AUX_PATH to $DOWNLOAD_PATH"
    ${HADOOP_HDFS_HOME}/bin/hdfs dfs -copyToLocal ${AUX_PATH}/* .
  elif ${HADOOP_HDFS_HOME}/bin/hdfs dfs -test -f ${AUX_PATH}; then
    echo "Copying file $AUX_PATH to $DOWNLOAD_PATH"
    ${HADOOP_HDFS_HOME}/bin/hdfs dfs -copyToLocal ${AUX_PATH} .
  else
    echo "Cannot find $AUX_PATH"
  fi
}

if [[ -z "${CDW_HIVE_AUX_JARS_PATH}" ]]; then
  echo "CDW_HIVE_AUX_JARS_PATH is not defined. Skipping jars download from path.."
  exit
else
  cd ${DOWNLOAD_PATH}
  for f in $(echo $CDW_HIVE_AUX_JARS_PATH | tr ':' ' '); do
    download_and_copy $f
  done
  ls -l .
fi

cd ${DOWNLOAD_PATH}
find . -type f -iname "*.tar.gz" -print0 -execdir tar xf {} \; -delete
find . -name "*.jar" -exec md5sum {} \;
