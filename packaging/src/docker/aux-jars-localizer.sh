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
    # Store single words which are most likely scheme Ex: s3a, file, abfs, hdfs, ...
    if [[ "$f" =~ ^[A-Za-z][-A-Za-z0-9+.]*$ ]]; then
      prev="$f"
      continue
    fi
    # If prev was set and current file starts with "//" prepend scheme
    # this is to create full path i.e s3a://bucket, hdfs://path ...
    if ! test -z "$prev"; then
      if [[ "$f" =~ ^// ]]; then
        f="$prev:$f"
      else
        # The match did not happen, we missed the prev, just try downloading it.
        download_and_copy $prev
      fi
    fi
    download_and_copy $f
    prev=""
  done
  ls -l .
fi

cd ${DOWNLOAD_PATH}
find . -type f -iname "*.tar.gz" -print0 -execdir tar xf {} \; -delete
find . -name "*.jar" -exec md5sum {} \;
