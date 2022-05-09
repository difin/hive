#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

set -x

if [ -z "$UDF_JARS_PATH" ]
then
  echo "UDF_JARS_PATH env is not set. Skipping UDF localization!"
  exit 0
fi

# Save UDF JARs/list to temp directory
mkdir -p $(dirname ${UDF_JARS_PATH})
TMP_UDF_PATH=$(mktemp -d ${UDF_JARS_PATH}-XXXXXX)
if [ ! -d "${TMP_UDF_PATH}" ]; then
  echo "Unable to create temp dir ${TMP_UDF_PATH} for UDF localization!"
  exit 1
fi

UDF_JAR_LOCALIZER_KERBEROS_OPTS=
if [ "${USE_KERBEROS}" == "true" ]; then
  UDF_JAR_LOCALIZER_KERBEROS_OPTS="--hiveconf hive.metastore.sasl.enabled=true --hiveconf hive.metastore.kerberos.principal=${METASTORE_SERVICE_PRINCIPAL}"
fi

${HIVE_HOME}/bin/hive --service jar ${HIVE_HOME}/lib/hive-llap-server-${CDH_VERSION}.jar org.apache.hadoop.hive.llap.cli.service.LlapServiceDriver -i 1 --directory ${TMP_UDF_PATH} --skipValidateConf --partialDownload --downloadType udffile $UDF_JAR_LOCALIZER_KERBEROS_OPTS

if [ $? -ne 0 ]; then
  echo "Failed to localize UDF JARs to ${TMP_UDF_PATH}!"
  exit 1
fi

ln -sfn ${TMP_UDF_PATH} ${UDF_JARS_PATH}

find ${UDF_JARS_PATH}/lib/udfs -name "*.jar" -exec md5sum {} \;
cat ${UDF_JARS_PATH}/conf/llap-udfs.lst

# Clean up any old temp directories
MAX_AGE=3600
CURRTIME=$(date +%s)
ls -d ${UDF_JARS_PATH}-* | while read DIR; do
  if [ "${DIR}" != "${TMP_UDF_PATH}" ]; then
    DIR_MTIME=$(date -r ${DIR} +%s)
    if [ $((${CURRTIME} - ${DIR_MTIME})) -gt ${MAX_AGE} ]; then
      echo "Cleaning up old udf dir " ${DIR}
      rm -rf ${DIR}
    fi
  fi
done

exit 0
