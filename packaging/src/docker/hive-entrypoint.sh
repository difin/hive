#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
set -x

: ${DB_DRIVER:=postgres}

SCHEMATOOL_COMMAND="$HIVE_HOME/bin/schematool -dbType $DB_DRIVER"
SKIP_SCHEMA_INIT="${IS_RESUME:-false}"

function is_metastore_initialized {
  ${SCHEMATOOL_COMMAND} -info
  return $?
}

function initialize_hive {
  # Warehouse paths
  ${HADOOP_HDFS_HOME}/bin/hdfs dfs -mkdir -p ${HIVE_MANAGED_WAREHOUSE_PATH} ${HIVE_EXTERNAL_WAREHOUSE_PATH}

  # Metastore DB
  $HIVE_HOME/bin/schematool -dbType $DB_DRIVER -initOrUpgradeSchema
  if [ $? -eq 0 ]; then
    echo "Initialized schema successfully.."
  else
    echo "Schema initialization failed!"
    exit 1
  fi
}

# handles schema initialization
if [[ "${EDWS_SERVICE_NAME}" == "metastore" && "${SKIP_SCHEMA_INIT}" == "false" ]]; then
  initialize_hive
fi

if [ "${EDWS_SERVICE_NAME}" == "hiveserver2" ]; then
  if [[ "${CLOUD_PLATFORM}" == "aws" && "${INTERMEDIATE_FS}" == "efs" ]]; then
    echo "Setting up efs dirs.."
    [[ -z "${SCRATCH_DIR}" ]] && mkdir -p ${SCRATCH_DIR}
    [[ -z "${STAGING_DIR}" ]] && mkdir -p ${STAGING_DIR}
    [[ -z "${RESULTS_CACHE_DIR}" ]] && mkdir -p ${RESULTS_CACHE_DIR}
    [[ -z "${USER_DIR}" ]] && mkdir -p ${USER_DIR}
  fi
  echo "Localizing UDF JARs ..."
  /udf-jars-localizer.sh
  if [ $? -ne 0 ]; then
    echo "Localization of UDF JARs failed."
    # Disable failure here until new hive image is pushed with HIVE-22050, or hs2 will never start
    #exit 1
  fi
fi

if [[ "${EDWS_SERVICE_NAME}" == "metastore" || "${EDWS_SERVICE_NAME}" == "hiveserver2" ]]; then
    export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} ${SERVICE_OPTS}"
fi

exec ${HIVE_HOME}/bin/hive --skiphadoopversion --skiphbasecp --service ${EDWS_SERVICE_NAME}
