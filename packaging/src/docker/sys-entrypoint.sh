#!/bin/bash
set -x

# Script used for information schema, sys db and DAS proto tables initialization. Uses embedded HS2 confs.
: ${DB_DRIVER:=postgres}

# Information schema
$HIVE_HOME/bin/schematool -dbType hive -metaDbType $DB_DRIVER -initSchema
if [ $? -eq 0 ]; then
  echo "Initialized information schema successfully.."
else
  echo "Information schema initialization failed!"
  exit 1
fi

# Create sys.logs
BEELINE_CONNECTION_URL="jdbc:hive2:///;transportMode=http;httpPath=cliservice;"
if [ "${CREATE_LOGS_TABLE}" == "true" ]; then
  ${HIVE_HOME}/bin/beeline --skiphadoopversion --skiphbasecp -u "${BEELINE_CONNECTION_URL}" -e "drop table if exists sys.logs;"
  $HIVE_HOME/bin/schematool -dbType hive -metaDbType $DB_DRIVER -createLogsTable ${REMOTE_LOGS_PATH} -retentionPeriod ${LOG_RETENTION_PERIOD}
  if [ $? -eq 0 ]; then
    echo "Creation of logs table succeeded."
  else
    echo "Creation of logs table failed!"
    exit 1
  fi
fi

${HIVE_HOME}/bin/beeline --skiphadoopversion --skiphbasecp -u "${BEELINE_CONNECTION_URL}" -f /sys-db-proto-hooks.sql
if [ $? -eq 0 ]; then
    echo "Successfully loaded sys.db proto hooks tables!"
else
    echo "Failed loading sys.db proto hooks tables!"
    exit 1
fi
