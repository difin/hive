#!/bin/bash
set -x;

function data_load() {
  # if flights table exists we assume stats are correct and are analyzed by first table load
  ${HIVE_HOME}/bin/beeline --skiphadoopversion --skiphbasecp -u "${BEELINE_CONNECTION_URL}" -e "show databases;" | grep "airline_ontime_${DATA_TYPE}"
  if [ $? -eq 0 ]; then
      # flights table already exists, assuming tables already exists
      echo "Skipping data load as 'airline_ontime_${DATA_TYPE}' already exists.."
  else
      echo "Starting ${DATA_TYPE} data load of all airlines data.."
      ${HIVE_HOME}/bin/beeline --skiphadoopversion --skiphbasecp -u "${BEELINE_CONNECTION_URL}" -f ddl/airlines_${TABLE_TYPE}_${DATA_TYPE}.sql --hivevar datapath=${DATAPATH}
      if [ $? -eq 0 ]; then
        echo "Successfully loaded airlines database!"
      else
        echo "Failed loading airlines database!"
        exit 1
      fi
  fi
}

HS2_SERVICE=${HS2_SERVICE:-"hiveserver2-service"}
HS2_PORT=${HS2_PORT:-"10010"}

BEELINE_CONNECTION_URL="jdbc:hive2:///;transportMode=http;httpPath=cliservice;"

if [[ -z "${HS2_AUTH_USER}" ]]; then
  echo "HS2_AUTH_USER env is not set. Skipping auth.."
else
  BEELINE_CONNECTION_URL="${BEELINE_CONNECTION_URL};user=${HS2_AUTH_USER}"
fi

if [[ -z "${HS2_AUTH_PASSWORD}" ]]; then
  echo "HS2_AUTH_PASSWORD env is not set. Skipping auth.."
else
  BEELINE_CONNECTION_URL="${BEELINE_CONNECTION_URL};password=${HS2_AUTH_PASSWORD}"
fi

# wait until we can list databases, if we can list databases then schema init is done and data load can proceed (doesn't have to wait for sys tables)
while ! ${HIVE_HOME}/bin/beeline --skiphadoopversion --skiphbasecp -u "${BEELINE_CONNECTION_URL}/" -e "show databases;"; do
  sleep 10
  echo "Waiting to connect via beeline to ${BEELINE_CONNECTION_URL}.."
done

echo "Connected to ${HS2_SERVICE} on ${HS2_PORT}! Using beeline url: ${BEELINE_CONNECTION_URL}"

START=$(date +%s)

data_load

END=$(date +%s)
DIFF=$(echo "$END - $START" | bc)

echo "Data load job completed successfully! Time taken: ${DIFF} seconds"

