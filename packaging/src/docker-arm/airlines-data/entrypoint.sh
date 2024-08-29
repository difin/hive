#!/bin/bash

set -x;

if [ "${USE_KERBEROS}" == "true" ]; then
  SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
  SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
  kinit -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}
fi

if [ "${COPY_DATA}" == "true" ]; then
  echo "Starting data copy to ${DATAPATH}/airline_ontime_${DATA_TYPE}.db"
  hadoop fs -mkdir -p ${DATAPATH}/airline_ontime_${DATA_TYPE}.db
  hadoop fs -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider -Dfs.s3a.endpoint=s3.us-west-2.amazonaws.com -cp s3a://airlines-orc-protected/airlines_new_${DATA_LOAD_TYPE}.db/* ${DATAPATH}/airline_ontime_${DATA_TYPE}.db
fi

cd /airlines-data

. load.sh

if [ "${USE_KERBEROS}" == "true" ] && [ "${USE_RANGER}" == "true" ]; then
  . generate_ranger_policy.sh
fi
