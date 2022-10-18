#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

set -e
set -x

if [ "${EDWS_DRIVER}" == "yarn" ]; then
  . /fluentd-utils.sh
  start_fluentd
fi

if [[ "${USE_KERBEROS}" == "true" && -n "$SERVICE_KEYTAB" ]]; then
  SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
  SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
  kinit -V -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}
  klist
fi

# For graceful shutdown of processes, the JVM process should run as PID 1 to receive SIGTERM from k8s.
# Running as exec will completely replace this shell script (not run as child process)
if [ "${EDWS_SERVICE_NAME}" == "query-executor" ]; then
  exec /llap-entrypoint.sh
elif [ "${EDWS_SERVICE_NAME}" == "query-coordinator" ]; then
  exec /tez-entrypoint.sh
elif [ "${EDWS_SERVICE_NAME}" == "data-load" ]; then
  exec /airlines-data/entrypoint.sh
else
  exec /hive-entrypoint.sh
fi

if [ "${EDWS_DRIVER}" == "yarn" ]; then
  stop_fluentd
fi