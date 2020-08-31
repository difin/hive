#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

set -e -x

SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
RANGER_URL=${RANGER_URL:?RANGER_URL is required for ranger repo deletion}
SERVICE_NAME=${SERVICE_NAME:?SERVICE_NAME is required for ranger repo deletion}

until $(curl --output /dev/null --silent --head ${RANGER_URL}); do
    echo "waiting to connect to ${RANGER_URL} .."
    sleep 5
done

kinit -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}

status_code=$(curl --write-out %{http_code} --silent --output /tmp/ranger-resp.txt -X DELETE --negotiate -u : "${RANGER_URL}/service/public/v2/api/service/name/${SERVICE_NAME}")

if [[ "$status_code" -ne 204 ]] ; then
  echo "Ranger repo ${SERVICE_NAME} deletion failed! status_code: $status_code"
  cat /tmp/ranger-resp.txt
  exit 1
else
  echo "Ranger repo ${SERVICE_NAME} deletion successful!"
  exit 0
fi