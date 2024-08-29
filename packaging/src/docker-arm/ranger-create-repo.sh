#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

set -e -x

SERVICE_KEYTAB=${SERVICE_KEYTAB:?SERVICE_KEYTAB is required for kinit}
SERVICE_PRINCIPAL=${SERVICE_PRINCIPAL:?SERVICE_PRINCIPAL is required for kinit}
RANGER_URL=${RANGER_URL:?RANGER_URL is required for ranger repo creation}
DWX_CLUSTER_ID=${DWX_CLUSTER_ID:?DWX_CLUSTER_ID is required for ranger repo creation}
RANGER_CLUSTER_NAME=${RANGER_CLUSTER_NAME:?RANGER_CLUSTER_NAME is required for ranger repo creation}
WAREHOUSE_ID=${WAREHOUSE_ID:?WAREHOUSE_ID is required for ranger repo creation}
WAREHOUSE_NAME=${WAREHOUSE_NAME:?WAREHOUSE_NAME is required for ranger repo creation}
SERVICE_NAME=${SERVICE_NAME:?SERVICE_NAME is required ranger for repo creation}

generate_post_body() {
cat << EOF
{
   "name":"${SERVICE_NAME}",
   "description":"Ranger repo for DWX hive warehouse (id: ${WAREHOUSE_ID} name: ${WAREHOUSE_NAME} ranger-cluster-name: ${RANGER_CLUSTER_NAME} dwx-cluster-id: ${DWX_CLUSTER_ID})",
   "isEnabled":true,
   "tagService":"cm_tag",
   "username":"hive",
   "password":"hive",
   "jdbc.driverClassName":"org.apache.hive.jdbc.HiveDriver",
   "jdbc.url":"jdbc:hive2://127.0.0.1:10000/default",
   "configs":{
      "username":"hive",
      "password":"hive",
      "jdbc.driverClassName":"org.apache.hive.jdbc.HiveDriver",
      "jdbc.url":"jdbc:hive2://127.0.0.1:10000/default",
      "commonNameForCertificate":"",
      "tag.download.auth.users":"hive,impala",
      "policy.download.auth.users":"hive,impala",
      "policy.grantrevoke.auth.users":"hive,impala",
      "service.check.user":"hive,impala",
      "enable.hive.metastore.lookup":true,
      "hive.site.file.path":"/etc/hive/conf/hive-site.xml"
   },
   "type":"hive"
}
EOF
}

kinit -k -t ${SERVICE_KEYTAB} ${SERVICE_PRINCIPAL}

FILE=/mnt/config/current/principal.env
if [ -f "$FILE" ]; then
    until $(curl --output /dev/null --insecure   --silent --head ${RANGER_URL}); do
        echo "waiting to connect to ${RANGER_URL} .."
        sleep 5
    done
    status_code=$(curl --write-out %{http_code} --silent --insecure --output /tmp/ranger-resp.txt -H "Accept: application/json" -H "Content-Type:application/json" -X POST --data "$(generate_post_body)" --negotiate -u : "${RANGER_URL}/service/public/v2/api/service")
else
    until $(curl --output /dev/null --silent --head ${RANGER_URL}); do
        echo "waiting to connect to ${RANGER_URL} .."
        sleep 5
    done
    status_code=$(curl --write-out %{http_code} --silent --output /tmp/ranger-resp.txt -H "Accept: application/json" -H "Content-Type:application/json" -X POST --data "$(generate_post_body)" --negotiate -u : "${RANGER_URL}/service/public/v2/api/service")
fi

if [[ "$status_code" -ne 200 ]] ; then
  echo "Ranger repo ${SERVICE_NAME} creation failed! status_code: $status_code"
  cat /tmp/ranger-resp.txt
  exit 1
else
  echo "Ranger repo ${SERVICE_NAME} creation successful!"
  exit 0
fi
