#!/bin/bash
set -e -x

RANGER_URL=${RANGER_URL:?RANGER_URL is required for ranger policy creation}
SERVICE_NAME=${SERVICE_NAME:?SERVICE_NAME is required ranger for policy creation}

generate_ranger_policy() {
cat << EOF
{
    "allowExceptions": [],
    "dataMaskPolicyItems": [],
    "denyExceptions": [],
    "denyPolicyItems": [],
    "description": "Policy for public demo data airline_ontime_${DATA_TYPE}",
    "isAuditEnabled": true,
    "isDenyAllElse": false,
    "isEnabled": true,
    "name": "airline_ontime_${DATA_TYPE}",
    "options": {},
    "policyItems": [
        {
            "accesses": [
                {
                    "isAllowed": true,
                    "type": "All"
                }
            ],
            "conditions": [],
            "delegateAdmin": false,
            "groups": ["public"],
            "roles": [],
            "users": []
        }
    ],
    "policyLabels": [],
    "policyPriority": 0,
    "policyType": 0,
    "resources": {
        "column": {
            "isExcludes": false,
            "isRecursive": false,
            "values": [
                "*"
            ]
        },
        "database": {
            "isExcludes": false,
            "isRecursive": false,
            "values": [
                "airline_ontime_${DATA_TYPE}"
            ]
        },
        "table": {
            "isExcludes": false,
            "isRecursive": false,
            "values": [
                "*"
            ]
        }
    },
    "rowFilterPolicyItems": [],
    "service": "${SERVICE_NAME}",
    "serviceType": "hive",
    "validitySchedules": [],
    "zoneName": ""
}
EOF
}

until $(curl --output /dev/null --silent --head ${RANGER_URL}); do
    echo "waiting to connect to ${RANGER_URL} .."
    sleep 5
done

status_code=$(curl --write-out %{http_code} --silent --output /tmp/ranger-resp.txt \
  -H "Accept: application/json" -H "Content-Type:application/json" \
  -X POST --data "$(generate_ranger_policy)" --negotiate -u : "${RANGER_URL}/service/public/v2/api/policy")

if [[ "$status_code" -ne 200 ]] ; then
  echo "Sending read-write Ranger policy for db airline_ontime_${DATA_TYPE} failed! status_code: $status_code"
  cat /tmp/ranger-resp.txt
  exit 1
else
  echo "Set read-write Ranger policy for db airline_ontime_${DATA_TYPE}"
  exit 0
fi