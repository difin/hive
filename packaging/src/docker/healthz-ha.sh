#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
set -e

IS_LEADER=$(curl -s http://localhost:${HEALTH_CHECK_PORT}/ha-healthcheck/health-ha)

if [ "$IS_LEADER" == "true" ]; then
  echo "IS_LEADER=true"
  exit 0
else
  echo "IS_LEADER=false"
  exit 1
fi