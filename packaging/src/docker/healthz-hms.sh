#!/bin/bash
# Copyright (c) 2023 Cloudera, Inc. All rights reserved.
set -e
set -x

DB_ENDPOINT=${DB_ENDPOINT:-postgres-service}
DB_PORT=${DB_PORT:-5432}

# FIXME: This is workaround for DWX-1645
# Proper fix for this will come from CDPD-5246

# This makes sure we can connect to database service and query HMS specific metrics exposed as for prometheus via JMX exporter.
# If HMS is alive the metrics should exist and grep should return 0 exit code.
nmap -Pn -oG - -p $DB_PORT $DB_ENDPOINT | awk -F/ '{print $2}' | grep "open" && curl -s http://localhost:35000 | grep "^api_init"
