#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
set -e
set -x

# FIXME: This is workaround for DWX-1645
# Proper fix for this will come from CDPD-5246

# This makes sure we can connect to postgres service and query HMS specific metrics exposed as for prometheus via JMX exporter.
# If HMS is alive the metrics should exist and grep should return 0 exit code.
nc -zv -w 2 postgres-service 5432 && curl -s http://localhost:35000 | grep "^api_init"
