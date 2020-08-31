#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
set -e
set -x

nc -zv -w 2 127.0.0.1 ${HEALTH_CHECK_PORT}