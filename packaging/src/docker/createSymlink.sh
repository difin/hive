#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
echo "version: " $1

for filename in /usr/lib/hive/lib/*$1.jar; do
    ln -s $filename ${filename%-$1.jar}.jar
done
