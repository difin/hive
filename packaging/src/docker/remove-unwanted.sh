#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

find . -name *.tar.gz | xargs rm -f
find . -name *curator*-2.* | xargs rm -f
rm -rf /usr/lib/hadoop/share/hadoop/hdfs/webapps/
rm -rf /usr/lib/hadoop/share/hadoop/common/sources/
rm -rf /usr/lib/hadoop/share/hadoop/common/webapps/
rm -rf /usr/lib/hadoop/share/hadoop/mapreduce/sources/
rm -rf /usr/lib/hadoop/share/hadoop/yarn/timelineservice
rm -rf /usr/lib/hadoop/share/hadoop/yarn/sources/
rm -rf /usr/lib/hadoop/share/hadoop/yarn/webapps
rm -rf /usr/lib/hadoop/share/hadoop/common/lib/jackson-*
rm -rf /usr/lib/hive/jdbc
rm -rf /usr/lib/hive/auxlib
rm -rf /usr/lib/hive/data
rm -rf /usr/lib/hive/contrib
rm -rf /usr/lib/hive/docs
rm -rf /usr/lib/hive/examples
rm -rf /usr/lib/hive/hcatalog
rm -rf /usr/lib/hive/lib/php
rm -rf /usr/lib/hive/lib/py
rm -rf /usr/lib/hive/lib/hbase*
rm -rf /usr/lib/hive/lib/accumulo*
rm -rf /usr/lib/hive/lib/druid*
rm -rf /tmp/solr
rm -rf /usr/lib/tez/lib

