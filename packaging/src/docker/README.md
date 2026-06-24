<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Hive Docker image

## Build instructions

The Docker image relies on multiple binaries (e.g, Hadoop, Solr, Atlas, Ranger, Tez, etc.) that are not included in the 
Hive source code. In the CDWH/CDH build the binaries are 
[copied manually](https://github.infra.cloudera.com/CDH/cdpd/blob/9d82bee3eb0f2acbdc84b0dc778a397a9c85c9bf/redhat8/custom.ini#L162) 
into the current directory before building the image. In order to build the image locally the relevant binaries need to
be downloaded and copied into the current directory.

The `download_dep_tars.sh` script can be used to download the required binaries.
```bash
./packaging/src/docker/download_dep_tars.sh
```
At the end, the script prints the command to run for building the image.
