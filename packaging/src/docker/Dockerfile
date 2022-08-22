# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
FROM registry.access.redhat.com/ubi8/ubi:latest as hadoop-base
ARG HADOOP_VERSION
ADD hadoop-${HADOOP_VERSION}.tar.gz  /usr/lib/
RUN ln -s /usr/lib/hadoop-${HADOOP_VERSION} /usr/lib/hadoop
RUN rm -f /usr/lib/hadoop/share/hadoop/tools/lib/kafka-clients-0.8.2.1.jar
COPY remove-unwanted.sh /
RUN /remove-unwanted.sh

FROM registry.access.redhat.com/ubi8/ubi:latest as ranger-base
ARG RANGER_VERSION
ADD ranger-${RANGER_VERSION}-hive-plugin.tar.gz /usr/lib/
RUN ln -s /usr/lib/ranger-${RANGER_VERSION}-hive-plugin /usr/lib/ranger-hive-plugin
COPY remove-unwanted.sh /
RUN /remove-unwanted.sh

FROM registry.access.redhat.com/ubi8/ubi:latest as solr-base
ARG SOLR_VERSION
ADD solr-${SOLR_VERSION}.tar.gz /usr/lib/
COPY remove-unwanted.sh /
RUN /remove-unwanted.sh

FROM registry.access.redhat.com/ubi8/ubi:latest as atlas-base
ARG ATLAS_VERSION
ADD apache-atlas-${ATLAS_VERSION}-hive-hook.tar.gz /usr/lib/
COPY remove-unwanted.sh /
RUN /remove-unwanted.sh

FROM registry.access.redhat.com/ubi8/ubi:latest as tez-base
ARG TEZ_VERSION
ADD tez-${TEZ_VERSION}.tar.gz /usr/lib/tez/
COPY remove-unwanted.sh /
RUN /remove-unwanted.sh

FROM registry.access.redhat.com/ubi8/ubi:latest as hive-base
ARG CDH_VERSION
COPY maven/apache-hive-${CDH_VERSION}-bin /usr/lib/hive/
COPY remove-unwanted.sh /
RUN /remove-unwanted.sh

FROM registry.access.redhat.com/ubi8/ubi:latest as hive
# setup os and install packages
RUN curl -o /etc/yum.repos.d/rhel8_mirrors.repo http://mirror.infra.cloudera.com/repos/rhel/server/8/8.4/rhel8.4_cldr_mirrors.repo
# install debugging tools and other dependencies
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
RUN yum -y update && \
  yum install -y java-11-openjdk-headless java-11-openjdk-devel.x86_64 strace sysstat bind-utils openssl openssl-devel wget vim nmap-ncat bc gnupg2 krb5-libs krb5-workstation gettext epel-release zstd libzstd iftop procps && \
  yum clean all && \
  rm -rf /var/cache/yum

# add NOTICES directory
ADD /notices-files /NOTICES

ENV JAVA_HOME /etc/alternatives/jre

# by default JVM caches successful DNS resolution infinitely. If a pod or node dies, the ip address changes
# so we set TTL for DNS cache to non-zero value.
RUN echo "networkaddress.cache.ttl=30" >> $JAVA_HOME/lib/security/java.security

ARG CDH_VERSION
ARG HADOOP_VERSION
ARG TEZ_VERSION
ARG RANGER_VERSION
ARG ATLAS_VERSION
ARG SOLR_VERSION

LABEL "com.cloudera.dwx.cdh.version"="${CDH_VERSION}"
LABEL \
    "name"="Cloudera Data Warehouse - Hive Virtual Warehouse Engine." \
    "description"="Provides the engine executables for Hive based Virtual Warehouses." \
    "summary"="CDW Runtime component for Virtual Warehouse deployments." \
    "vendor"="Cloudera, Inc."

ENV CDH_VERSION="${CDH_VERSION}"
ENV HADOOP_VERSION="${HADOOP_VERSION}"

RUN useradd -ms /bin/bash hive

# async-profiler for perf profiling
RUN mkdir /async-profiler && \
   wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v1.5/async-profiler-1.5-linux-x64.tar.gz && \
   tar -xvf async-profiler-1.5-linux-x64.tar.gz -C /async-profiler && \
   rm async-profiler-1.5-linux-x64.tar.gz

# jmx-exporter for prometheus metrics
RUN mkdir /jmx-exporter && \
   wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar -O /jmx-exporter/jmx_prometheus_javaagent-0.11.0.jar

# llap work directory and logs directory, Ranger cache and audit directories
RUN mkdir -p /apps/llap/work && \
   mkdir /custom-jars && \
   mkdir -p /var/log/hive && \
   mkdir -p /var/log/hiveserver2 && \
   mkdir -p /var/log/metastore && \
   mkdir -p /var/log/query-coordinator && \
   mkdir -p /var/log/query-executor && \
   chown -R hive:hive /apps/llap && \
   chown -R hive:hive /var/log/hive && \
   chown -R hive:hive /var/log/hiveserver2 && \
   chown -R hive:hive /var/log/metastore && \
   chown -R hive:hive /var/log/query-coordinator && \
   chown -R hive:hive /var/log/query-executor && \
   chown -R hive:hive /custom-jars && \
   mkdir -p /var/log/hive/audit/hdfs/spool && \
   mkdir -p /var/log/hive/audit/solr/spool && \
   chown -R hive:hive /var/log/hive/audit && \
   mkdir -p /var/lib/ranger/hive/policy-cache && \
   chown -R hive:hive /var/lib/ranger/hive/policy-cache

COPY createSymlink.sh remove-unwanted.sh healthz.sh healthz-ha.sh healthz-hms.sh /

COPY --from=hadoop-base /usr/lib/hadoop-${HADOOP_VERSION}/ /usr/lib/hadoop/
RUN mkdir -p /etc/hadoop/conf
RUN mkdir -p /usr/lib/hadoop-hdfs
RUN mkdir -p /usr/lib/hadoop-mapreduce
RUN mkdir -p /usr/lib/hadoop-yarn
RUN ln -s /usr/lib/hadoop/etc/hadoop/* /etc/hadoop/conf
RUN ln -s /usr/lib/hadoop/share/hadoop/hdfs/ /usr/lib/hadoop-hdfs
RUN ln -s /usr/lib/hadoop/share/hadoop/mapreduce/ /usr/lib/hadoop-mapreduce
RUN ln -s /usr/lib/hadoop/share/hadoop/yarn/ /usr/lib/hadoop-yarn
RUN ln -s /usr/lib/hadoop/bin/hadoop /usr/bin/hadoop
RUN ln -s /usr/lib/hadoop/bin/hdfs /usr/bin/hdfs
RUN ln -s /usr/lib/hadoop/share/hadoop/tools/lib/* /usr/lib/hadoop/share/hadoop/common
RUN ln -s /usr/lib/hadoop/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar /usr/lib/hadoop/hadoop-aws-${HADOOP_VERSION}.jar

COPY --from=ranger-base /usr/lib/ranger-${RANGER_VERSION}-hive-plugin /usr/lib/ranger-hive-plugin
RUN mkdir -p /usr/lib/hive/lib/
RUN ln -s /usr/lib/ranger-hive-plugin/lib/* /usr/lib/hive/lib/

COPY --from=solr-base /usr/lib/solr-${SOLR_VERSION}/dist/solr-solrj-${SOLR_VERSION}.jar  /usr/lib/hive/lib/ranger-hive-plugin-impl/
RUN chmod 644 /usr/lib/hive/lib/ranger-hive-plugin-impl/solr-solrj-${SOLR_VERSION}.jar

COPY --from=atlas-base  /usr/lib/apache-atlas-hive-hook-${ATLAS_VERSION}/ /usr/lib/atlas/
RUN ln -s /usr/lib/atlas/hook/hive/* /usr/lib/hive/lib/

COPY --from=tez-base /usr/lib/tez /usr/lib/tez

COPY --from=hive-base /usr/lib/hive /usr/lib/hive
RUN mkdir -p /etc/hive/conf
RUN ln -s /usr/lib/hive/conf/* /etc/hive/conf
RUN mkdir -p /etc/tez/conf
RUN /createSymlink.sh ${CDH_VERSION}

RUN mkdir -p /usr/lib/hive/lib/

# install mysql JDBC connector for mysql and mariadb, and Oracle JDBC driver, then move them where the schematool can find them
RUN yum -y clean all && \
    yum -y install postgresql postgresql-jdbc && \
    rm -rf /var/cache/yum && \
    wget --no-verbose https://s3.amazonaws.com/dev.hortonworks.com/ARTIFACTS/jdbc/1911/ojdbc8.jar -O /usr/lib/hive/lib/oracle-connector-java.jar && \
    wget --no-verbose https://mirror.infra.cloudera.com/mysql/mysql-connector-java-8.0.28.jar -O /usr/lib/hive/lib/mysql-connector-java-8.0.28.jar && \
    /remove-unwanted.sh
RUN ln -sF /usr/share/java/postgresql-jdbc.jar /usr/lib/hive/lib/postgresql-jdbc.jar

# postgres-jdbc fetches java-8 as a dependency, let's make java11 as default, pointed by /etc/alternatives/java
RUN alternatives --set java java-11-openjdk.x86_64

COPY bigtop-hive /usr/bin/hive
RUN ln -s /usr/bin/hive /usr/bin/beeline
RUN ln -s /usr/bin/hive /usr/bin/hiveserver2

# switch user and all related env
USER hive
ENV USER hive

# home directories
ENV HADOOP_HOME_WARN_SUPPRESS true
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_LIBEXEC_DIR /usr/lib/hadoop/libexec
ENV HADOOP_COMMON_HOME /usr/lib/hadoop
ENV TEZ_HOME /usr/lib/tez
ENV HIVE_HOME /usr/lib/hive
ENV LLAP_DAEMON_HOME /usr/lib/hive
ENV LLAP_DAEMON_BIN_HOME /usr/lib/hive/scripts/llap/bin

# zlib native are present here in debian. also adding hadoop native libs
ENV LD_LIBRARY_PATH /lib/x86_64-linux-gnu:/usr/lib/hadoop/lib/native

# conf directories
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV TEZ_CONF_DIR /etc/tez/conf
ENV HIVE_CONF_DIR /etc/hive/conf
ENV LLAP_DAEMON_CONF_DIR /etc/hive/conf

COPY entrypoint.sh hive-entrypoint.sh tez-entrypoint.sh llap-entrypoint.sh \
     custom-jars-localizer.sh udf-jars-localizer.sh sys-entrypoint.sh \
     sys-db-proto-hooks.sql ranger-create-repo.sh ranger-drop-repo.sh \
     aux-jars-localizer.sh /

# for sample data loading
ADD airlines-data /airlines-data

# TODO: add support for foreground execution of these scripts?
COPY llapDaemon.sh runLlapDaemon.sh /usr/lib/hive/scripts/llap/bin/

# TODO: temp change for now to revert HIVE-18472
ADD hive /usr/lib/hive/bin/hive

ADD healthz.sh /
ADD healthz-ha.sh /
ADD healthz-hms.sh /

ENTRYPOINT ["/entrypoint.sh"]

# 9083 - metastore thrift service port
# 10000 - thrift port (binary mode)
# 10010 - thrift port (http mode)
# 10100 - HS2 web ui
# 25000 - api
# 25002 - llap web ui
# 25003 - output service
# 25004 - management
# 25551 - shuffle
# 50505 - debugging port
# 22222 - AM RPC port
# 33333 - Umbilical server port
EXPOSE 9083 10000 10010 10100 25000 25002 25003 25004 25551 50505 22222 33333
