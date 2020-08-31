# Copyright (c) 2020 Cloudera, Inc. All rights reserved.
# UBI8 has issues with ranger-knox plugin not able to find Python
FROM registry.access.redhat.com/ubi7/ubi:latest as base-ubi-jdk

RUN curl -o /etc/yum.repos.d/rhel7_mirrors.repo http://mirror.infra.cloudera.com/repos/rhel/server/7/7.8/rhel7.8_cldr_mirrors.repo

# jdk and other debugging tools
RUN yum -y update && yum install -y java-1.8.0-openjdk-headless java-1.8.0-openjdk-devel.x86_64 strace sysstat bind-utils openssl openssl-devel wget vim nmap-ncat bc gnupg2 krb5-libs krb5-workstation krb5-auth-dialog gettext epel-release zstd libzstd iftop && \
  yum clean all && \
  rm -rf /var/cache/yum

# Skip mirror if it's unavailable
RUN yum-config-manager --save --setopt=ubi-server-rhscl-7-rpms.skip_if_unavailable=true

RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

# add NOTICES directory
ADD /notices-files /NOTICES

ENV JAVA_HOME /etc/alternatives/jre

# by default JVM caches successful DNS resolution infinitely. If a pod or node dies, the ip address changes
# so we set TTL for DNS cache to non-zero value.
RUN echo "networkaddress.cache.ttl=30" >> $JAVA_HOME/lib/security/java.security

## base-ubi-jdk end

ARG CDH_VERSION
ARG HADOOP_VERSION
ARG TEZ_VERSION
ARG RANGER_VERSION
ARG ATLAS_VERSION
ARG SOLR_VERSION
WORKDIR /usr/lib/

LABEL "com.cloudera.dwx.cdh.version"="${CDH_VERSION}"
LABEL \
    "name"="Cloudera Data Warehouse - Hive Virtual Warehouse Engine." \
    "description"="Provides the engine executables for Hive based Virtual Warehouses." \
    "summary"="CDW Runtime component for Virtual Warehouse deployments." \
    "vendor"="Cloudera, Inc."

ENV CDH_VERSION="${CDH_VERSION}"
ENV HADOOP_VERSION="${HADOOP_VERSION}"

WORKDIR /usr/lib/
RUN useradd -ms /bin/bash hive
COPY remove-unwanted.sh /

# async-profiler for perf profiling
RUN mkdir /async-profiler && \
   cd /async-profiler && \
   wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v1.5/async-profiler-1.5-linux-x64.tar.gz && \
   tar -xvf async-profiler-1.5-linux-x64.tar.gz && \
   rm async-profiler-1.5-linux-x64.tar.gz && \
   cd -

# jmx-exporter for prometheus metrics
RUN mkdir /jmx-exporter && \
   cd /jmx-exporter && \
   wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar && \
   cd -

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

# This expects that the haddop.tar.gz is provided
COPY hadoop-${HADOOP_VERSION}.tar.gz createSymlink.sh remove-unwanted.sh healthz.sh healthz-ha.sh healthz-hms.sh /
COPY ranger-${RANGER_VERSION}-hive-plugin.tar.gz /
COPY tez-${TEZ_VERSION}.tar.gz /
COPY apache-atlas-${ATLAS_VERSION}-hive-hook.tar.gz /
COPY solr-${SOLR_VERSION}.tar.gz /

## Add hadoop from tar.gz
RUN mkdir -p /usr/lib/hadoop && \
    tar -zxf /hadoop-${HADOOP_VERSION}.tar.gz --strip-components=1 -C /usr/lib/hadoop && \
    cp /usr/lib/hadoop/bin/hadoop /usr/bin/hadoop && \
    cp /usr/lib/hadoop/bin/hdfs /usr/bin/hdfs && \
    mkdir -p /etc/hadoop/conf && \
    mkdir -p /usr/lib/hadoop-hdfs && \
    mkdir -p /usr/lib/hadoop-mapreduce && \
    mkdir -p /usr/lib/hadoop-yarn && \
    cp -R /usr/lib/hadoop/etc/hadoop/* /etc/hadoop/conf && \
    cp -R /usr/lib/hadoop/share/hadoop/hdfs/ /usr/lib/hadoop-hdfs && \
    cp -R /usr/lib/hadoop/share/hadoop/mapreduce/ /usr/lib/hadoop-mapreduce && \
    cp -R /usr/lib/hadoop/share/hadoop/yarn/ /usr/lib/hadoop-yarn && \
    cp /usr/lib/hadoop/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar /usr/lib/hadoop && \
    rm /hadoop-${HADOOP_VERSION}.tar.gz && \
    /remove-unwanted.sh

# Add ranger jars from tar.gz
RUN mkdir -p /usr/lib/ranger-hive-plugin && \
    tar -xzf /ranger-${RANGER_VERSION}-hive-plugin.tar.gz --strip-components=1 -C /usr/lib/ranger-hive-plugin && \
    mkdir -p /usr/lib/hive/lib/ranger-hive-plugin-impl && \
    cp -R /usr/lib/ranger-hive-plugin/lib/* /usr/lib/hive/lib && \
    rm /ranger-${RANGER_VERSION}-hive-plugin.tar.gz && \
    /remove-unwanted.sh

# solr jar replacement
RUN mkdir -p /tmp/solr && \
    tar -xvf /solr-${SOLR_VERSION}.tar.gz --strip-components=1 -C /tmp/solr && \
    yes | cp /tmp/solr/dist/solr-solrj-${SOLR_VERSION}.jar /usr/lib/hive/lib/ranger-hive-plugin-impl/solr-solrj-${SOLR_VERSION}.jar && \
    chmod 644 /usr/lib/hive/lib/ranger-hive-plugin-impl/solr-solrj-${SOLR_VERSION}.jar && \
    rm /solr-${SOLR_VERSION}.tar.gz && \
    /remove-unwanted.sh

# Add atlas jars from tar.gz
RUN mkdir -p /usr/lib/atlas && \
    tar -xzf /apache-atlas-${ATLAS_VERSION}-hive-hook.tar.gz --strip-components=1 -C /usr/lib/atlas && \
    mkdir -p /usr/lib/hive/lib/atlas-hive-plugin-impl && \
    cp -R /usr/lib/atlas/hook/hive/* /usr/lib/hive/lib/ && \
    rm /apache-atlas-${ATLAS_VERSION}-hive-hook.tar.gz && \
    rm /usr/lib/hive/lib/atlas-hive-plugin-impl/kafka-*.jar && \
    /remove-unwanted.sh

# Add tez jars from tar.gz
RUN mkdir -p /usr/lib/tez && \
    tar -xzf /tez-${TEZ_VERSION}.tar.gz -C /usr/lib/tez && \
    cp /usr/lib/tez/*.jar /usr/lib/hive && \
    cp /usr/lib/tez/lib/*.jar /usr/lib/hive && \
    /remove-unwanted.sh

# copy hive jars, dependencies and static resources
COPY maven/apache-hive-${CDH_VERSION}-bin /usr/lib/hive/
RUN mkdir -p /etc/hive/conf && \
    cp -R /usr/lib/hive/conf/* /etc/hive/conf && \
    cp /usr/lib/hive/bin/hive /usr/bin/hive && \
    mkdir -p /etc/tez/conf && \
    /createSymlink.sh ${CDH_VERSION} && \
    /remove-unwanted.sh

# install postgres client for debugging usage
# install mysql JDBC connector for mysql and mariadb and postgres JDBC driver, then move them where the schematool can find them
RUN mkdir -p /usr/lib/hive/lib/ && \
    yum -y update
RUN yum -y install postgresql mysql-connector-java postgresql-jdbc
RUN yum -y clean all && \
    rm -rf /var/cache/yum && \
    /remove-unwanted.sh && \
    cp /usr/share/java/mysql-connector-java.jar /usr/lib/hive/lib/ && \
    cp /usr/share/java/postgresql-jdbc.jar /usr/lib/hive/lib/

# switch user and all related env
USER hive
ENV USER hive

# home directories
ENV HADOOP_HOME_WARN_SUPPRESS true
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_LIBEXEC_DIR /usr/lib/hadoop/libexec
ENV HADOOP_COMMON_HOME /usr/lib/hadoop
ENV TEZ_HOME /usr/lib/hive
ENV HIVE_HOME /usr/lib/hive
ENV LLAP_DAEMON_HOME ${HIVE_HOME}
ENV LLAP_DAEMON_BIN_HOME ${HIVE_HOME}/scripts/llap/bin

# zlib native are present here in debian. also adding hadoop native libs
ENV LD_LIBRARY_PATH /lib/x86_64-linux-gnu:${HADOOP_HOME}/lib/native

# conf directories
ENV HADOOP_CONF_DIR /etc/hadoop/conf
ENV TEZ_CONF_DIR /etc/tez/conf
ENV HIVE_CONF_DIR /etc/hive/conf
ENV LLAP_DAEMON_CONF_DIR /etc/hive/conf

COPY entrypoint.sh hive-entrypoint.sh tez-entrypoint.sh llap-entrypoint.sh \
     custom-jars-localizer.sh udf-jars-localizer.sh sys-entrypoint.sh sys-db-proto-hooks.sql ranger-create-repo.sh ranger-drop-repo.sh /

# TODO: add support for foreground execution of these scripts?
COPY llapDaemon.sh runLlapDaemon.sh ${HIVE_HOME}/scripts/llap/bin/

# TODO: temp change for now to revert HIVE-18472
ADD hive ${HIVE_HOME}/bin/hive

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
