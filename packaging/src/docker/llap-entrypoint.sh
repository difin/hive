#!/bin/bash
# Copyright (c) 2020 Cloudera, Inc. All rights reserved.

# retrieve ec2 instance's public hostname and ip via metadata API
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
export HIVE_LOG4J2_PROPERTIES_FILE_NAME=${LLAP_LOG4J2_PROPERTIES_FILE_NAME} #make services run by hive executable log properly

# HADOOP_CLIENT_OPTS variable is applied to all processes started by the 'hive' executable
export HADOOP_CLIENT_OPTS="-Xmx2048m ${HADOOP_CLIENT_OPTS}"

RESP=`curl --connect-timeout 2 -s -f http://169.254.169.254/latest/meta-data/public-hostname`
exit_status=$?
FQDN=`hostname -f`
if [ $exit_status -eq 0 ]; then
    export PUBLIC_HOSTNAME=$RESP
    echo "Public hostname of ${FQDN} is ${PUBLIC_HOSTNAME}"
fi

RESP=`curl --connect-timeout 2 -s -f http://169.254.169.254/latest/meta-data/public-ipv4`
exit_status=$?
if [ $exit_status -eq 0 ]; then
    export PUBLIC_HOST_IP=$RESP
    echo "Public ip address of ${FQDN} is ${PUBLIC_HOST_IP}"
fi

echo "Localizing UDF JARS"
#point to kerberized hadoop config
export CONF_DIRS=/etc/hadoop/conf-kerberos
/udf-jars-localizer.sh
if [ $? -ne 0 ]; then
    echo " Localization of UDF JARS failed"
fi
unset CONF_DIRS #back to non-kerberized

export LLAP_DAEMON_USER_CLASSPATH=${HIVE_HOME}/lib/*:`${HADOOP_HOME}/bin/hadoop classpath`:${TEZ_HOME}/*:${TEZ_HOME}/lib/*:/aux-jars/*
exec ${HIVE_HOME}/scripts/llap/bin/llapDaemon.sh start
