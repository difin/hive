#!/bin/bash
set -x

CLASSPATH=${HIVE_CONF_DIR}:/custom-jars/*:/custom-jars/lib/*:${HIVE_HOME}/lib/*:${TEZ_CONF_DIR}:${TEZ_HOME}/*:${TEZ_HOME}/lib/*:`${HADOOP_HOME}/bin/hadoop classpath`:${UDF_JARS_PATH}/conf:${UDF_JARS_PATH}/lib/udfs/*:/usr/lib/hadoop/share/hadoop/tools/lib/*:/aux-jars/*

# unix epoch time in ms
export APP_SUBMIT_TIME_ENV=`echo $(($(date +%s%N)/1000000))`
export USER=`whoami`
export JVM_PID="$$"
export NM_PORT=0
export NM_HTTP_PORT=0
unset CONTAINER_ID
export TEZ_AM_EXTERNAL_ID="${TEZ_AM_EXTERNAL_ID:-1}"
export TEZ_ASYNC_LOG_ENABLED="${TEZ_ASYNC_LOG_ENABLED:-true}"
export TEZ_LOG4J2_PROPERTIES_FILE_NAME="${TEZ_LOG4J2_PROPERTIES_FILE_NAME:-tez-edws-log4j2.properties}"
HIVE_LIB=${HIVE_HOME}/lib

export JVM_OPTS="-server -Djava.net.preferIPv4Stack=true -Dlog4j.configurationFile=${TEZ_LOG4J2_PROPERTIES_FILE_NAME} -DisThreadContextMapInheritable=true"
export HIVE_LOG4J2_PROPERTIES_FILE_NAME=${TEZ_LOG4J2_PROPERTIES_FILE_NAME} #make services run by hive executable log properly

if [ "${TEZ_ASYNC_LOG_ENABLED}" = true ] ; then
    echo "Async log enabled for tez.."
    export JVM_OPTS="${JVM_OPTS} -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector -Dlog4j2.asyncLoggerRingBufferSize=1000000"
fi

# TODO: Change back to ${HIVE_LIB} once we are done with the workaround hack from DWX-3985
AUTOSCALING_JAR=$(ls /custom-jars/dwx-autoscaling-*.jar | tail -n 1)

echo 'Waiting for LLAP'
hive --service jar ${AUTOSCALING_JAR} com.github.cloudera.llap.WaitForLlap;
if [ $? -ne 0 ]; then
    echo "Failed while waiting for LLAP instances, possibly timed out."
    exit 1
fi

if [ "${GET_TEZ_TOKEN}" == "true" ] ; then
    export HIVE_LOG4J2_PROPERTIES_FILE_NAME=hive-edws-log4j2.properties
    # HADOOP_TOKEN_FILE_LOCATION cannot be set while we are trying to generate the tokens because it tries to load credentials from that location.
    unset HADOOP_TOKEN_FILE_LOCATION
    # kinit already been done as part of entrypoint.sh
    if hdfs fetchdt ${TOKEN_FILE_PATH}; then
        printf 'HDFS fetch token succeeded\n'
    else
        printf 'HDFS fetch token failed\n'
        exit 1
    fi
    if [[ -n "$TEZ_FETCH_TOKENS_FOR_FS" ]] ; then
        IFS=';' read -ra fs_array <<< "$TEZ_FETCH_TOKENS_FOR_FS"
        for fs in "${fs_array[@]}"
        do
            if hdfs fetchdt -fs ${fs} /tmp/fs_token && hadoop dtutil append /tmp/fs_token ${TOKEN_FILE_PATH} && rm /tmp/fs_token; then
                printf "Delegation token fetch for ${fs} succeeded\n"
            else
                printf "Delegation token fetch for ${fs} failed\n"
                exit 1
            fi
        done
    else
        printf "No additional delegation tokens fetched\n"
    fi
    if hive --service jar ${AUTOSCALING_JAR} com.github.cloudera.llap.FetchLlapDT -principal ${SERVICE_PRINCIPAL} -keytab ${SERVICE_KEYTAB} ${TOKEN_FILE_PATH}; then
        printf 'LLAP token succeeded\n'
    else
        printf 'LLAP token failed\n'
        exit 1
    fi
    export HADOOP_TOKEN_FILE_LOCATION=${TOKEN_FILE_PATH}
fi

echo "Localizing UDF JARS"
/udf-jars-localizer.sh
if [ $? -ne 0 ]; then
    echo " Localization of UDF JARS failed"
fi

# JAVA_OPTS is additional jvm options that can be passed during container launch
JAVA_OPTS="${JAVA_OPTS_JDK11:-$JAVA_OPTS}"
exec $JAVA_HOME/bin/java -Dproc_querycoordinator -classpath "$CLASSPATH" ${JVM_OPTS} ${JAVA_OPTS} ${SERVICE_OPTS} org.apache.tez.dag.app.DAGAppMaster --session
