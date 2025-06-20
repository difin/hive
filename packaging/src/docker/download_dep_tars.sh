# This script helps to download dependency tars needed for hive image (referenced in Dockerfile)
# 1. Fetches latest build for a certain CDH_PREFIX, CDWH_PREFIX
# 2. If CDH_VERSION, CDWH_VERSION are defined, they're used instead of the fetched build number
# 3. Resolves the url on which the tar artifacts of the builds are available
# 4. Downloads artifacts to scripts's folder
# 5. Prints a recommended way to build hive image from hive repo root folder

PLATFORM="${PLATFORM:-redhat8}"
CDH_PREFIX=${CDH_PREFIX:=$(sed -nE 's|.*<hadoop.version>.*([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)-[0-9]+.*</hadoop.version>.*|\1|p' pom.xml)} # according to hadoop.version in pom.xml
CDWH_PREFIX=${CDWH_PREFIX:=$(curl -s "https://release.infra.cloudera.com/hwre-api/stackinfo?stack=CDWH&build_type=dev" | jq '.[] | select(.branch=="cdw-master")' | jq ."stack_version" | tr -d '"')}

function get_component_version(){
    COMPONENT=$1
    CDH_VERSION=$2
    STACK=$3
    METADATA_URL="https://release.infra.cloudera.com/hwre-api/getbuildmetadata?stack=${STACK}&release=${CDH_VERSION}"
    echo $(curl -s ${METADATA_URL} | jq '.'\"$COMPONENT\"'.component_version' | tr -d '"')
}

if [ -n "$ZSH_VERSION" ]; then
   SCRIPT_DIR="${0:a:h}"
else
   SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
fi
DOCKER_FENG_DIR="$SCRIPT_DIR/../docker-feng"
CDWH_YEAR="${CDWH_PREFIX%%\.*}"

BUILD_INFO_URL="https://release.infra.cloudera.com/hwre-api/latestcompiledbuild?stack=CDH&release=${CDH_PREFIX}&os{PLATFORM}"

if [ -z $CDH_VERSION ]; then
    CDH_VERSION=$(curl -s $BUILD_INFO_URL | jq '.build' | tr -d '"')
else #CDH_VERSION can be forced from ENV, in this case version specific latestcompiledbuild can be called
    BUILD_INFO_URL="https://release.infra.cloudera.com/hwre-api/latestcompiledbuild?stack=CDH&release=${CDH_VERSION}&os{PLATFORM}"
fi

echo "CDH_VERSION=${CDH_VERSION}"

CDH_GBN=$(curl -s $BUILD_INFO_URL | jq '.gbn' | tr -d '"')
CDH_TARS_URL=https://cloudera-build-us-west-1.vpc.cloudera.com/s3/build/${CDH_GBN}/cdh/7.x/$PLATFORM/yum/tars/
echo "CDH_TARS_URL=${CDH_TARS_URL}"

CDWH_BUILD_URL="https://release.infra.cloudera.com/hwre-api/stackinfo?stack=CDWH&build_type=dev"
if [ -z $CDWH_VERSION ]; then #CDWH_VERSION can be forced from ENV, like: export CDWH_VERSION=2022.0.9.0-14
    CDWH_VERSION=$(curl -s ${CDWH_BUILD_URL} | jq '.["'${CDWH_PREFIX}'"].last_sucessful_build' | tr -d '"')
fi
echo "CDWH_VERSION=${CDWH_VERSION}"

export CDWH_REPO_DETAILS_URL="https://release.infra.cloudera.com/hwre-api/versioninfo?stack=CDWH&stack_version=${CDWH_VERSION}&per_page=50"
CDWH_REPO_URL=$(curl -s $CDWH_REPO_DETAILS_URL | jq '.["'${CDWH_VERSION}'"]'.platforms.${PLATFORM}.repo_url | tr -d '"')

CDWH_GBN=$(curl -s $CDWH_REPO_DETAILS_URL | jq '.["'${CDWH_VERSION}'"]'.gbn | tr -d '"')
CDWH_TARS_URL=https://cloudera-build-us-west-1.vpc.cloudera.com/s3/build/${CDWH_GBN}/cdwh/${CDWH_YEAR}.x/$PLATFORM/yum/tars/
echo "CDWH_TARS_URL=${CDWH_TARS_URL}"

TEZ_VERSION=$(get_component_version "tez" $CDWH_VERSION, "CDWH")
HADOOP_VERSION=$(get_component_version "hadoop" $CDH_VERSION, "CDH")
SOLR_VERSION=$(get_component_version "solr" $CDH_VERSION, "CDH")
RANGER_VERSION=$(get_component_version "ranger" $CDH_VERSION, "CDH")
ATLAS_VERSION=$(get_component_version "atlas" $CDH_VERSION, "CDH")
IMPALA_VERSION=$(get_component_version "impala" $CDH_VERSION, "CDH")

HADOOP_TAR_FILE=hadoop-$HADOOP_VERSION.tar.gz
HADOOP_TAR_URL="$CDH_TARS_URL/hadoop/$HADOOP_TAR_FILE"
if [ ! -f "$SCRIPT_DIR/$HADOOP_TAR_FILE" ]; then
    echo "HADOOP_TAR_URL=${HADOOP_TAR_URL}"
    curl ${HADOOP_TAR_URL} --output $SCRIPT_DIR/$HADOOP_TAR_FILE
fi

RANGER_TAR_FILE=ranger-$RANGER_VERSION-hive-plugin.tar.gz
RANGER_TAR_URL="$CDH_TARS_URL/ranger/$RANGER_TAR_FILE"
if [ ! -f "$SCRIPT_DIR/$RANGER_TAR_FILE" ]; then
    echo "RANGER_TAR_URL=${RANGER_TAR_URL}"
    curl ${RANGER_TAR_URL} --output $SCRIPT_DIR/$RANGER_TAR_FILE
fi

SOLR_TAR_FILE=solr-$SOLR_VERSION.tar.gz
SOLR_TAR_URL="$CDH_TARS_URL/solr/$SOLR_TAR_FILE"
if [ ! -f "$SCRIPT_DIR/$SOLR_TAR_FILE" ]; then
    echo "SOLR_TAR_URL=${SOLR_TAR_URL}"
    curl ${SOLR_TAR_URL} --output $SCRIPT_DIR/$SOLR_TAR_FILE
fi

ATLAS_TAR_FILE=apache-atlas-$ATLAS_VERSION-hive-hook.tar.gz
ATLAS_TAR_URL="$CDH_TARS_URL/atlas/$ATLAS_TAR_FILE"
if [ ! -f "$SCRIPT_DIR/$ATLAS_TAR_FILE" ]; then
    echo "ATLAS_TAR_URL=${ATLAS_TAR_URL}"
    curl ${ATLAS_TAR_URL} --output $SCRIPT_DIR/$ATLAS_TAR_FILE
fi

TEZ_TAR_FILE=tez-$TEZ_VERSION.tar.gz
TEZ_TAR_URL="$CDWH_TARS_URL/tez/$TEZ_TAR_FILE"
if [ ! -f "$SCRIPT_DIR/$TEZ_TAR_FILE" ]; then
    echo "TEZ_TAR_URL=${TEZ_TAR_URL}"
    curl ${TEZ_TAR_URL} --output $SCRIPT_DIR/$TEZ_TAR_FILE
fi

IMPALA_TAR_FILE=impala-fesupport-${IMPALA_VERSION}.tar.gz
IMPALA_TAR_URL="$CDWH_TARS_URL/impala/$IMPALA_TAR_FILE"
if [ ! -f "$DOCKER_FENG_DIR/$IMPALA_TAR_FILE" ]; then
    echo "IMPALA_TAR_URL=${IMPALA_TAR_URL}"
    curl ${IMPALA_TAR_URL} --output $DOCKER_FENG_DIR/$IMPALA_TAR_FILE
fi


echo "\n*** YOU CAN BUILD HIVE IMAGE AS DESCRIBED BELOW ***\n"
echo "unset CDH_VERSION # it should be picked up from packaging/pom.xml as the project.version"
echo "mvn docker:build -Pdocker -pl packaging -DskipTests -Dmaven.javadoc.skip=true -Denforcer.skip=true -Dtez.version=$TEZ_VERSION -Dhadoop.version=$HADOOP_VERSION -Dsolr.version=$SOLR_VERSION -Dranger.version=$RANGER_VERSION -Datlas.version=$ATLAS_VERSION -Dimpala.version=$IMPALA_VERSION -Ddocker.verbose"