#!/usr/bin/env zsh

usage() {
  echo
  echo "Script to build hive with code coverage and run Sonar analysis"
  echo "See usage at https://github.infra.cloudera.com/CDH/hive/tree/cdh_main/dev-support/code-coverage/readme.md"
  echo
  echo "Options:"
  echo "  -u     SonarQube Host URL"
  echo "  -k     SonarQube Project Key"
  echo "  -t     SonarQube Token"
  echo "  -s     Maven settings file"
  echo "  -h     Display help"
  echo
  echo "Important:"
  echo "  The required parameters for publishing the coverage results to SonarQube are:"
  echo "    - Host URL"
  echo "    - Project Key"
  echo "    - Token"
  echo
  echo "Example:"
  echo "  ./run-coverage.sh -n Hive -u https://sonarqube.infra.cloudera.com/ -k org.apache.hive:hive -t SONAR_TOKEN -s ~/work/downstream/.m2/settings.xml"
}

execute() {
  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
  PROJECT_ROOT="${SCRIPT_DIR}/../.."
  ROOT_POM="${PROJECT_ROOT}/pom.xml"

  echo "Compile..."
  mvn clean install --batch-mode --errors --no-transfer-progress --file "${ROOT_POM}" -PwithCoverage,itests,dist -DskipTests -Denforcer.skip -s "${MAVEN_SETTINGS_FILE}" -Dtest=TestParseDriver#nonExistent

  echo "Running tests with coverage..."
  mvn org.jacoco:jacoco-maven-plugin:prepare-agent org.apache.maven.plugins:maven-surefire-plugin:test --batch-mode --errors --no-transfer-progress --file "${ROOT_POM}" -PwithCoverage,itests,dist -Denforcer.skip -s "${MAVEN_SETTINGS_FILE}"

  echo "Verifying and creating reports..."
  mvn verify --batch-mode --errors --no-transfer-progress --file "${ROOT_POM}" -PmergeCoverage,itests,dist -DskipTests -Denforcer.skip -s "${MAVEN_SETTINGS_FILE}"
  mvn verify --batch-mode --errors --no-transfer-progress --file "${ROOT_POM}" -PreportCoverage,itests,dist -DskipTests -Denforcer.skip -s "${MAVEN_SETTINGS_FILE}"

  echo "Running SonarQube analysis..."
  # If the required parameters are given, the code coverage results are uploaded to the SonarQube Server
  if [ -n "$SONAR_URL" ] && [ -n "$SONAR_PROJECT_KEY" ] && [ -n "$SONAR_TOKEN" ]; then
    CURRENT_JAVA_VER="$(jenv global)"
    jenv global 17
    mvn sonar:sonar --batch-mode --errors --file "${ROOT_POM}" -Psonar,itests,dist -DskipTests -Denforcer.skip -s "${MAVEN_SETTINGS_FILE}" \
      -Dsonar.host.url="$SONAR_URL" \
      -Dsonar.projectKey="$SONAR_PROJECT_KEY" \
      -Dsonar.token="$SONAR_TOKEN"
    jenv global "$CURRENT_JAVA_VER"
  fi

  echo "Build finished"
}

while getopts ":u:k:t:s:h" option; do
  case $option in
  u) SONAR_URL=${OPTARG:-} ;;
  k) SONAR_PROJECT_KEY=${OPTARG:-} ;;
  t) SONAR_TOKEN=${OPTARG:-} ;;
  s) MAVEN_SETTINGS_FILE=${OPTARG:-} ;;
  h) # Display usage
    usage
    exit
    ;;
  ?) # Invalid option
    echo "Error: Invalid option"
    usage
    exit
    ;;
  esac
done

# Start code analysis
execute
