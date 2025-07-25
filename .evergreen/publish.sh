#!/bin/bash

# DO NOT ECHO COMMANDS AS THEY CONTAIN SECRETS!

set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################
RELATIVE_DIR_PATH="$(dirname "${BASH_SOURCE:-$0}")"
. "${RELATIVE_DIR_PATH}/setup-env.bash"

RELEASE=${RELEASE:false}

export ORG_GRADLE_PROJECT_nexusUsername=${NEXUS_USERNAME}
export ORG_GRADLE_PROJECT_nexusPassword=${NEXUS_PASSWORD}
export ORG_GRADLE_PROJECT_signingKey="${SIGNING_KEY}"
export ORG_GRADLE_PROJECT_signingPassword=${SIGNING_PASSWORD}

if [ "$RELEASE" == "true" ]; then
  TASK="publishArchives closeAndReleaseSonatypeStagingRepository"
else
  TASK="publishSnapshots"
fi

SYSTEM_PROPERTIES="-Dorg.gradle.internal.publish.checksums.insecure=true"

./gradlew -version
./gradlew ${SYSTEM_PROPERTIES} --stacktrace --info  ${TASK} # Scala 2.13 is published as result of this gradle execution.
./gradlew ${SYSTEM_PROPERTIES} --stacktrace --info :bson-scala:${TASK} :driver-scala:${TASK} -PscalaVersion=2.12
./gradlew ${SYSTEM_PROPERTIES} --stacktrace --info :bson-scala:${TASK} :driver-scala:${TASK} -PscalaVersion=2.11
