#!/bin/bash

set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

export JAVA_HOME="/opt/java/jdk11"

############################################
#            Main Program                  #
############################################

echo "Running scala tests with Scala $SCALA"

export MONGODB_URI="mongodb://localhost:27017"

./gradlew -version
./gradlew --stacktrace --info clean :bson-scala:test :driver-scala:test :driver-scala:integrationTest -Dorg.mongodb.test.uri=${MONGODB_URI} -PscalaVersion=$SCALA
