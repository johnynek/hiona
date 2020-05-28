#!/bin/sh

set -e

JAR="../jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"

java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp $JAR dev.posco.hiona.aws.LambdaDeployApp \
  delete --name $1
