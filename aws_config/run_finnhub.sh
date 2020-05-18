#!/bin/sh

set -e

JAR="../jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JOB="dev.posco.hiona.jobs.AwsFinnhubBars"

java -cp $JAR dev.posco.hiona.aws.LambdaDeployApp \
  invoke_remote \
  --jar $JAR \
  --cas_root s3://predictionmachine-data/cas/ \
  --method $JOB \
  --payload_path finnhub_input.json \
  --role arn:aws:iam::131579175100:role/hiona-FinnhubBars-functionRole-1DGE5DL8HGZRQ
