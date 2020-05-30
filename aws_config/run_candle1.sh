#!/bin/sh

# Usage: eg
# aws_config/run_candle1.sh
# aws_config/run_candle1.sh --async

set -e

# Set mydir to the directory containing the script
# The ${var%pattern} format will remove the shortest match of
# pattern from the end of the string. Here, it will remove the
# script's name, leaving only the directory.
DIR="${0%/*}"

JAR="$DIR/../jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
# JOB="dev.posco.hiona.jobs.Candle1DBLambdaJob"
JOB="dev.posco.hiona.jobs.FinnhubDBCandle1Lambda"

java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp "$JAR" dev.posco.hiona.aws.LambdaDeployApp \
  invoke_remote \
  --jar "$JAR" \
  --cas_root s3://predictionmachine-data/cas/ \
  --method $JOB \
  --payload "$(cat <<-'EOF'
      {
        "body": {
          "args":
            [
              "run",
              "--logevery", "24h",
              "--limit", "1000",
              "--output", "s3://predictionmachine-data/temp/candle1_1000_exch.csv"
            ]
          }
      }
EOF
)" \
  --role arn:aws:iam::131579175100:role/hiona-FinnhubBars-functionRole-1DGE5DL8HGZRQ \
  --subnet subnet-108a2467 \
  --sec_group sg-2defd948 \
  $1
