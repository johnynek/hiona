#!/bin/bash

set -eu
# Hiona colab nb: https://colab.research.google.com/drive/104WOl-31bBKMMvwRV7bI8HFV9Gg5wXS0

# Set DIR to the directory containing the script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

JARNAME="hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JAR="$DIR/../jobs/target/scala-2.13/$JARNAME"

S3BASE="s3://data-pm/avalanche/firstratedata"
PREFIX="top100stocks_tickers_"

S3OUT="s3://data-pm/sources/processed/firstratedata/us1500_2000_2019/"

function launch {
  INPUT="$S3BASE/$PREFIX$1"
  echo "input=$INPUT"
  TASKARN=$(
    java -cp $JAR dev.posco.hiona.aws.ECSDeployApp \
      run \
      --taskdef hiona_job \
      --cluster pm-fargate-oregon \
      -- \
    java -cp $JARNAME dev.posco.hiona.jobs.FirstRateDataApp \
      migrate \
      --s3in $INPUT \
      --base_s3 $S3OUT
  )


  echo "started $TASKARN for $INPUT at $(date --utc +"%Y-%m-%d %H:%M:%S")"
}


launch "A_C_jg7hbqh.zip"
launch "D_F_b6fggj8h.zip"
launch "G_N_mfljgoa8.zip"
launch "O_S_hgn8mf7.zip"
launch "T_Z_jf8ns0co.zip"

echo "done launching"
