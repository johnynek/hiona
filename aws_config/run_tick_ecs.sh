#!/bin/bash

set -eu

TIME_SPAN_NAME="2020-04"
LOWER_MS=1585699200000
UPPER_MS=1588291200000

#TIME_SPAN_NAME="2020-05"
#LOWER_MS=1588291200000
#UPPER_MS=1590969600000

#TIME_SPAN_NAME="2020-06"
#LOWER_MS=1590969600000
#UPPER_MS=1593561600000

BUCKET="data-pm"

OUT_PREFIX="s3://$BUCKET/hiona/tick_result"

OUTPUT="${OUT_PREFIX}/tick_${TIME_SPAN_NAME}_$(date --utc +%Y%m%dT%H%M%S).csv"

# NUM_ROWS=10000000
# OUTPUT="${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${NUM_ROWS}.csv"

# Set DIR to the directory containing the script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

JARNAME="hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JAR="$DIR/../jobs/target/scala-2.13/$JARNAME"

TASKARN=$(
  java -cp $JAR dev.posco.hiona.aws.ECSDeployApp \
    run \
    --taskdef hiona_job \
    --cluster pm-fargate-oregon \
    --env "$(
      cat <<-EOF
  {"inclusive_lower_ms": "$LOWER_MS",
   "exclusive_upper_ms": "$UPPER_MS" }
EOF
    )" \
    -- \
    java -cp $JARNAME dev.posco.hiona.jobs.FinnhubTicks \
    run \
    --logevery 1h \
    --output $OUTPUT \
    --inclusive_lower_ms $LOWER_MS \
    --exclusive_upper_ms $UPPER_MS
  # --limit $NUM_ROWS
)

echo "will write $OUTPUT"
echo "$(date --utc +"%Y-%m-%d %H:%M:%S") waiting for start of $TASKARN"
# on long jobs, this times out
# see also https://stackoverflow.com/questions/30977532/aws-command-line-interface-aws-ec2-wait-max-attempts-exceeded
time aws ecs wait tasks-running --tasks "$TASKARN" --cluster pm-fargate-oregon
echo "$(date --utc +"%Y-%m-%d %H:%M:%S") task running; waiting for it to stop; consider:"
echo "awslogs get hiona_logs hiona/hiona_image/$(cut -d/ -f3 <<<"$TASKARN") -G --watch"

# This times out after 5 mins of waiting
time aws ecs wait tasks-stopped --tasks "$TASKARN" --cluster pm-fargate-oregon
echo "$(date --utc +"%Y-%m-%d %H:%M:%S") wrote: $OUTPUT"
