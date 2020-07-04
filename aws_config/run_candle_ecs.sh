#!/bin/bash

set -eu
# Hiona colab nb: https://colab.research.google.com/drive/104WOl-31bBKMMvwRV7bI8HFV9Gg5wXS0

CANDLE_SIZE=5  # 1, 5, 60
# EXCH_CODE="HK"
# SYMS_GROUP=""
EXCH_CODE="US" # "HK", "T"
SYMS_GROUP="_biotech"

TIME_SPAN_NAME="2020-04"
LOWER_MS=1585699200000
UPPER_MS=1588291200000

#TIME_SPAN_NAME="2020-05"
#LOWER_MS=1588291200000
#UPPER_MS=1590969600000

#TIME_SPAN_NAME="2020-06"
#LOWER_MS=1590969600000
#UPPER_MS=1593561600000


# https://medium.com/@Drew_Stokes/bash-argument-parsing-54f3b81a6a8f
while (( "$#" )); do
  case "$1" in
#    -a|--my-boolean-flag)
#      MY_FLAG=0
#      shift
#      ;;
    -c|--candle-size)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        CANDLE_SIZE=$2
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -e|--exchange)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        EXCH_CODE=$2
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -s|--syms-group)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        SYMS_GROUP=$2
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -*) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # unsupported arguments
      echo "Error: Unsupported argument $1" >&2
      exit 1
      ;;
  esac
done


OUT_PREFIX="s3://predictionmachine-data/hiona/${EXCH_CODE}${SYMS_GROUP}"
# OUT_PREFIX="s3://predictionmachine-data/temp/test_$(date --utc +%Y%m%dT%H%M%S)"

OUTPUT="${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${TIME_SPAN_NAME}_$(date --utc +%Y%m%dT%H%M%S).csv"

# NUM_ROWS=10000000
# OUTPUT="${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${NUM_ROWS}.csv"


# Set DIR to the directory containing the script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

JARNAME="hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JAR="$DIR/../jobs/target/scala-2.13/$JARNAME"

TASKARN=$(
  java -cp $JAR dev.posco.hiona.aws.ECSDeployApp \
    run \
    --taskdef hiona_job:4 \
    --cluster pm-fargate-oregon \
    --env "$(
      cat <<-EOF
  {"db_candles_view": "finnhub.stock_candles_${CANDLE_SIZE}min",
   "db_symbols_view": "finnhub.stock_symbols${SYMS_GROUP}_view",
   "db_exchange": "${EXCH_CODE}"}
EOF
    )" \
    -- \
    java -cp $JARNAME dev.posco.hiona.jobs.FinnhubDBCandle \
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
