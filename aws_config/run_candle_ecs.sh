#!/bin/bash

set -eu

# Hiona colab nb: https://colab.research.google.com/drive/104WOl-31bBKMMvwRV7bI8HFV9Gg5wXS0
# TODO: this can be a python script, running month-sized chunks in parallel

CANDLE_SIZE=5  # 1, 5, 60
EXCH_CODE="US" # "HK", "T"
DB_VIEW="_biotech"

# NUM_ROWS=10000000

TIME_SPAN="2020-04"
LOWER_MS=1585699200000
UPPER_MS=1588291200000

#TIME_SPAN="2020-05"
#LOWER_MS=1588291200000
#UPPER_MS=1590969600000

#TIME_SPAN="2020-06"
#LOWER_MS=1590969600000
#UPPER_MS=1593561600000

OUT_PREFIX="s3://predictionmachine-data/hiona/${EXCH_CODE}${DB_VIEW}"
# OUT_PREFIX="s3://predictionmachine-data/temp/test_$(date --utc +%Y%m%dT%H%M%S)"
# OUTPUT="${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${NUM_ROWS}_turnover.csv"
OUTPUT="${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${TIME_SPAN}.csv"


# Set DIR to the directory containing the script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

JARNAME="hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JAR="$DIR/../jobs/target/scala-2.13/$JARNAME"

TASKARN=$(
  java -cp $JAR dev.posco.hiona.aws.ECSDeployApp \
    run \
    --taskdef hiona_job:3 \
    --cluster pm-fargate-oregon \
    --env "$(
      cat <<-EOF
  {"db_candles_view": "finnhub.stock_candles_${CANDLE_SIZE}min",
   "db_symbols_view": "finnhub.stock_symbols${DB_VIEW}_view",
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

echo "waiting on $TASKARN"
time aws ecs wait tasks-stopped --tasks $TASKARN --cluster pm-fargate-oregon
echo "wrote: $OUTPUT"
