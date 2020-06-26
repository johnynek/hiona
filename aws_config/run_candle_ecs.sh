#!/bin/bash

set -eu

# Set DIR to the directory containing the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

JARNAME="hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"

JAR="$DIR/../jobs/target/scala-2.13/$JARNAME"
NOWUTC=$(date --utc +%Y%m%dT%H%M%S)

NUM_ROWS=1000
EXCH_CODE="US"  # "HK", "T"

CANDLE_SIZE=5  # 1, 5, 60
#
# "--inclusive_lower_ms"
# "--exclusive_lower_ms"
# https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch
# import pandas as pd
# stamps = pd.to_datetime(["2020-01-01", "2020-03-01"])
# (stamps - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s') * 1000
# "--inclusive_lower_ms", "",
DB_VIEW="_biotech"
#OUT_PREFIX="s3://predictionmachine-data/hiona/${EXCH_CODE}${DB_VIEW}"
OUT_PREFIX="s3://predictionmachine-data/temp/oscar_test_${NOWUTC}"

OUTPUT="${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${NUM_ROWS}_turnover.csv"

TASKARN=$(java -cp $JAR dev.posco.hiona.aws.ECSDeployApp \
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
  --limit $NUM_ROWS \
  --output $OUTPUT
)

echo "waiting on $TASKARN"
time aws ecs wait tasks-stopped --tasks $TASKARN --cluster pm-fargate-oregon
echo "wrote: $OUTPUT"
