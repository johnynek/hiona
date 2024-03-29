#!/bin/bash

# Usage: eg
# aws_config/run_candle_lambda.sh
# aws_config/run_candle_lambda.sh --async

# -u exits on ref on undef var
set -eu

# Set DIR to the directory containing the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

JAR="$DIR/../jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JOB="dev.posco.hiona.jobs.FinnhubDBCandleLambda"

NUM_ROWS=1000000
EXCH_CODE="US"  # "HK", "T"  Also update FinnhubDBCandle:18
# val exch_code: ExchangeCode = "HK" // "HK", "T" and re-run assembly

CANDLE_SIZE=5  # 1, 5, 60 Also update FinnhubDBCandle:42
# DB_VIEW=finnhub.stock_candles_${CANDLE_SIZE}min  #  and re-run assembly
#
# "--inclusive_lower_ms"
# "--exclusive_lower_ms"
# https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch
# import pandas as pd
# stamps = pd.to_datetime(["2020-01-01", "2020-03-01"])
# (stamps - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s') * 1000
# "--inclusive_lower_ms", "",
DB_VIEW="_biotech"
OUT_PREFIX="s3://predictionmachine-data/hiona/${EXCH_CODE}${DB_VIEW}"

# echo '{"db_sql_view": "finnhub.stock_symbols'${DB_VIEW}'_view", "db_exchange": "'${EXCH_CODE}'"}'
# --env '{"db_sql_view": "finnhub.stock_symbols'${DB_VIEW}'_view", "db_exchange": "'${EXCH_CODE}'"}' \

# see https://github.com/predictionmachine/hiona/pull/78 for --env and --env_path
# shellcheck disable=SC2086
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp "$JAR" dev.posco.hiona.aws.LambdaDeployApp \
  invoke_remote \
  --jar "$JAR" \
  --cas_root s3://predictionmachine-data/cas/ \
  --method $JOB \
  --env "$(
  cat <<-EOF
  {"db_candles_view": "finnhub.stock_candles_${CANDLE_SIZE}min",
   "db_symbols_view": "finnhub.stock_symbols${DB_VIEW}_view",
   "db_exchange": "${EXCH_CODE}"}
EOF
  )" \
  --payload "$(
    cat <<-EOF
      {
        "body": {
          "args":
            [
              "run",
              "--logevery", "12h",
              "--limit", "$NUM_ROWS",
              "--output", "${OUT_PREFIX}/${EXCH_CODE}_candle${CANDLE_SIZE}_${NUM_ROWS}_turnover.csv"
            ]
          }
      }
EOF
  )" \
  --role arn:aws:iam::131579175100:role/hiona-FinnhubBars-functionRole-1DGE5DL8HGZRQ \
  --subnet subnet-108a2467 \
  --sec_group sg-2defd948 \
  "${@:2}"
