#!/bin/sh

set -e

JAR="../jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar"
JOB="dev.posco.hiona.jobs.ExampleDBLambdaJob"

JSON=$(cat <<EOF
{
  "body": {
    "args":
      [
        "run", "--output", "s3://predictionmachine-data/temp/example_db_out_lambda.csv",
        "--logevery", "24h", "--limit", "1000"
      ]
    }
}
EOF
)

java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp $JAR dev.posco.hiona.aws.LambdaDeployApp \
  invoke_remote \
  --jar $JAR \
  --cas_root s3://predictionmachine-data/cas/ \
  --method $JOB \
  --payload "$JSON" \
  --async \
  --role arn:aws:iam::131579175100:role/hiona-FinnhubBars-functionRole-1DGE5DL8HGZRQ \
  --subnet subnet-108a2467 \
  --sec_group sg-2defd948
