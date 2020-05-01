#!/bin/sh

set -e

APP="FinnhubBars"
BUCKET="predictionmachine-data"

sam package --template-file template.yaml --output-template-file package.yml --s3-bucket $BUCKET

aws lambda list-functions

sam deploy --template-file package.yml --stack-name hiona-$APP --capabilities CAPABILITY_IAM

aws lambda list-functions

# this is an example of how to run it
# aws lambda invoke --function-name arn:aws:lambda:us-west-2:635347781708:function:hiona-wordcount-function-LWA2FZG7ALE4 out --payload $(cat finnhub_input.json)" --log-type Tail --query 'LogResult' --output text --cli-read-timeout 1000 |  base64 -d

# to run locally
# sam local invoke -e awswordcount_input.json
