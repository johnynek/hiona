#!/bin/sh

set -e

sam package --template-file template.yaml --output-template-file package.yml --s3-bucket dev-posco-0

aws lambda list-functions

sam deploy --template-file package.yml --stack-name hiona-wordcount --capabilities CAPABILITY_IAM

aws lambda list-functions

# this is an example of how to run it
# aws lambda invoke --function-name arn:aws:lambda:us-west-2:635347781708:function:hiona-wordcount-function-LWA2FZG7ALE4 out --payload "$(cat awswordcount_input.json)" --log-type Tail --query 'LogResult' --output text |  base64 -d

# to run locally
# sam local invoke -e awswordcount_input.json
