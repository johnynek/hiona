#!/bin/bash

set -eua

REPO="hiona_img"
TAG="latest"
REGION="us-west-2"
AWSID="131579175100"

#TODO make sure the timestamp on this jar >= the timestamp on any source file

cp jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar ./

function create {
  aws ecr create-repository --repository-name $REPO
}

function login {
  aws ecr get-login-password --region $REGION | docker login \
    --username AWS \
    --password-stdin \
    $AWSID.dkr.ecr.$REGION.amazonaws.com
}


#create
login
IMAGE=$(docker build . | grep "Successfully built" | tail -n1 | sed -e 's/Successfully built //')
echo "built: $IMAGE"
docker tag $IMAGE $AWSID.dkr.ecr.$REGION.amazonaws.com/$REPO:$TAG
docker push $AWSID.dkr.ecr.$REGION.amazonaws.com/$REPO:$TAG

# to register:
# aws ecs register-task-definition --cli-input-json file://aws_config/hiona_task_def.json
# to run:
# aws ecs run-task --cli-input-json file://aws_config/hiona_run_ecs.json
