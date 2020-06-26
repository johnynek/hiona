This directory contains notes and configuration files for deploying to AWS lambda

Steps:

1. make sure you have run `assembly` at the sbt prompt to build the fat jar of all the code.
2. run build_docker.sh in the root to update the docker image
2. edit the aws_config/run_candle_ecs.sh to see an example of launching the job. you will likely have a
   new job name and/or new parameters to the input file.
