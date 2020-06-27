This directory contains config files for deploying to AWS ECS

Steps:

1. at the `sbt` prompt, run `assembly` to build the fat jar of all the code.
2. run `./build_docker.sh` in the hiona root to update the docker image and upload it to AWS ECR
3. edit and run `./aws_config/run_candle_ecs.sh` to launch a job. you will likely have a
   new job name and/or new parameters to the input file.
4. also see [Hiona Colab Notebook](https://colab.research.google.com/drive/104WOl-31bBKMMvwRV7bI8HFV9Gg5wXS0#scrollTo=XKJHF-D6QPRH) for configuring runs, converting dates into start/stop timestamps, etc
