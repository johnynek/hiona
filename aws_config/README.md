This directory contains notes and configuration files for deploying to AWS lambda

Steps:

1. make sure you have run `assembly` at the sbt prompt to build the fat jar of all the code.
2. use `aws lambda list-functions` to see which lambdas are deployed.
3. you need `sam` installed in order to invoke functions currently. I installed this using pip inside a virtual env: `pip install aws-sam-cli`
4. edit the deploy.sh file in this directory and see if it matches what you want to deploy, the APP name should be changed potentially if you want to deploy a new app. To change which class is invoked see the `Handler:` line in the template.yaml file. If you make changes there, you need to redeploy
5. after you have deployed, invoke the function using sam, see the comment line at the end of deploy.sh for an example invocation. The invocation needs the name of the function, which you can see from `list-functions` or from the stdout of the deploy step
