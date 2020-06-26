FROM openjdk:8-jdk-alpine

WORKDIR /usr/src/app

COPY hiona-jobs-assembly-0.1.0-SNAPSHOT.jar .

CMD ["java", "-cp", "hiona-jobs-assembly-0.1.0-SNAPSHOT.jar", "dev.posco.hiona.aws.LambdaDeployApp"]
