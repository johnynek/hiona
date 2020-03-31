# Hiona is a feature engineering system to correctly convert events into feature vectors for ML

## Getting started
Install a jdk and sbt:

e.g. on OSX with homebrew:

```
brew install homebrew/cask/java
brew install sbt
```

To run the tests, start sbt:
```
sbt
```
Which will give you the sbt command prompt. Enter `compile` to compile the code and `test` to run
all the tests.

## Running the example

At the sbt prompt, run `assembly` to build a "fat-jar" a jar with all dependencies in it. You can
then run that using the java command:

```
java -jar target/scala-2.12/hiona-assembly-0.1.0-SNAPSHOT.jar
```
which should print the help message.

The command you want is `run` and you need to pass paths it asks for in `show`. E.g.
```
java -jar target/scala-2.12/hiona-assembly-0.1.0-SNAPSHOT.jar run --input hk-stocks=/home/oscar/Downloads/hk_prices_1h_2y_2017-06-20.csv --output results.csv
```

works on my machine.

To make changes, try changing Example.scala and then reissue the `assembly` command in the sbt
prompt.

## IntelliJ

IntelliJ 2020.1 Beta (201.6668.13) (on Mac catalina)
appears to correcly "create new project" for hiona (from github)
ONLY after `brew uninstall sbt` -- brew's `sbt` seems to
interfere with the proper import process, which results in eg `build.sbt`
file being all red. 
