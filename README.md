# Hiona is a feature engineering system to correctly convert events into feature vectors for ML

It is a reactive system, akin to Kafka streams, but can be run more simply -- in a single docker container or lambda.
for more details on Event-Driven Functional Reactive Progamming, see eg "[Programming Paradigms for Dummies: What Every Programmer Should Know](https://www.info.ucl.ac.be/~pvr/VanRoyChapter.pdf)" by Peter Van Roy, the section on "Discrete synchronous programming" and surrounding context.

> Using discrete time enormously simplifies programming for reactive systems. For example, it means that subprograms can be trivially composed: output events from one subcomponent are instantaneously available as input events in other subcomponents.

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

To format the code run `scalafmtAll` at the `sbt` prompt.

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

## Running the benchmarks

At the sbt prompt:
```
bench/jmh:run -i 3 -wi 3 -f1 -t1 .*Bench.*
```
or use any regular expression to select the benchmark class you want to run. Change the parameters
higher numbers for more reliable output, but longer running benchmarks.

## IntelliJ

IntelliJ 2020.1 Beta (201.6668.13) (on Mac catalina)
appears to correcly "create new project" for hiona (from github)
ONLY after `brew uninstall sbt` -- brew's `sbt` seems to
interfere with the proper import process, which results in eg `build.sbt`
file being all red. 

## Python
We're targeting python 3.6 for simple scripting, without external libs.
On a mac, follow instructions at [homebrew-python github](https://github.com/sashkab/homebrew-python) to install:
`brew install sashkab/python/python@3.6`
