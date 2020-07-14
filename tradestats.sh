#!/bin/bash

java -cp jobs/target/scala-2.13/hiona-jobs-assembly-0.1.0-SNAPSHOT.jar dev.posco.hiona.jobs.TradeStats "$@"
