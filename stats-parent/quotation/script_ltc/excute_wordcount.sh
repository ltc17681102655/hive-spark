#!/usr/bin/env bash

spark-submit \
--class com.liyou.stats.quotation.WordCount \
--master yarn \
--deploy-mode client \
--num-executors 4 \
--executor-memory 4G \
--executor-cores 2 \
--driver-memory 4G  \
/home/liutongchun/stats_quotation.jar \
hdfs://mycluster/user/liutongchun/word.txt


