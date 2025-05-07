#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/db.hql > output/hive_results.txt
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q1.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q2.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q3.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q4.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q5.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q6.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q7.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q8.hql

hdfs dfs -cat project/output/q1/* >> output/q1.csv
hdfs dfs -cat project/output/q2/* >> output/q2.csv
hdfs dfs -cat project/output/q3/* >> output/q3.csv
hdfs dfs -cat project/output/q4/* >> output/q4.csv
hdfs dfs -cat project/output/q5/* >> output/q5.csv
hdfs dfs -cat project/output/q6/* >> output/q6.csv
hdfs dfs -cat project/output/q5/* >> output/q7.csv
hdfs dfs -cat project/output/q6/* >> output/q8.csv