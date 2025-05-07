#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/db.hql > output/hive_results.txt
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q1.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q2.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q3.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q4.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q5.hql
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q6.hql