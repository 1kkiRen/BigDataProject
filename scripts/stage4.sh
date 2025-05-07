#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -e "
USE team29_projectdb;

DROP TABLE IF EXISTS model_svc_predictions;
DROP TABLE IF EXISTS model_rf_predictions;
DROP TABLE IF EXISTS model_nb_predictions;
DROP TABLE IF EXISTS model_mlp_predictions;
DROP TABLE IF EXISTS model_lr_predictions;
DROP TABLE IF EXISTS model_evaluation;
DROP TABLE IF EXISTS samples;
"

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -e "
USE team29_projectdb;

CREATE EXTERNAL TABLE IF NOT EXISTS model_svc_predictions (
  label INT,
  prediction INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/model_svc_predictions';

CREATE EXTERNAL TABLE IF NOT EXISTS model_rf_predictions (
  label INT,
  prediction INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/model_rf_predictions';

CREATE EXTERNAL TABLE IF NOT EXISTS model_nb_predictions (
  label INT,
  prediction INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/model_nb_predictions';

CREATE EXTERNAL TABLE IF NOT EXISTS model_mlp_predictions (
  label INT,
  prediction INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/model_mlp_predictions';

CREATE EXTERNAL TABLE IF NOT EXISTS model_lr_predictions (
  label INT,
  prediction INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/model_lr_predictions';
"

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -e "
USE team29_projectdb;
CREATE EXTERNAL TABLE IF NOT EXISTS model_evaluation (
  model STRING,
  accuracy DOUBLE,
  f1 DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/evaluation'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'serialization.format'= ',',
  'field.delim' = ','
);
"

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -e "
USE team29_projectdb;
CREATE EXTERNAL TABLE IF NOT EXISTS samples (
    feature_0 DOUBLE,
    feature_1 DOUBLE,
    feature_2 DOUBLE,
    feature_3 DOUBLE,
    feature_4 DOUBLE,
    feature_5 DOUBLE,
    feature_6 DOUBLE,
    feature_7 DOUBLE,
    feature_8 DOUBLE,
    feature_9 DOUBLE,
    feature_10 DOUBLE,
    feature_11 DOUBLE,
    feature_12 DOUBLE,
    feature_13 DOUBLE,
    feature_14 DOUBLE,
    feature_15 DOUBLE,
    feature_16 DOUBLE,
    feature_17 DOUBLE,
    label INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team29/project/output/samples/';
"
