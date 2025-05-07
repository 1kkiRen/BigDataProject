#!/bin/bash

source .vevn/bin/activate

# Launch stage 1
python scripts/build_database.py

# Create required directory in HDFS
if ! hdfs dfs -test -e project/warehouse ; then
  hdfs dfs -mkdir project/warehouse
fi

password=$(head -n 1 secrets/.psql.pass)

# Send data to HDFS via scoop
sqoop import-all-tables \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team29_projectdb \
  --username team29 \
  --password "$password" \
  --compression-codec=snappy \
  --compress --as-parquetfile \
  --warehouse-dir=project/warehouse \
  --m 1

echo "Stage 1 done!"