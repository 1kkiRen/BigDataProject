#!/bin/bash

url="https://cdn-lfs-us-1.hf.co/repos/81/40/814032a72da2abaf4da1c05dd283a797230b975585c0da2772bd632ed9182062/ccf67a25b3c66e12afbe9086a04cf3f8f51459c3b4e322c00c70b339ffd848f0?response-content-disposition=inline%3B+filename*%3DUTF-8%27%27training_data.csv%3B+filename%3D%22training_data.csv%22%3B&response-content-type=text%2Fcsv&Expires=1746609341&Policy=eyJTdGF0ZW1lbnQiOlt7IkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iOnsiQVdTOkVwb2NoVGltZSI6MTc0NjYwOTM0MX19LCJSZXNvdXJjZSI6Imh0dHBzOi8vY2RuLWxmcy11cy0xLmhmLmNvL3JlcG9zLzgxLzQwLzgxNDAzMmE3MmRhMmFiYWY0ZGExYzA1ZGQyODNhNzk3MjMwYjk3NTU4NWMwZGEyNzcyYmQ2MzJlZDkxODIwNjIvY2NmNjdhMjViM2M2NmUxMmFmYmU5MDg2YTA0Y2YzZjhmNTE0NTljM2I0ZTMyMmMwMGM3MGIzMzlmZmQ4NDhmMD9yZXNwb25zZS1jb250ZW50LWRpc3Bvc2l0aW9uPSomcmVzcG9uc2UtY29udGVudC10eXBlPSoifV19&Signature=AlvcJDc3iz1y1ABFptBZTD332tlq1bG82bzlcUDYu8lNbDvyOL5Hp-9-MyiFoD3cnnj7lLDwlXmHC5S3zk2J8ZKF3rZ26N30asXsQRWKSMB%7EpG5FTlTCQToxMMYAD4ewdrPaMl%7EVLl-auVgWABxXEAckh4oTXaDVr2DmSbAfX03j05LP7k2cQIyDXUITyN0Y4lQQMzeN9hjkKOXMSKtglQRjR%7E1IuZGyc0gH3YqTh4QkT828-PHBZeCTy2X%7EIxhWJ5ppSuh9VAoW15a83F0LMMe9ffQG8E2%7EI-zJahkWrXPmWL8PfGfiCMrM4n8qCfoP4uJwKjgxepSN23AJfo0q3w__&Key-Pair-Id=K24J24Z295AEI9"

# Download dataset
if [ ! -f "data/training_data.csv" ]; then
  wget -O "data/training_data.csv" "$url"
fi

# Launch stage 1
python scripts/build_database.py

# Send data to HDFS via scoop
#sqoop import-all-tables \
#  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team29_projectdb \
#  --username team29 -P \
#  --compression-codec=snappy \
#  --compress --as-parquetfile \
#  --warehouse-dir=project/warehouse \
#  --m 1

echo "Stage 1 done!"