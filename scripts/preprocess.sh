#!/bin/bash

# Prepare virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
  echo "Creating virtual environment"
  python3.6 -m venv .venv
  source .venv/bin/activate
fi

python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt -qqq
MAKEFLAGS="-j1" HOROVOD_WITHOUT_GLOO=1 HOROVOD_WITH_TENSORFLOW=1 pip install horovod[tensorflow,spark]==0.22.1

# Create 'data' directory
if [ ! -d "data" ]; then
  mkdir data
fi

# Create 'secrets' directory
if [ ! -d "secrets" ]; then
  mkdir secrets
fi

# Check all secret files with passwords are presented
password_files=(".psql.pass" ".hive.pass")
for file in $password_files; do
  if [ ! -f "secrets/$file" ]; then
    echo "WARN: not found $file in 'secrets' directory!
     Please add secret file or enter password manually."
  fi
done
