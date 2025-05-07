#!/bin/bash

# Prepare virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
  echo "Creating virtual environment"
  python3.6 -m venv .venv
fi

source .venv/bin/activate
python3.6 -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
