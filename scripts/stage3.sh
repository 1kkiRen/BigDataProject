#!/bin/bash
find . -name "*.py" ! -path "./.*" -print | zip stage3.zip -@
spark-submit --py-files stage3.zip --master yarn scripts/model.py
