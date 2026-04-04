#!/bin/bash

echo "starting storing index to cassandra."
source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON
python3 app.py
echo "index stored"