#!/bin/bash


source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)



unset PYSPARK_PYTHON

# DOWNLOAD a.parquet before running this

if [ -f "a.parquet" ]; then
    hdfs dfs -put -f a.parquet / || { echo "[ERROR] Failed to upload a.parquet"; exit 1; }
else
    echo "[INFO] a.parquet not found"
fi
spark-submit prepare_data.py || { echo "[ERROR] prepare_data.py failed"; exit 1; }

hdfs dfs -ls /data

hdfs dfs -ls /input/data

echo "DONE DATA PREPARATION"