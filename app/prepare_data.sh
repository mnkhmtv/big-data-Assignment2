#!/bin/bash


source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)



unset PYSPARK_PYTHON

# DOWNLOAD a.parquet before running this

hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /input/data && \
    echo "done data preparation!"