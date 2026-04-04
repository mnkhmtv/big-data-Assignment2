#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

echo "yarn status"
yarn node -list 2>/dev/null || echo "YARN not ready yet"

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    --conf spark.yarn.archive=hdfs:///spark-jars.zip \
    --conf spark.yarn.am.memory=256m \
    --conf spark.yarn.am.memoryOverhead=256m \
    --conf spark.executor.memory=512m \
    --conf spark.executor.memoryOverhead=256m \
    --conf spark.driver.memory=512m \
    --num-executors 1 \
    --executor-cores 1 \
    query.py "$@"