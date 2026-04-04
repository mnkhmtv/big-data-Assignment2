#!/bin/bash
# Start ssh server
service ssh restart

# Starting the services
bash start-services.sh

if ! hdfs dfs -test -e /spark-jars.zip 2>/dev/null; then
    echo "Uploading Spark jars to HDFS"
    cd /usr/local/spark/jars && zip -r /tmp/spark-jars.zip . && cd /app
    hdfs dfs -put /tmp/spark-jars.zip /spark-jars.zip
    echo "Spark jars uploaded"
fi

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz --force

if hdfs dfs -test -d /input/data 2>/dev/null; then
    echo "Data already in HDFS, skipping prepare_data"
else
    bash prepare_data.sh
fi

# Run the indexer
bash index.sh

echo "Waiting for YARN to release mapreduce resources"
sleep 30

# Run the ranker
bash search.sh "machine learning"
bash search.sh "history of science"