#!/bin/bash

INPUT_PATH=${1:-"/input/data"}
STREAMING_JAR=$(find /usr/local/hadoop -name "hadoop-streaming*.jar" | head -1)

echo "Input: $INPUT_PATH"
echo "Streaming jar: $STREAMING_JAR"

hdfs dfs -rm -r -f /tmp/indexer
hdfs dfs -rm -r -f /indexer
hdfs dfs -mkdir -p /indexer

echo "PIPELNE 1 | COMPUTE TF"
hadoop jar $STREAMING_JAR \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/indexer/tf
echo "DONE PIPELINE 1"

echo "PIPELINE 2 | INVERTED INDES"
hadoop jar $STREAMING_JAR \
    -files mapreduce/mapper2.py,mapreduce/reducer2.py \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input /tmp/indexer/tf \
    -output /indexer/index
echo "DONE PIPELINE 2"

echo "PIPELINE 3 | CORPUS STATA."
hadoop jar $STREAMING_JAR \
    -files mapreduce/mapper3.py,mapreduce/reducer3.py \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input "$INPUT_PATH" \
    -output /indexer/stats \
    -numReduceTasks 1
echo "DONE PIPELINE 1"

hdfs dfs -ls /indexer