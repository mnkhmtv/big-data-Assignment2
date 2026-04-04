#!/bin/bash

INPUT_PATH=${1:-"/input/data"}

echo "++++ creating index ++++"
bash create_index.sh "$INPUT_PATH"

echo "++++ storing to cassandra ++++"
bash store_index.sh
