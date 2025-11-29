#!/bin/bash

# Spark Submit Script for pandas-pyspark-dataframe.py
# This script submits the job to the Spark standalone cluster

# Get the absolute path of the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_FILE="$SCRIPT_DIR/pandas-pyspark-dataframe.py"

# Check if the Python file exists
if [ ! -f "$PYTHON_FILE" ]; then
    echo "Error: $PYTHON_FILE not found"
    exit 1
fi

# Check if spark-submit is available
if ! command -v spark-submit &> /dev/null; then
    echo "Error: spark-submit not found. Please ensure Spark is installed and in your PATH."
    exit 1
fi

# Submit the job to the Spark standalone cluster
# The master URL connects to localhost:7077 (exposed by docker-compose)
spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --name "pandas-pyspark-dataframe" \
    --conf "spark.sql.execution.arrow.enabled=true" \
    --conf "spark.sql.execution.arrow.pyspark.fallback.enabled=true" \
    "$PYTHON_FILE"

