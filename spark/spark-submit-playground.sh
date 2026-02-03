#!/bin/bash

# Spark Submit Script for spark-submit-playground.py

# Get the absolute path of the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_FILE="$PROJECT_ROOT/src/spark_submit_playground.py"

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
# The Snowflake connector JARs will be downloaded automatically
spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --name "spark-playground" \
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
    "$PYTHON_FILE"

