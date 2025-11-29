#!/bin/bash

# Spark Submit Script for snowflake-spark-transform.py
# This script submits the Snowflake transformation job to the Spark standalone cluster

# Get the absolute path of the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_FILE="$SCRIPT_DIR/snowflake-spark-transform.py"

# Allow specifying environment-specific .env file via ENV_FILE variable
# Example: ENV_FILE=.env.prod ./spark-submit-snowflake.sh
# Defaults to .env if not specified
ENV_FILE_NAME="${ENV_FILE:-.env}"
ENV_FILE="$SCRIPT_DIR/$ENV_FILE_NAME"

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

# Load environment variables from .env file if it exists
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment variables from $ENV_FILE"
    # Export variables from .env file, ignoring comments and empty lines
    set -a
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip comments and empty lines
        if [[ ! "$line" =~ ^[[:space:]]*# ]] && [ -n "$line" ]; then
            # Export the variable
            export "$line"
        fi
    done < "$ENV_FILE"
    set +a
else
    echo "Warning: .env file not found at $ENV_FILE"
    echo "Will use environment variables from the current shell"
fi

# Check for required environment variables
required_vars=("SNOWFLAKE_ACCOUNT" "SNOWFLAKE_USER" "SNOWFLAKE_DATABASE" "SNOWFLAKE_SCHEMA" "SNOWFLAKE_WAREHOUSE" "SNOWFLAKE_TABLE")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "Error: The following required environment variables are not set:"
    printf '  - %s\n' "${missing_vars[@]}"
    echo ""
    echo "Please set them in the .env file or as environment variables, for example:"
    echo "  SNOWFLAKE_ACCOUNT=your_account"
    echo "  SNOWFLAKE_USER=your_username"
    echo "  SNOWFLAKE_DATABASE=your_database"
    echo "  SNOWFLAKE_SCHEMA=your_schema"
    echo "  SNOWFLAKE_WAREHOUSE=your_warehouse"
    echo "  SNOWFLAKE_TABLE=your_table"
    exit 1
fi

# Check for private key file
KEY_FILE_PATH="${SNOWFLAKE_KEY_PATH:-$SCRIPT_DIR/sf_k2ib_key.p8}"
if [ ! -f "$KEY_FILE_PATH" ]; then
    echo "Error: Snowflake private key file not found: $KEY_FILE_PATH"
    echo "Please ensure the key file exists or set SNOWFLAKE_KEY_PATH to the correct path"
    exit 1
fi

# Submit the job to the Spark standalone cluster
# The master URL connects to localhost:7077 (exposed by docker-compose)
# The Snowflake connector JARs will be downloaded automatically
spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --name "snowflake-transform" \
    --packages "net.snowflake:snowflake-jdbc:3.14.0,net.snowflake:spark-snowflake_2.12:3.1.5" \
    --conf "spark.sql.execution.arrow.enabled=true" \
    "$PYTHON_FILE"

