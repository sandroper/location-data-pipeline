#!/bin/bash

# Spark Submit Script for snowflake-spark-transform.py
# This script submits the Snowflake transformation job to the Spark standalone cluster

# Get the absolute path of the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_FILE="$PROJECT_ROOT/src/spark_location_pipeline_ps.py"

# Allow specifying environment-specific .env file via ENV_FILE variable
# Example: ENV_FILE=.env.prod ./spark-submit-snowflake.sh
# Defaults to .env if not specified
ENV_FILE_NAME="${ENV_FILE:-.env-prod}"
# Look for .env in project root first, then in script directory
ENV_FILE="$PROJECT_ROOT/$ENV_FILE_NAME"
if [ ! -f "$ENV_FILE" ]; then
    ENV_FILE="$SCRIPT_DIR/$ENV_FILE_NAME"
fi

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
    echo "  SNOWFLAKE_URL=your_url"
    echo "  SNOWFLAKE_ACCOUNT=your_account"
    echo "  SNOWFLAKE_USER=your_username"
    echo "  SNOWFLAKE_DATABASE=your_database"
    echo "  SNOWFLAKE_SCHEMA=your_schema"
    echo "  SNOWFLAKE_WAREHOUSE=your_warehouse"
    echo "  SNOWFLAKE_TABLE=your_table"
    exit 1
fi

# Check for private key file (validation only - Python code will do the actual check)
# This is just a pre-flight check to give a helpful error early
if [ -n "$SNOWFLAKE_KEY_PATH" ]; then
    KEY_FILE_PATH="$SNOWFLAKE_KEY_PATH"
    # If relative path, try resolving from project root
    if [[ "$KEY_FILE_PATH" != /* ]]; then
        # Try project root first
        if [ -f "$PROJECT_ROOT/$KEY_FILE_PATH" ]; then
            KEY_FILE_PATH="$PROJECT_ROOT/$KEY_FILE_PATH"
        elif [ -f "$SCRIPT_DIR/$KEY_FILE_PATH" ]; then
            KEY_FILE_PATH="$SCRIPT_DIR/$KEY_FILE_PATH"
        fi
    fi
else
    # Default location
    KEY_FILE_PATH="$SCRIPT_DIR/sf_k2ib_key.p8"
    if [ ! -f "$KEY_FILE_PATH" ]; then
        KEY_FILE_PATH="$PROJECT_ROOT/sf_k2ib_key.p8"
    fi
fi

# Note: We don't fail here if the file doesn't exist, as the Python code
# will handle path resolution more intelligently (checking multiple locations)
# This is just a warning
if [ ! -f "$KEY_FILE_PATH" ]; then
    echo "Warning: Snowflake private key file not found at: $KEY_FILE_PATH"
    echo "The Python code will attempt to locate it using SNOWFLAKE_KEY_PATH from .env"
fi

# Submit the job to the Spark standalone cluster
# The master URL connects to localhost:7077 (exposed by docker-compose)
# The Snowflake connector JARs will be downloaded automatically
spark-submit \
    --master spark://10.239.23.4:7077 \
    --deploy-mode client \
    --name "snowflake-transform" \
    --packages "net.snowflake:snowflake-jdbc:3.14.0,net.snowflake:spark-snowflake_2.13:3.1.6" \
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
    "$PYTHON_FILE"

