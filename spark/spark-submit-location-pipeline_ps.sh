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
ENV_FILE_NAME="${ENV_FILE:-.env}"
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

# Configure Python environment for PySpark
LOCAL_VENV="$PROJECT_ROOT/.venv/bin/python"
VENV_ARCHIVE="$PROJECT_ROOT/pyspark_venv.tar.gz"

if [ ! -f "$LOCAL_VENV" ]; then
    echo "ERROR: Local venv not found at $LOCAL_VENV"
    echo "Run 'uv sync' (local) or './scripts/remote-setup.sh' (remote) to create it"
    exit 1
fi

export PYSPARK_DRIVER_PYTHON="$LOCAL_VENV"

# Determine if we're connecting to local Docker or remote cluster
# For local Docker (127.0.0.1), use system Python installed in Docker image
# For remote clusters, use packed venv if available
if [ "$SPARK_MASTER_HOST" = "127.0.0.1" ] || [ "$SPARK_MASTER_HOST" = "localhost" ]; then
    echo "Local Docker mode: using system Python on executors"
    ARCHIVES_OPT=""
    export PYSPARK_PYTHON="python3"
elif [ -f "$VENV_ARCHIVE" ]; then
    echo "Remote mode: using packed virtual environment: $VENV_ARCHIVE"
    ARCHIVES_OPT="--archives ${VENV_ARCHIVE}#venv"
    export PYSPARK_PYTHON="./venv/bin/python"
else
    echo "Remote mode: no packed venv found, using system Python on executors"
    ARCHIVES_OPT=""
    # Prefer python3.14 if available (for remote clusters)
    if command -v python3.14 &> /dev/null; then
        export PYSPARK_PYTHON="python3.14"
    else
        export PYSPARK_PYTHON="python3"
    fi
fi

# Spark master host - defaults to 127.0.0.1 if not set
SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-127.0.0.1}"

# Check if local JARs exist (more reliable for remote clusters)
JARS_DIR="$PROJECT_ROOT/jars"
if [ -d "$JARS_DIR" ] && [ "$(ls -A $JARS_DIR/*.jar 2>/dev/null)" ]; then
    echo "Using local JARs from: $JARS_DIR"
    JAR_FILES=$(ls -1 "$JARS_DIR"/*.jar | tr '\n' ',' | sed 's/,$//')
    JARS_OPT="--jars $JAR_FILES"
    PACKAGES_OPT=""
else
    echo "No local JARs found, using --packages (requires internet)"
    JARS_OPT=""
    PACKAGES_OPT='--packages "net.snowflake:snowflake-jdbc:3.24.2,net.snowflake:spark-snowflake_2.13:3.1.6,org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1,org.datasyslab:geotools-wrapper:1.8.1-33.1"'
fi

# JVM options for Java 21+ compatibility (required by Snowflake JDBC driver)
# These are harmless on Java 17 and earlier
JAVA_MODULE_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED"

# Submit the job to the Spark standalone cluster
spark-submit \
    --master spark://${SPARK_MASTER_HOST}:7077 \
    --deploy-mode client \
    --name "snowflake-location-pipeline" \
    $JARS_OPT \
    $PACKAGES_OPT \
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator" \
    --conf "spark.driver.extraJavaOptions=$JAVA_MODULE_OPTIONS" \
    --conf "spark.executor.extraJavaOptions=$JAVA_MODULE_OPTIONS" \
    $ARCHIVES_OPT \
    "$PYTHON_FILE"

