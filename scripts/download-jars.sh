#!/bin/bash

# Download required JARs for Spark pipeline
# These JARs are needed for Snowflake and Sedona (geospatial) functionality

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JARS_DIR="$PROJECT_ROOT/jars"

# JAR versions (keep in sync with spark-submit script)
SNOWFLAKE_JDBC_VERSION="3.14.0"
SPARK_SNOWFLAKE_VERSION="3.1.6"
SEDONA_VERSION="1.8.1"
GEOTOOLS_VERSION="1.8.1-33.1"

# Maven Central base URL
MAVEN_URL="https://repo1.maven.org/maven2"

mkdir -p "$JARS_DIR"

echo "Downloading JARs to $JARS_DIR..."

download_jar() {
    local name="$1"
    local url="$2"
    local filename=$(basename "$url")

    if [ -f "$JARS_DIR/$filename" ]; then
        echo "  [skip] $name (already exists)"
    else
        echo "  [download] $name..."
        curl -sL -o "$JARS_DIR/$filename" "$url"
    fi
}

# Snowflake JARs
download_jar "snowflake-jdbc" \
    "$MAVEN_URL/net/snowflake/snowflake-jdbc/$SNOWFLAKE_JDBC_VERSION/snowflake-jdbc-$SNOWFLAKE_JDBC_VERSION.jar"

download_jar "spark-snowflake" \
    "$MAVEN_URL/net/snowflake/spark-snowflake_2.13/$SPARK_SNOWFLAKE_VERSION/spark-snowflake_2.13-$SPARK_SNOWFLAKE_VERSION.jar"

# Sedona JARs (geospatial)
download_jar "sedona-spark-shaded" \
    "$MAVEN_URL/org/apache/sedona/sedona-spark-shaded-4.0_2.13/$SEDONA_VERSION/sedona-spark-shaded-4.0_2.13-$SEDONA_VERSION.jar"

download_jar "geotools-wrapper" \
    "$MAVEN_URL/org/datasyslab/geotools-wrapper/$GEOTOOLS_VERSION/geotools-wrapper-$GEOTOOLS_VERSION.jar"

echo ""
echo "Done! JARs downloaded to: $JARS_DIR"
ls -lh "$JARS_DIR"
echo ""
echo "To use these JARs, the spark-submit script will automatically detect them."
