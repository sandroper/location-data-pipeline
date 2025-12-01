# -*- coding: utf-8 -*-
"""
Spark job to connect to Snowflake, retrieve data, and transform SRC column to lowercase
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from snowflake_key_manager import get_snowflake_key_options

# Load environment variables from .env file
# Look for .env in the script's directory
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded environment variables from {env_path}")
else:
    # Fallback: try to load from current directory
    load_dotenv()

# Create SparkSession
# When using spark-submit, the master is specified in the command
# The Snowflake connector JARs will be downloaded automatically via Maven
spark = SparkSession.builder \
    .appName("snowflake-transform") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.14.0,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5") \
    .getOrCreate()

# Snowflake connection options
# These are loaded from .env file or environment variables
# See .env.example for the required variables
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "your_account")
# Construct full URL if not provided
snowflake_url = os.getenv("SNOWFLAKE_URL", "Your URL")

# Get private key authentication options from key manager
key_options = get_snowflake_key_options()

# Build Snowflake connection options
snowflake_options = {
    "sfURL": snowflake_url,
    "sfUser": os.getenv("SNOWFLAKE_USER", "your_username"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "your_database"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA", "your_schema"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "your_warehouse"),
    "sfTable": os.getenv("SNOWFLAKE_TABLE", "your_table"),
}

# Add private key authentication options
snowflake_options.update(key_options)

# Optional role
if os.getenv("SNOWFLAKE_ROLE"):
    snowflake_options["sfRole"] = os.getenv("SNOWFLAKE_ROLE")

# Table name - can be set via environment variable
table_name = os.getenv("SNOWFLAKE_TABLE", "your_table")

print(f"Connecting to Snowflake table: {snowflake_options['sfDatabase']}.{snowflake_options['sfSchema']}.{table_name}")

query = f"""
    SELECT * 
    FROM {snowflake_options['sfTable']}
    LIMIT 50
"""

df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("query", query) \
    .load()

print("Data retrieved from Snowflake:")
print(f"Number of rows: {df.count()}")
df.printSchema()
df.show(truncate=False)

# Transform SRC column to lowercase
# This transformation will be distributed across Spark workers
if "SRC" in df.columns:
    print("\nTransforming SRC column to lowercase...")
    df_transformed = df.withColumn("SRC", lower(col("SRC")))
    
    print("\nTransformed data:")
    df_transformed.show(truncate=False)
    
    print("\nSample of transformed SRC column:")
    df_transformed.select("SRC").show(truncate=False)
else:
    print(f"\nWarning: Column 'SRC' not found in the table.")
    print(f"Available columns: {', '.join(df.columns)}")
    df.show(truncate=False)

# Stop Spark session
spark.stop()

