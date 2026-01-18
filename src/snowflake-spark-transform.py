"""
Spark job to connect to Snowflake, retrieve data, and transform SRC column to lowercase
"""

import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
from snowflake_config_manager import SnowflakeConfig
from location_pipeline_config_manager import LocationPipelineConfig

location_pipeline_config = LocationPipelineConfig()
logger.info(f"Preparing locations pipeline with the following settings: {location_pipeline_config}")

snowflake_config = SnowflakeConfig()
snowflake_options = snowflake_config.snowflake_options
logger.info(f"Connecting to Snowflake table: {snowflake_options['sfDatabase']}.{snowflake_options['sfSchema']}.{snowflake_config.table_name}")

query = f"""
    SELECT * 
    FROM {snowflake_config.table_name} 
    LIMIT 50
"""

# Create SparkSession
# When using spark-submit, the master is specified in the command
# The Snowflake connector JARs will be downloaded automatically via Maven
spark = SparkSession.builder \
    .appName("snowflake-transform") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.14.0,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5") \
    .getOrCreate()

df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("query", query) \
    .load()

logger.info("Data retrieved from Snowflake:")
logger.info(f"Number of rows: {df.count()}")
df.printSchema()
df.show(truncate=False)

# Transform SRC column to lowercase
# This transformation will be distributed across Spark workers
if "SRC" in df.columns:
    logger.info("Transforming SRC column to lowercase...")
    df_transformed = df.withColumn("SRC", lower(col("SRC")))

    logger.info("Transformed data:")
    df_transformed.show(truncate=False)

    logger.info("Sample of transformed SRC column:")
    df_transformed.select("SRC").show(truncate=False)
else:
    logger.warning(f"Column 'SRC' not found in the table.")
    logger.info(f"Available columns: {', '.join(df.columns)}")
    df.show(truncate=False)

# Stop Spark session
spark.stop()

