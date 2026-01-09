"""
Spark job to connect to Snowflake, retrieve data, and run the location pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from data_cleanser import DataCleanser
from snowflake_config_manager import SnowflakeConfig
from location_pipeline_config_manager import LocationPipelineConfig

location_pipeline_config = LocationPipelineConfig() 
print(f"Preparing locations pipeline with the following settings: {location_pipeline_config}")

snowflake_config: dict = SnowflakeConfig()
snowflake_options = snowflake_config.snowflake_options
print(f"Connecting to Snowflake table: {snowflake_options["sfDatabase"]}.{snowflake_options['sfSchema']}.{snowflake_config.table_name}")

query = f"""
    SELECT * 
    FROM {snowflake_config.table_name} 
"""

query_all_device_ids = f"""
    SELECT distinct(id)
    FROM {snowflake_config.table_name} 
    LIMIT 10
"""

# Create SparkSession
# When using spark-submit, the master is specified in the command
# The Snowflake connector JARs will be downloaded automatically via Maven
spark = SparkSession.builder \
    .appName("snowflake-transform") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.14.0,net.snowflake:spark-snowflake_2.12:2.15.0-spark_3.5") \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

device_id_df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("query", query_all_device_ids) \
    .load()

device_ids = [row.ID for row in device_id_df.select("ID").collect()]

df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("query", query) \
    .load() \
    .filter(col("ID").isin(device_ids))


print("Before cleaning: ")
print(df.show())

data_cleanser = DataCleanser(location_pipeline_config)
cleansed_df = spark.createDataFrame(data_cleanser.cleanse(df))

print("After cleaning: ")
print(cleansed_df.show())