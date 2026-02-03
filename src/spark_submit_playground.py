"""
Spark job to connect to Snowflake, retrieve data, and run the location pipeline
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import pandas as pd
from utils.location_pipeline_config_manager import LocationPipelineConfig


location_pipeline_config = LocationPipelineConfig()
logger.info(f"Preparing locations pipeline with the following settings: {location_pipeline_config}")

# Create SparkSession
# When using spark-submit, the master is specified in the command
# The Snowflake connector JARs will be downloaded automatically via Maven
spark = SparkSession.builder \
    .appName("spark-playground") \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


logger.info("==================== Starting: ")

df_list: list[int] = [(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd')]
df_schema = StructType([StructField("col1", IntegerType(), True),
                           StructField("col2", StringType(), True)])

pd_df = pd.DataFrame([(101, 'abc'),
                      ('def', 201),
                      ('xyz', 'pqr')],
                     columns=['col1', 'col2'])

pdf_schema = StructType([StructField("col1", StringType(), True)\
                       ,StructField("col2", StringType(), True)])

sdf_qualified = spark.createDataFrame(pd_df, schema=pdf_schema)

logger.info("==================== Final DataFrame: ")
logger.info(sdf_qualified.show())

spark.stop()