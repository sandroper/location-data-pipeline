"""
Spark job to connect to Snowflake, retrieve data, run the location pipeline and save back to Snowflake
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from snowflake_config_manager import SnowflakeConfig
from utils.location_pipeline_config_manager import LocationPipelineConfig

from data_cleanser_ps import DataCleanser
from points_qualifier_ps import PointsQualifier
from cluster_stay_points_ps import ClusterStayPoints
from utils.osrm.osrm_route_predictor import RoutePredictorOSRM

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

location_pipeline_config = LocationPipelineConfig()
logger.info(f"Preparing locations pipeline with the following settings: {location_pipeline_config}")

snowflake_config = SnowflakeConfig()
snowflake_options = snowflake_config.snowflake_options
logger.info(f"Connecting to Snowflake table: {snowflake_options['sfDatabase']}.{snowflake_options['sfSchema']}.{snowflake_config.table_name}")

data_query = f"""
    SELECT * 
    FROM {snowflake_config.table_name} 
"""

# Create SparkSession
# When using spark-submit, the master is specified in the command
# The Snowflake connector JARs will be downloaded automatically via Maven
spark = SparkSession.builder \
    .appName("snowflake-transform") \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")

df = spark.read \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**snowflake_options) \
    .option("query", data_query) \
    .load()

logger.info("STEP 1/5: Extracting data for devices")
data_cleanser = DataCleanser(location_pipeline_config)
cleansed_df = data_cleanser.cleanse(df)
logger.info("==================== After cleaning: ")

logger.info("STEP 2/5: Qualifying data points")
points_qualifier = PointsQualifier(location_pipeline_config)
qualified_df = points_qualifier.get_stay_points(cleansed_df)

logger.info("STEP 3/5: Clustering data points")
clusterer = ClusterStayPoints(location_pipeline_config)
clustered_df, cluster_labels = clusterer.cluster_stay_points(qualified_df)

logger.info("STEP 4/5: Creating trajectories and interactive route map")
pd_clustered_df = clustered_df.toPandas()
start_date = pd_clustered_df['arrival_time'].min()
end_date = pd_clustered_df['arrival_time'].max()
route_predictor = RoutePredictorOSRM(location_pipeline_config, pd_clustered_df, start_date, end_date)
routes_df, df_schema = route_predictor.create_enhanced_interactive_route_map()

logger.info("STEP 5/5: Saving data to Snowflake")
sdf_qualified = spark.createDataFrame(routes_df, schema=df_schema)

# # Writing to disk for debugging purposes
# sdf_qualified.write.format("csv") \
#         .option("header", "true") \
#         .option("delimiter", "|") \
#         .save("/data")

logger.info(sdf_qualified.show())
# sdf_qualified.write \
#     .format(SNOWFLAKE_SOURCE_NAME) \
#     .options(**snowflake_options) \
#     .option("dbtable", "t2") \
#     .mode(SaveMode.Overwrite) \
#     .save()