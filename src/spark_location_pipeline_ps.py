"""
Spark job to connect to Snowflake, retrieve data, run the location pipeline and save back to Snowflake.

Pure Spark implementation - all operations use partitionBy('device_id') to ensure
each device's data is processed independently without cross-contamination.
"""

import logging

from datetime import datetime
from utils.logging_config import setup_logging
from utils.osrm.osrm_json_data_dumper import OSRMRoutesWriter
from utils.osrm.device_route_processor import DeviceRouteProcessor

# Configure logging from environment variables
setup_logging()

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from snowflake_config_manager import SnowflakeConfig
from utils.location_pipeline_config_manager import LocationPipelineConfig
from utils.dataframe_dumper import DataFrameDumper

from data_cleanser_ps import DataCleanser
from points_qualifier_ps import PointsQualifier
from cluster_stay_points_ps import ClusterStayPoints

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


def create_spark_session() -> SparkSession:
    """Create and configure SparkSession with Sedona for geodesic calculations."""
    spark = SparkSession.builder \
        .appName("location-data-pipeline") \
        .getOrCreate()

    # Initialize Sedona for geodesic distance calculations
    from sedona.spark import SedonaContext
    sedona = SedonaContext.create(spark)
    logger.info("Sedona geospatial extensions initialized")

    return sedona


def run_pipeline():
    """
    Main pipeline execution using pure Spark operations.

    All operations are partitioned by device_id to ensure:
    - Each device's data is processed independently
    - No cross-contamination between devices
    - Distributed processing across workers
    """
    # Initialize configurations
    location_pipeline_config = LocationPipelineConfig()
    logger.info(f"Pipeline config: {location_pipeline_config}")

    snowflake_config = SnowflakeConfig()
    snowflake_options = snowflake_config.snowflake_options
    logger.info(f"Connecting to: {snowflake_options['sfDatabase']}.{snowflake_options['sfSchema']}.{snowflake_config.table_name}")

    # Initialize debug dumper for CSV/JSON output
    dumper = DataFrameDumper(location_pipeline_config)

    start_time = datetime.now()

    # Create Spark session
    spark = create_spark_session()

    # Load data from Snowflake
    logger.info(f"Query: {snowflake_config.query}")

    df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("query", snowflake_config.query) \
        .load()

    total_records = df.count()
    logger.info(f"Loaded {total_records} total records")

    # Identify and standardize device_id column
    # Assuming device_id is the second column or named 'ID'
    columns = df.columns
    device_id_col = columns[1] if len(columns) > 1 else columns[0]

    if device_id_col.upper() == 'ID':
        df = df.withColumnRenamed(device_id_col, 'device_id')
    elif device_id_col != 'device_id':
        df = df.withColumnRenamed(device_id_col, 'device_id')

    # Repartition by device_id for data locality
    # This ensures all data for a device is on the same partition
    num_devices_initial = df.select('device_id').distinct().count()
    num_partitions = max(num_devices_initial, 10)  # At least 10 partitions for parallelism

    logger.info(f"Found {num_devices_initial} unique devices, repartitioning to {num_partitions} partitions")
    df = df.repartition(num_partitions, col('device_id'))

    # ========== STEP 1: Data cleansing ==========
    logger.info("STEP 1/5: Cleansing data (partitioned by device_id)")
    data_cleanser = DataCleanser(location_pipeline_config)
    cleansed_df = data_cleanser.cleanse(df)

    if location_pipeline_config.save_routes_json:
        dumper.dump(cleansed_df, "cleansed")

    cleansed_count = cleansed_df.count()
    logger.info(f"After cleansing: {cleansed_count} records")

    # ========== STEP 2: Stay points detection ==========
    logger.info("STEP 2/5: Detecting stay points (partitioned by device_id)")
    points_qualifier = PointsQualifier(location_pipeline_config)
    qualified_df = points_qualifier.get_stay_points(cleansed_df)

    if location_pipeline_config.save_routes_json:
        dumper.dump(qualified_df, "qualified")

    stay_point_count = qualified_df.filter(col('point_type') == 'stay_point').count()
    trajectory_count = qualified_df.filter(col('point_type') == 'trajectory').count()
    logger.info(f"Detected {stay_point_count} stay points, {trajectory_count} trajectory points")

    # ========== STEP 3: Clustering stay points ==========
    logger.info("STEP 3/5: Clustering stay points (partitioned by device_id)")
    clusterer = ClusterStayPoints(location_pipeline_config)
    clustered_df, cluster_labels = clusterer.cluster_stay_points(qualified_df)

    if location_pipeline_config.save_routes_json:
        dumper.dump(clustered_df, "clustered")

    # ========== STEP 4: Creating trajectories and interactive routes ==========
    logger.info("STEP 4/5: Creating trajectories and routes (per device)")

    # Convert to pandas and process each device
    pd_clustered_df = clustered_df.toPandas()
    num_devices_for_routes = pd_clustered_df['device_id'].nunique()

    # Log if devices were lost during processing steps
    if num_devices_for_routes < num_devices_initial:
        logger.warning(f"Device count reduced: {num_devices_initial} -> {num_devices_for_routes} "
                       f"({num_devices_initial - num_devices_for_routes} devices lost during cleansing/qualification/clustering)")

    # Process routes for all devices
    route_processor = DeviceRouteProcessor(location_pipeline_config)
    all_device_results, route_stats = route_processor.process_all_devices(pd_clustered_df)

    # Save JSON output if enabled
    if location_pipeline_config.save_routes_json and all_device_results:
        logger.info(f"Saving route data for {len(all_device_results)} devices to JSON...")
        routes_writer = OSRMRoutesWriter(location_pipeline_config)
        saved_paths = routes_writer.save_all_devices(all_device_results)
        logger.info(f"Saved {len(saved_paths)} JSON files")

    clustered_df.cache()

    # ========== STEP 5: Summary and final output ==========
    logger.info("STEP 5/5: Pipeline complete")

    # Log summary statistics
    final_count = clustered_df.count()
    num_clusters = clustered_df.filter(col('cluster_id') >= 0) \
        .select('device_id', 'cluster_id').distinct().count()

    end_time = datetime.now()
    pipeline_duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("Pipeline Complete - Summary:")
    logger.info(f"  - Input records: {total_records}")
    logger.info(f"  - Devices in source data: {num_devices_initial}")
    logger.info(f"  - Devices after processing: {num_devices_for_routes}")
    logger.info(f"  - Devices with routes: {route_stats.devices_with_routes}")
    logger.info(f"  - Output records: {final_count}")
    logger.info(f"  - Stay points: {stay_point_count}")
    logger.info(f"  - Trajectory points: {trajectory_count}")
    logger.info(f"  - Clusters found: {num_clusters}")
    logger.info(f"  - Total route segments: {route_stats.total_routes}")
    logger.info(f"  - Total route distance: {route_stats.total_distance_km:.2f} km")
    logger.info(f"  - Total pipeline duration: {pipeline_duration.total_seconds():.2f} seconds")
    logger.info("=" * 60)

    # Show sample output
    clustered_df.show(20, truncate=False)

    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()