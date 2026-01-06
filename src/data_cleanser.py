
import logging
from datetime import datetime
from pyspark.sql import DataFrame
from location_pipeline_config_manager import LocationPipelineConfig 
from pyspark.sql.functions import to_timestamp, col, date_format

class DataCleanser:
    def __init__(self, df_raw_data: DataFrame, config: LocationPipelineConfig):
        self.max_distance_threshold_km = config.max_distance_threshold_km
        self.min_distance_threshold_km = config.min_distance_threshold_km
        self.max_speed_kmh = config.max_speed_kmh
        self.df_raw_data = df_raw_data

    def cleanse(self) -> DataFrame: 
        logging.info(f"STEP 1/5: Extracting data for devices")
        step_start = datetime.now()
        device_data = self.__extract_device_data__(self.df_raw_data)
        # device_data = self.__clean_trajectory__(device_data)
        step_duration = datetime.now() - step_start
        logging.info(f"Data extraction completed in {step_duration.total_seconds():.2f} seconds")
        return device_data

    def __extract_device_data__(self, device_df: DataFrame):
        if device_df is None:
            logging.error("Error: No dataframe provided")
            return None
        if 'ID' not in device_df.columns:
            logging.error("Error: 'device_id' column not found in dataframe")
            return None
        
        # Rename first three columns to event_ts, lat, lon and keep only those
        columns = device_df.columns
        device_df = device_df.select(*device_df.columns[:4])
        device_df = device_df.toDF('event_ts', 'device_id', 'lat', 'lon')
        
        logging.info(f"\nDevice Data Summary:")
        logging.info(f"   Total rows: {device_df.count():,}")
        
        # Convert event_ts to datetime and sort chronologically before formatting as string
        device_df = device_df.withColumn('event_ts', to_timestamp(col('event_ts')))
        device_df = device_df.orderBy('event_ts')
        # Format datetime as string in desired format after sorting
        device_df = device_df.withColumn('event_ts', date_format(col('event_ts'), 'yyyy-MM-dd HH:mm'))

        return device_df