
from ast import List
import logging
from datetime import datetime
from typing import Any
from pyspark.sql import DataFrame
from location_pipeline_config_manager import LocationPipelineConfig 
from pyspark.sql.functions import to_timestamp, col, date_format
from geopy.distance import geodesic
import pandas as pd

class DataCleanser:
    def __init__(self, df_raw_data: DataFrame, config: LocationPipelineConfig):
        self.max_distance_threshold_km: int = int(config.max_distance_threshold_km)
        self.min_distance_threshold_km: int = int(config.min_distance_threshold_km)
        self.max_speed_kmh : int = int(config.max_speed_kmh)
        self.df_raw_data: DataFrame = df_raw_data

    def cleanse(self) -> DataFrame: 
        logging.info(f"STEP 1/5: Extracting data for devices")
        step_start = datetime.now()
        device_data = self.__extract_device_data__(self.df_raw_data)
        cleaned_data = self.__clean_trajectory__(device_data)
        step_duration = datetime.now() - step_start
        logging.info(f"Data extraction completed in {step_duration.total_seconds():.2f} seconds")
        return cleaned_data

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
        device_df = device_df.withColumn('event_ts', to_timestamp(col('event_ts'), "yyyy-MM-dd HH:mm:ss"))
        device_df = device_df.orderBy('event_ts')
        # Format datetime as string in desired format after sorting
        device_df = device_df.withColumn('event_ts', date_format(col('event_ts'), 'yyyy-MM-dd HH:mm'))

        return device_df


    def __clean_trajectory__(self, df: DataFrame) -> DataFrame:
        if df is None or df.count() == 0:
            logging.warning("No data to clean")
            return df
        cleaned_df = df

        cleaned_df = cleaned_df.withColumn('event_ts', to_timestamp(col('event_ts'), 'yyyy-MM-dd HH:mm'))
        cleaned_df = cleaned_df.orderBy('event_ts')

        initial_count = cleaned_df.count()

        cleaned_df = cleaned_df.filter(
            col('lat').isNotNull() & 
            col('lon').isNotNull() & 
            col('event_ts').isNotNull()
        )

        missing_coords = initial_count - cleaned_df.count()
        if missing_coords > 0:
            logging.info(f"Removed {missing_coords} rows with missing coordinates or timestamps")

        if cleaned_df.count() < 2:
            logging.warning("Not enough data points for trajectory cleaning")
            return cleaned_df
        
        return self.__remove_anomalies__(df, initial_count)


    ## TODO: This is just a literal copy of the original panadas-based logic and no optimization has been applied
    ## However it should be possible to apply at least 2 optimizations in a pretty straightforward way:
    ##
    ## 1. With Pandas: compute all logic in one go, all in the same loop. At the same time that we traverse the
    ##    dataset to compute distances, speed and time_diffs, we also check for anomalies
    ## 2. With Spark: use SQL-based logic rather than row-based calculations
    def __remove_anomalies__(self, original_df: DataFrame, initial_count) -> DataFrame:
        cleaned_df = original_df.toPandas()
        cleaned_df['event_ts'] = pd.to_datetime(cleaned_df['event_ts'], errors='coerce')
        distances, time_diffs, speeds =  self.__compute_distance_speeds__(cleaned_df)
        
        points_to_remove = set()
        for i, speed in enumerate(speeds):
            if speed > self.max_speed_kmh:
                # Remove the next point (i+1) as it's likely the anomaly
                points_to_remove.add(i + 1)
                logging.debug(f"Point {i+1}: Impossible speed {speed:.2f} km/h (distance: {distances[i]:.2f} km, time: {time_diffs[i]*60:.2f} min)")

        # Remove invalid points
        if points_to_remove:
            cleaned_df = cleaned_df.drop(index=list(points_to_remove)).reset_index(drop=True)
            logging.info(f"Removed {len(points_to_remove)} incoherent points due to impossible speeds/accelerations")
        
        # Check for stationary outliers (points far from neighbors while stationary)
        if len(cleaned_df) >= 3:
            outliers = set()
            for i in range(1, len(cleaned_df) - 1):
                coord_prev = (cleaned_df.loc[i-1, 'lat'], cleaned_df.loc[i-1, 'lon'])
                coord_curr = (cleaned_df.loc[i, 'lat'], cleaned_df.loc[i, 'lon'])
                coord_next = (cleaned_df.loc[i+1, 'lat'], cleaned_df.loc[i+1, 'lon'])
                
                dist_to_prev = geodesic(coord_curr, coord_prev).kilometers
                dist_to_next = geodesic(coord_curr, coord_next).kilometers
                dist_prev_next = geodesic(coord_prev, coord_next).kilometers
                
                # If current point is far from both neighbors but neighbors are close
                if dist_to_prev > self.max_distance_threshold_km and dist_to_next > self.max_distance_threshold_km and dist_prev_next < self.min_distance_threshold_km:
                    outliers.add(i)
                    logging.debug(f"Point {i}: Spatial outlier (distances: {dist_to_prev:.2f}, {dist_to_next:.2f} km)")
            
            if outliers:
                cleaned_df = cleaned_df.drop(index=list(outliers)).reset_index(drop=True)
                logging.info(f"Removed {len(outliers)} spatial outlier points")
        
        removed_total = initial_count - len(cleaned_df)
        if removed_total > 0:
            logging.info(f"\nTrajectory Cleaning Summary:")
            logging.info(f"   Initial points: {initial_count}")
            logging.info(f"   Final points: {len(cleaned_df)}")
            logging.info(f"   Removed: {removed_total} ({removed_total/initial_count*100:.1f}%)")
        else:
            logging.info("No incoherent points detected in trajectory")
        
        if 'event_ts' in cleaned_df.columns:
            cleaned_df['event_ts'] = cleaned_df['event_ts'].dt.strftime('%Y-%m-%d %H:%M')

        return cleaned_df

    
    def __compute_distance_speeds__(self, pd_df: pd.DataFrame) -> (List, List, List):
        distances = []
        time_diffs = []
        speeds = []
        
        for i in range(len(pd_df)- 1):
            coord1 = (pd_df.loc[i, 'lat'], pd_df.loc[i, 'lon'])
            coord2 = (pd_df.loc[i+1, 'lat'], pd_df.loc[i+1, 'lon'])
            
            # Distance in kilometers
            distance = geodesic(coord1, coord2).kilometers
            distances.append(distance)

            # Time difference in hours
            time_diff = (pd_df.loc[i+1, 'event_ts'] - pd_df.loc[i, 'event_ts']).total_seconds() / 3600
            time_diffs.append(time_diff)
            
            # Speed in km/h
            if time_diff > 0:
                speed = distance / time_diff
            else:
                speed = float('inf')  # Mark same timestamp as invalid
            speeds.append(speed)

        return (distances, time_diffs, speeds)
        
        
