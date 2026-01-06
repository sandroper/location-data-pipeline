
import os
import pandas as pd
import configparser
from datetime import datetime
import logging
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, date_format, lag, lead, row_number, when, isnan, isnull, unix_timestamp, lit, pandas_udf
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from location_pipeline_config_manager import LocationPipelineConfig

# Function for geodesic distance calculation - defined at module level for serialization
def _geodesic_distance_func(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    """Calculate geodesic distance in kilometers between two coordinates"""
    from geopy.distance import geodesic
    result = []
    for i in range(len(lat1)):
        if pd.isna(lat1.iloc[i]) or pd.isna(lon1.iloc[i]) or pd.isna(lat2.iloc[i]) or pd.isna(lon2.iloc[i]):
            result.append(None)
        else:
            try:
                dist = float(geodesic((float(lat1.iloc[i]), float(lon1.iloc[i])), (float(lat2.iloc[i]), float(lon2.iloc[i]))).kilometers)
                result.append(dist)
            except:
                result.append(None)
    return pd.Series(result)

# Create pandas UDF at module level
_geodesic_distance_pandas = pandas_udf(_geodesic_distance_func, returnType=DoubleType())


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
        device_data = self.__clean_trajectory__(device_data)
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

    def __clean_trajectory__(self, df):
        if df is None or df.count() == 0:
            logging.warning("No data to clean")
            return df
        
        # Get Spark session for DataFrame conversion
        from pyspark.sql import SparkSession
        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            spark_session = df.sql_ctx.sparkSession
        
        cleaned_df = df
        
        initial_count = cleaned_df.count()
        # Drop rows with null values in lat, lon, or event_ts
        cleaned_df = cleaned_df.filter(
            col('lat').isNotNull() & 
            col('lon').isNotNull() & 
            col('event_ts').isNotNull()
        )
        missing_coords = initial_count - cleaned_df.count()
        if missing_coords > 0:
            logging.info(f"Removed {missing_coords} rows with missing coordinates or timestamps")
        
        # Convert event_ts to timestamp if it's a string, then sort
        cleaned_df = cleaned_df.withColumn('event_ts', to_timestamp(col('event_ts'), 'yyyy-MM-dd HH:mm'))
        cleaned_df = cleaned_df.orderBy('event_ts')
        
        # Add row number for tracking
        window_spec = Window.orderBy('event_ts')
        cleaned_df = cleaned_df.withColumn('row_num', row_number().over(window_spec))
        
        row_count = cleaned_df.count()
        if row_count < 2:
            logging.warning("Not enough data points for trajectory cleaning")
            cleaned_df = cleaned_df.drop('row_num')
            return cleaned_df
        
        # Calculate distances and speeds using window functions
        # Get previous row values
        cleaned_df = cleaned_df.withColumn('prev_lat', lag('lat', 1).over(window_spec))
        cleaned_df = cleaned_df.withColumn('prev_lon', lag('lon', 1).over(window_spec))
        cleaned_df = cleaned_df.withColumn('prev_event_ts', lag('event_ts', 1).over(window_spec))
        
        # Convert to pandas for distance calculation (avoids UDF serialization issues)
        # This is a workaround for serialization problems with UDFs
        pdf = cleaned_df.toPandas()
        from geopy.distance import geodesic
        
        # Calculate distance to previous point
        distances = []
        for i in range(len(pdf)):
            if pd.isna(pdf.iloc[i]['prev_lat']) or pd.isna(pdf.iloc[i]['prev_lon']) or pd.isna(pdf.iloc[i]['lat']) or pd.isna(pdf.iloc[i]['lon']):
                distances.append(None)
            else:
                try:
                    dist = float(geodesic((float(pdf.iloc[i]['prev_lat']), float(pdf.iloc[i]['prev_lon'])), 
                                         (float(pdf.iloc[i]['lat']), float(pdf.iloc[i]['lon']))).kilometers)
                    distances.append(dist)
                except:
                    distances.append(None)
        pdf['distance_prev'] = distances
        
        # Convert back to Spark DataFrame
        cleaned_df = spark_session.createDataFrame(pdf)
        
        # Calculate time difference in hours
        cleaned_df = cleaned_df.withColumn('time_diff_hours',
            when(col('prev_event_ts').isNotNull(),
                 (unix_timestamp(col('event_ts')) - unix_timestamp(col('prev_event_ts'))) / 3600.0)
            .otherwise(None))
        
        # Calculate speed in km/h
        cleaned_df = cleaned_df.withColumn('speed',
            when((col('time_diff_hours').isNotNull()) & (col('time_diff_hours') > 0) & (col('distance_prev').isNotNull()),
                 col('distance_prev') / col('time_diff_hours'))
            .otherwise(lit(float('inf'))))
        
        # Mark points to remove based on speed threshold
        # Remove the current point if speed from previous point exceeds threshold
        cleaned_df = cleaned_df.withColumn('remove_speed',
            when(col('speed') > self.max_speed_kmh, lit(True)).otherwise(lit(False)))
        
        # Collect rows to remove for logging
        rows_to_remove_speed = cleaned_df.filter(col('remove_speed') == True).collect()
        if rows_to_remove_speed:
            for row in rows_to_remove_speed:
                logging.debug(f"Point {row['row_num']}: Impossible speed {row['speed']:.2f} km/h (distance: {row['distance_prev']:.2f} km, time: {row['time_diff_hours']*60:.2f} min)")
        
        # Remove points with impossible speeds
        cleaned_df = cleaned_df.filter(col('remove_speed') == False)
        
        removed_speed_count = len(rows_to_remove_speed)
        if removed_speed_count > 0:
            logging.info(f"Removed {removed_speed_count} incoherent points due to impossible speeds/accelerations")
        
        # Recalculate row numbers after filtering
        cleaned_df = cleaned_df.drop('row_num')
        window_spec = Window.orderBy('event_ts')
        cleaned_df = cleaned_df.withColumn('row_num', row_number().over(window_spec))
        
        # Check for stationary outliers (points far from neighbors while stationary)
        row_count = cleaned_df.count()
        if row_count >= 3:
            # Get previous and next row values
            cleaned_df = cleaned_df.withColumn('prev_lat', lag('lat', 1).over(window_spec))
            cleaned_df = cleaned_df.withColumn('prev_lon', lag('lon', 1).over(window_spec))
            cleaned_df = cleaned_df.withColumn('next_lat', lead('lat', 1).over(window_spec))
            cleaned_df = cleaned_df.withColumn('next_lon', lead('lon', 1).over(window_spec))
            
            # Calculate distances using pandas (avoids UDF serialization issues)
            pdf = cleaned_df.toPandas()
            from geopy.distance import geodesic
            
            dist_to_prev_list = []
            dist_to_next_list = []
            dist_prev_next_list = []
            
            for i in range(len(pdf)):
                # Distance to previous
                if pd.isna(pdf.iloc[i]['prev_lat']) or pd.isna(pdf.iloc[i]['prev_lon']) or pd.isna(pdf.iloc[i]['lat']) or pd.isna(pdf.iloc[i]['lon']):
                    dist_to_prev_list.append(None)
                else:
                    try:
                        dist = float(geodesic((float(pdf.iloc[i]['lat']), float(pdf.iloc[i]['lon'])), 
                                             (float(pdf.iloc[i]['prev_lat']), float(pdf.iloc[i]['prev_lon']))).kilometers)
                        dist_to_prev_list.append(dist)
                    except:
                        dist_to_prev_list.append(None)
                
                # Distance to next
                if pd.isna(pdf.iloc[i]['next_lat']) or pd.isna(pdf.iloc[i]['next_lon']) or pd.isna(pdf.iloc[i]['lat']) or pd.isna(pdf.iloc[i]['lon']):
                    dist_to_next_list.append(None)
                else:
                    try:
                        dist = float(geodesic((float(pdf.iloc[i]['lat']), float(pdf.iloc[i]['lon'])), 
                                             (float(pdf.iloc[i]['next_lat']), float(pdf.iloc[i]['next_lon']))).kilometers)
                        dist_to_next_list.append(dist)
                    except:
                        dist_to_next_list.append(None)
                
                # Distance between previous and next
                if pd.isna(pdf.iloc[i]['prev_lat']) or pd.isna(pdf.iloc[i]['prev_lon']) or pd.isna(pdf.iloc[i]['next_lat']) or pd.isna(pdf.iloc[i]['next_lon']):
                    dist_prev_next_list.append(None)
                else:
                    try:
                        dist = float(geodesic((float(pdf.iloc[i]['prev_lat']), float(pdf.iloc[i]['prev_lon'])), 
                                             (float(pdf.iloc[i]['next_lat']), float(pdf.iloc[i]['next_lon']))).kilometers)
                        dist_prev_next_list.append(dist)
                    except:
                        dist_prev_next_list.append(None)
            
            pdf['dist_to_prev'] = dist_to_prev_list
            pdf['dist_to_next'] = dist_to_next_list
            pdf['dist_prev_next'] = dist_prev_next_list
            
            # Convert back to Spark DataFrame
            cleaned_df = spark_session.createDataFrame(pdf)
            
            # Mark outliers: current point far from both neighbors but neighbors are close
            cleaned_df = cleaned_df.withColumn('remove_outlier',
                when(
                    (col('dist_to_prev').isNotNull()) &
                    (col('dist_to_next').isNotNull()) &
                    (col('dist_prev_next').isNotNull()) &
                    (col('dist_to_prev') > self.max_distance_threshold_km) &
                    (col('dist_to_next') > self.max_distance_threshold_km) &
                    (col('dist_prev_next') < self.min_distance_threshold_km),
                    lit(True)
                ).otherwise(lit(False)))
            
            # Collect outliers for logging
            outliers = cleaned_df.filter(col('remove_outlier') == True).collect()
            if outliers:
                for row in outliers:
                    logging.debug(f"Point {row['row_num']}: Spatial outlier (distances: {row['dist_to_prev']:.2f}, {row['dist_to_next']:.2f} km)")
            
            # Remove outliers
            cleaned_df = cleaned_df.filter(col('remove_outlier') == False)
            
            outlier_count = len(outliers)
            if outlier_count > 0:
                logging.info(f"Removed {outlier_count} spatial outlier points")
        
        # Clean up temporary columns
        cols_to_drop = ['row_num', 'prev_lat', 'prev_lon', 'prev_event_ts', 'distance_prev', 
                       'time_diff_hours', 'speed', 'remove_speed', 'next_lat', 'next_lon',
                       'dist_to_prev', 'dist_to_next', 'dist_prev_next', 'remove_outlier']
        existing_cols_to_drop = [c for c in cols_to_drop if c in cleaned_df.columns]
        if existing_cols_to_drop:
            cleaned_df = cleaned_df.drop(*existing_cols_to_drop)
        
        final_count = cleaned_df.count()
        removed_total = initial_count - final_count
        if removed_total > 0:
            logging.info(f"\nTrajectory Cleaning Summary:")
            logging.info(f"   Initial points: {initial_count}")
            logging.info(f"   Final points: {final_count}")
            logging.info(f"   Removed: {removed_total} ({removed_total/initial_count*100:.1f}%)")
        else:
            logging.info("No incoherent points detected in trajectory")
        
        # Format event_ts as string if it's still a timestamp
        cleaned_df = cleaned_df.withColumn('event_ts', date_format(col('event_ts'), 'yyyy-MM-dd HH:mm'))
        
        return cleaned_df




          
            
            
