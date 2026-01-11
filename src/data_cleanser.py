from ast import List
import logging
from datetime import datetime
from typing import Any
from pyspark.sql import DataFrame
from utils.location_pipeline_config_manager import LocationPipelineConfig 
from pyspark.sql.functions import to_timestamp, col, date_format
from geopy.distance import geodesic
import pandas as pd

class DataCleanser:
    def __init__(self, config: LocationPipelineConfig):
        self.max_distance_threshold_km: int = int(config.max_distance_threshold_km)
        self.min_distance_threshold_km: int = int(config.min_distance_threshold_km)
        self.max_speed_kmh : int = int(config.max_speed_kmh)

    def cleanse(self, df_raw_data: DataFrame) -> pd.DataFrame: 
        logging.info(f"STEP 1/5: Extracting data for devices")
        pdf_raw_data = df_raw_data.toPandas()
        if self.__sanitize_df__(pdf_raw_data) is None:
            logging.error("ERROR: can't proceed with data processing. DataFrame is empty")
            return pdf_raw_data

        step_start = datetime.now()
        device_data = self.__extract_device_data__(pdf_raw_data)
        cleaned_data = self.__clean_trajectory__(device_data)
        step_duration = datetime.now() - step_start
        logging.info(f"Data extraction completed in {step_duration.total_seconds():.2f} seconds")
        return cleaned_data

    def __sanitize_df__(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df) == 0:
            logging.error("Error: Empty dataframe provided")
            return None
        if 'ID' not in df.columns:
            logging.error("Error: 'device_id' column not found in dataframe")
            return None

        return df


    def __extract_device_data__(self, device_df: pd.DataFrame) -> pd.DataFrame:
        # Rename first three columns to event_ts, lat, lon and keep only those
        columns = device_df.columns.tolist()
        device_df = device_df.iloc[:, :4].copy()
        device_df.columns = ['event_ts', 'device_id', 'lat', 'lon']
        
        logging.info(f"\nDevice Data Summary:")
        logging.info(f"   Total rows: {len(device_df):,}")
        
        # Convert event_ts to datetime and sort chronologically before formatting as string
        device_df['event_ts'] = pd.to_datetime(device_df['event_ts'], errors='coerce')
        device_df = device_df.sort_values(by='event_ts')
        # Format datetime as string in desired format after sorting
        device_df['event_ts'] = device_df['event_ts'].dt.strftime('%Y-%m-%d %H:%M')

        device_df = device_df.reset_index(drop=True)
        return device_df

    
    def __clean_trajectory__(self, pdf: pd.DataFrame) -> pd.DataFrame:
        cleaned_df = pdf.copy()
        
        if cleaned_df['event_ts'].dtype == 'object':
            cleaned_df['event_ts'] = pd.to_datetime(cleaned_df['event_ts'], errors='coerce')
        
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df.dropna(subset=['lat', 'lon', 'event_ts'])
        missing_coords = initial_count - len(cleaned_df)
        if missing_coords > 0:
            logging.info(f"Removed {missing_coords} rows with missing coordinates or timestamps")

        distances, time_diffs, speeds = self.__compute_location_params__(cleaned_df)
        pdf = self.__remove_invalid_points__(cleaned_df, distances, time_diffs, speeds)
        pdf = self.__remove_stationary_outliers__(pdf)

        removed_total = initial_count - len(pdf)
        if removed_total > 0:
            logging.info(f"\nTrajectory Cleaning Summary:")
            logging.info(f"   Initial points: {initial_count}")
            logging.info(f"   Final points: {len(pdf)}")
            logging.info(f"   Removed: {removed_total} ({removed_total/initial_count*100:.1f}%)")
        else:
            logging.info("No incoherent points detected in trajectory")
        
        if 'event_ts' in pdf.columns:
            pdf['event_ts'] = pdf['event_ts'].dt.strftime('%Y-%m-%d %H:%M')

        return pdf


    def __remove_invalid_points__(self, cleaned_df: pd.DataFrame, distances: list[float], time_diffs: list[float], speeds: list[float]) -> pd.DataFrame:
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

        return cleaned_df

    
    def __remove_stationary_outliers__(self, pdf: pd.DataFrame) -> pd.DataFrame: 
        # Check for stationary outliers (points far from neighbors while stationary)
        if len(pdf) >= 3:
            outliers = set()
            for i in range(1, len(pdf) - 1):
                coord_prev = (pdf.loc[i-1, 'lat'], pdf.loc[i-1, 'lon'])
                coord_curr = (pdf.loc[i, 'lat'], pdf.loc[i, 'lon'])
                coord_next = (pdf.loc[i+1, 'lat'], pdf.loc[i+1, 'lon'])
                
                dist_to_prev = geodesic(coord_curr, coord_prev).kilometers
                dist_to_next = geodesic(coord_curr, coord_next).kilometers
                dist_prev_next = geodesic(coord_prev, coord_next).kilometers
                
                # If current point is far from both neighbors but neighbors are close
                if dist_to_prev > self.max_distance_threshold_km and dist_to_next > self.max_distance_threshold_km and dist_prev_next < self.min_distance_threshold_km:
                    outliers.add(i)
                    logging.debug(f"Point {i}: Spatial outlier (distances: {dist_to_prev:.2f}, {dist_to_next:.2f} km)")
            
            if outliers:
                df = pdf.drop(index=list(outliers)).reset_index(drop=True)
                logging.info(f"Removed {len(outliers)} spatial outlier points")

        return pdf
        
        

    def __compute_location_params__(self, pdf: pd.DataFrame) -> (list[float], list[float], list[float]):
        distances = []
        time_diffs = []
        speeds = []
        
        for i in range(len(pdf) - 1):
            coord1 = (pdf.loc[i, 'lat'], pdf.loc[i, 'lon'])
            coord2 = (pdf.loc[i+1, 'lat'], pdf.loc[i+1, 'lon'])
            
            # Distance in kilometers
            distance = geodesic(coord1, coord2).kilometers
            distances.append(distance)
            
            # Time difference in hours
            time_diff = (pdf.loc[i+1, 'event_ts'] - pdf.loc[i, 'event_ts']).total_seconds() / 3600
            time_diffs.append(time_diff)
            
            # Speed in km/h
            if time_diff > 0:
                speed = distance / time_diff
            else:
                speed = float('inf')  # Mark same timestamp as invalid
            speeds.append(speed)

        return (distances, time_diffs, speeds)
        
        