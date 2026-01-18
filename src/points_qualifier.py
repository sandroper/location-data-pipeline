import logging
from utils.location_pipeline_config_manager import LocationPipelineConfig 
from utils.time_zone_utils import get_timezone_offset
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta
import pandas as pd


def __haversine__(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c


class PointsQualifier:
    def __init__(self, config: LocationPipelineConfig):
        self.dist_threshold_m: int = int(config.dist_threshold_m)
        self.time_threshold_min: int = int(config.time_threshold_min)
        self.time_zone: str = str(config.time_zone)    
        self.TIME_ZONE_OFFSET_HOURS = get_timezone_offset(self.time_zone)

    def get_stay_points(self, device_data: pd.DataFrame):
        df = device_data.copy()
        stay_points = []
        i = 0
        while i < len(df):
            logging.info(f"Processing row {i} of {len(df)}")
            j = i + 1
            while j < len(df):
                dist = __haversine__(df.loc[i, 'lat'], df.loc[i, 'lon'], df.loc[j, 'lat'], df.loc[j, 'lon'])
                logging.debug(f"j={j}, i={i}, dist={dist}")
                if dist > self.dist_threshold_m:
                    break
                j += 1
            if j - 1 > i:
            #if j > i:

                t0_str = str(df.loc[i, 'event_ts']).split('+')[0] if '+' in str(df.loc[i, 'event_ts']) else str(df.loc[i, 'event_ts'])
                t1_str = str(df.loc[j-1, 'event_ts']).split('+')[0] if '+' in str(df.loc[j-1, 'event_ts']) else str(df.loc[j-1, 'event_ts'])
                
                t0_utc = datetime.strptime(t0_str, "%Y-%m-%d %H:%M")
                t1_utc = datetime.strptime(t1_str, "%Y-%m-%d %H:%M")

                # Aligning to local time zone using calculated offset
                t0_local = t0_utc + timedelta(hours=self.TIME_ZONE_OFFSET_HOURS)
                t1_local = t1_utc + timedelta(hours=self.TIME_ZONE_OFFSET_HOURS)
                
                logging.debug(f"t0_local={t0_local}, t1_local={t1_local}")
                logging.debug(f"difference={(t1_local - t0_local)}")
                delta_min = (t1_local - t0_local).total_seconds() / 60.0
                logging.debug(f"delta_min={delta_min}")
                if delta_min >= self.time_threshold_min:
                    lat_mean = df.loc[i:j, 'lat'].mean()
                    lon_mean = df.loc[i:j, 'lon'].mean()
                    stay_points.append({
                        'lat': lat_mean,
                        'lon': lon_mean,
                        'arrival_time': df.loc[i, 'event_ts'],
                        'departure_time': df.loc[j-1, 'event_ts'],
                        'duration_minutes': delta_min,
                        'num_points': j - i,
                        'point_type': 'stay_point'
                    })
                    i = j
                # it is not a stay point, it is a trajectory
                else:
                    # add all the points in between i and j to the trajectory
                    for k in range(i, j):
                        stay_points.append({
                            'lat': df.loc[k, 'lat'],
                            'lon': df.loc[k, 'lon'],
                            'arrival_time': df.loc[k, 'event_ts'],
                            'departure_time': df.loc[k, 'event_ts'],
                            'duration_minutes': 0,
                            'num_points': 1,
                            'point_type': 'trajectory'
                        })
                    # update i
                    i = j   
            else:
                # if j-1 is not greater than i, then it is a trajectory point
                stay_points.append({
                    'lat': df.loc[i, 'lat'],
                    'lon': df.loc[i, 'lon'],
                    'arrival_time': df.loc[i, 'event_ts'],
                    'departure_time': df.loc[i, 'event_ts'],
                    'duration_minutes': 0,
                    'num_points': 1,
                    'point_type': 'trajectory'
                })
                i += 1
        stay_df = pd.DataFrame(stay_points)
        
        logging.info(f"Detected {len(stay_points)} stay points. Saved to stay_points.csv.")
        if len(stay_points) > 0:
            return stay_df
        else:
            return None

