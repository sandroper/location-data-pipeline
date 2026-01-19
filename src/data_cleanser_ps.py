import logging
from datetime import datetime
from utils.location_pipeline_config_manager import LocationPipelineConfig
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, lag, lead, when, lit
from pyspark.sql.functions import sin, cos, asin, sqrt, radians
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

# @udf(returnType=DoubleType())
# def geodesic_km(lat1, lon1, lat2, lon2):
#     if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
#         return None
#     return geodesic((lat1, lon1), (lat2, lon2)).kilometers
#
# def haversine_spark(lat1, lon1, lat2, lon2):
def geodesic_km(lat1, lon1, lat2, lon2):
    """Haversine formula as Spark expression (no UDF overhead)"""
    R = 6371.0  # Earth radius in km
    lat1_r, lon1_r = radians(lat1), radians(lon1)
    lat2_r, lon2_r = radians(lat2), radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = sin(dlat / 2) ** 2 + cos(lat1_r) * cos(lat2_r) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))

class DataCleanser:
    def __init__(self, config: LocationPipelineConfig):
        self.max_distance_threshold_km: int = int(config.max_distance_threshold_km)
        self.min_distance_threshold_km: int = int(config.min_distance_threshold_km)
        self.max_speed_kmh : int = int(config.max_speed_kmh)

    def cleanse(self, df_raw_data: DataFrame) -> DataFrame:
        logger.info("STEP 1/5: Extracting data for devices")

        if df_raw_data.count() == 0:
            logger.error("ERROR: Empty DataFrame")
            return df_raw_data

        step_start = datetime.now()

        device_data = self.__extract_device_data__(df_raw_data)
        cleaned_data = self.__clean_trajectory__(device_data)

        step_duration = datetime.now() - step_start
        logger.info(f"Data extraction completed in {step_duration.total_seconds():.2f} seconds")

        return cleaned_data

    def __extract_device_data__(self, device_df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import to_timestamp, col, date_format

        # Get the first 4 column names
        columns = device_df.columns[:4]

        # Select and rename columns in one operation (lazy transformation)
        device_df = device_df.select(
            col(columns[0]).alias('event_ts'),
            col(columns[1]).alias('device_id'),
            col(columns[2]).alias('lat'),
            col(columns[3]).alias('lon')
        )

        # Convert to timestamp, sort, and format in a chain
        device_df = (device_df
                     .withColumn('event_ts', to_timestamp(col('event_ts')))
                     .orderBy('event_ts')
                     .withColumn('event_ts', date_format(col('event_ts'), 'yyyy-MM-dd HH:mm'))
                     )

        # Log count (triggers action, but only once)
        logger.info(f"\nDevice Data Summary:")
        logger.info(f"   Total rows: {device_df.count():,}")

        return device_df

    def __clean_trajectory__(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import to_timestamp, date_format

        # Ensure event_ts is timestamp type
        df = df.withColumn('event_ts', to_timestamp(col('event_ts')))

        initial_count = df.count()

        # Remove rows with missing coordinates or timestamps
        df = df.filter(
            col('lat').isNotNull() &
            col('lon').isNotNull() &
            col('event_ts').isNotNull()
        )

        # Compute location params (adds distance, time_diff, speed columns)
        df = self.__compute_location_params__(df)

        # Remove invalid points based on speed
        df = self.__remove_invalid_points__(df)

        # Remove stationary outliers
        df = self.__remove_stationary_outliers__(df)

        # Clean up intermediate columns
        cleanup_cols = ['prev_lat', 'prev_lon', 'prev_event_ts', 'next_lat', 'next_lon',
                        'distance_km', 'time_diff_hours', 'speed_kmh',
                        'dist_to_prev', 'dist_to_next', 'dist_prev_next', 'is_outlier']
        df = df.drop(*[c for c in cleanup_cols if c in df.columns])

        # Format event_ts as string
        df = df.withColumn('event_ts', date_format(col('event_ts'), 'yyyy-MM-dd HH:mm'))

        final_count = df.count()
        logger.info(f"Trajectory Cleaning: {initial_count} -> {final_count} points")

        return df

    def __remove_invalid_points__(self, df: DataFrame) -> DataFrame:
        initial_count = df.count()

        # Filter out points with impossible speeds
        df = df.filter(
            (col('speed_kmh').isNull()) |  # Keep first point (no previous)
            (col('speed_kmh') <= self.max_speed_kmh)
        )

        removed = initial_count - df.count()
        if removed > 0:
            logger.info(f"Removed {removed} points due to impossible speeds")

        return df

    def __remove_stationary_outliers__(self, df: DataFrame) -> DataFrame:
        initial_count = df.count()

        window_spec = Window.orderBy('event_ts')

        # Get previous and next point coordinates
        df = (df
              .withColumn('prev_lat', lag('lat').over(window_spec))
              .withColumn('prev_lon', lag('lon').over(window_spec))
              .withColumn('next_lat', lead('lat').over(window_spec))
              .withColumn('next_lon', lead('lon').over(window_spec))
              )

        # Compute distances to neighbors and between neighbors
        df = (df
              .withColumn('dist_to_prev',
                          geodesic_km(col('lat'), col('lon'), col('prev_lat'), col('prev_lon')))
              .withColumn('dist_to_next',
                          geodesic_km(col('lat'), col('lon'), col('next_lat'), col('next_lon')))
              .withColumn('dist_prev_next',
                          geodesic_km(col('prev_lat'), col('prev_lon'), col('next_lat'), col('next_lon')))
              )

        # Identify outliers: far from both neighbors, but neighbors are close to each other
        df = df.withColumn('is_outlier',
                           (col('dist_to_prev') > self.max_distance_threshold_km) &
                           (col('dist_to_next') > self.max_distance_threshold_km) &
                           (col('dist_prev_next') < self.min_distance_threshold_km)
                           )

        # Filter out outliers
        df = df.filter(~col('is_outlier') | col('is_outlier').isNull())

        removed = initial_count - df.count()
        if removed > 0:
            logger.info(f"Removed {removed} spatial outlier points")

        return df

    def __compute_location_params__(self, df: DataFrame) -> DataFrame:
        # Define window ordered by event_ts
        window_spec = Window.orderBy('event_ts')

        # Get previous point's coordinates and timestamp using lag()
        df = (df
              .withColumn('prev_lat', lag('lat').over(window_spec))
              .withColumn('prev_lon', lag('lon').over(window_spec))
              .withColumn('prev_event_ts', lag('event_ts').over(window_spec))
              )

        # Compute distance to previous point (km)
        df = df.withColumn(
            'distance_km',
            geodesic_km(col('prev_lat'), col('prev_lon'), col('lat'), col('lon'))
        )

        # Compute time difference in hours
        df = df.withColumn(
            'time_diff_hours',
            (col('event_ts').cast('long') - col('prev_event_ts').cast('long')) / 3600.0
        )

        # Compute speed (km/h), handling division by zero
        df = df.withColumn(
            'speed_kmh',
            when(col('time_diff_hours') > 0, col('distance_km') / col('time_diff_hours'))
            .otherwise(lit(float('inf')))
        )

        return df

