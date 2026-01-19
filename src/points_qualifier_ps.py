import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lag, when, lit, sum as spark_sum,
    min as spark_min, max as spark_max, avg, count,
    radians, cos, sin, asin, sqrt,
    row_number, expr, first
)
from pyspark.sql.window import Window
from utils.location_pipeline_config_manager import LocationPipelineConfig
from utils.time_zone_utils import get_timezone_offset

logger = logging.getLogger(__name__)


def haversine_spark(lat1, lon1, lat2, lon2):
    """
    Calculate haversine distance in meters using Spark native functions.
    Returns distance in meters between two points.
    """
    R = 6371000  # Earth radius in meters

    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    lon1_rad = radians(lon1)
    lon2_rad = radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))

    return R * c


class PointsQualifier:
    def __init__(self, config: LocationPipelineConfig):
        self.dist_threshold_m: int = int(config.dist_threshold_m)
        self.time_threshold_min: int = int(config.time_threshold_min)
        self.time_zone: str = str(config.time_zone)
        self.TIME_ZONE_OFFSET_HOURS = get_timezone_offset(self.time_zone)

    def get_stay_points(self, device_data: DataFrame) -> DataFrame:
        """
        Identify stay points and trajectory points using PySpark.

        Stay points are locations where the device remained within dist_threshold_m
        for at least time_threshold_min minutes.

        Args:
            device_data: Spark DataFrame with columns: event_ts, device_id, lat, lon

        Returns:
            Spark DataFrame with columns: lat, lon, arrival_time, departure_time,
            duration_minutes, num_points, point_type
        """
        # Ensure event_ts is timestamp type for calculations
        df = device_data.withColumn(
            'event_ts_parsed',
            col('event_ts').cast('timestamp')
        )

        # Define window for sequential processing, partitioned by device
        window_ordered = Window.orderBy('event_ts_parsed')

        # Step 1: Add row numbers and calculate distance to previous point
        df = df.withColumn('row_num', row_number().over(window_ordered))

        df = df.withColumn('prev_lat', lag('lat').over(window_ordered))
        df = df.withColumn('prev_lon', lag('lon').over(window_ordered))

        # Calculate distance to previous point in meters
        df = df.withColumn(
            'dist_to_prev',
            when(
                col('prev_lat').isNotNull(),
                haversine_spark(col('prev_lat'), col('prev_lon'), col('lat'), col('lon'))
            ).otherwise(lit(0.0))
        )

        # Step 2: Mark boundaries where distance exceeds threshold (new group starts)
        df = df.withColumn(
            'is_new_group',
            when(col('row_num') == 1, lit(1))
            .when(col('dist_to_prev') > self.dist_threshold_m, lit(1))
            .otherwise(lit(0))
        )

        # Step 3: Create group IDs using cumulative sum of boundaries
        df = df.withColumn(
            'group_id',
            spark_sum('is_new_group').over(window_ordered)
        )

        # Step 4: Aggregate statistics for each group
        group_window = Window.partitionBy('group_id')

        df = df.withColumn('group_lat_mean', avg('lat').over(group_window))
        df = df.withColumn('group_lon_mean', avg('lon').over(group_window))
        df = df.withColumn('group_num_points', count('*').over(group_window))
        df = df.withColumn('group_arrival', spark_min('event_ts_parsed').over(group_window))
        df = df.withColumn('group_departure', spark_max('event_ts_parsed').over(group_window))

        # Calculate duration in minutes
        df = df.withColumn(
            'group_duration_minutes',
            (col('group_departure').cast('long') - col('group_arrival').cast('long')) / 60.0
        )

        # Step 5: Classify groups as stay_point or trajectory
        df = df.withColumn(
            'is_stay_point',
            (col('group_duration_minutes') >= self.time_threshold_min) &
            (col('group_num_points') > 1)
        )

        # Step 6: For stay points, get one representative row per group
        # For trajectory points, keep all individual rows

        # Mark the first row of each group (for stay point aggregation)
        group_row_window = Window.partitionBy('group_id').orderBy('event_ts_parsed')
        df = df.withColumn('row_in_group', row_number().over(group_row_window))

        # Create stay points (one per group, using aggregated values)
        stay_points = df.filter(
            (col('is_stay_point') == True) & (col('row_in_group') == 1)
        ).select(
            col('group_lat_mean').alias('lat'),
            col('group_lon_mean').alias('lon'),
            col('group_arrival').alias('arrival_time'),
            col('group_departure').alias('departure_time'),
            col('group_duration_minutes').alias('duration_minutes'),
            col('group_num_points').alias('num_points'),
            lit('stay_point').alias('point_type')
        )

        # Create trajectory points (individual points from non-stay groups)
        trajectory_points = df.filter(
            col('is_stay_point') == False
        ).select(
            col('lat'),
            col('lon'),
            col('event_ts_parsed').alias('arrival_time'),
            col('event_ts_parsed').alias('departure_time'),
            lit(0.0).alias('duration_minutes'),
            lit(1).alias('num_points'),
            lit('trajectory').alias('point_type')
        )

        # Step 7: Union stay points and trajectory points
        result_df = stay_points.union(trajectory_points)

        # Order by arrival time for consistent output
        result_df = result_df.orderBy('arrival_time')

        # Log summary statistics
        stay_count = stay_points.count()
        trajectory_count = trajectory_points.count()
        logger.info(f"Detected {stay_count} stay points and {trajectory_count} trajectory points")

        return result_df

