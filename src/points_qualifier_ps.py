import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lag, when, lit, sum as spark_sum, first, last,
    min as spark_min, max as spark_max, avg, count,
    row_number, monotonically_increasing_id, expr
)
from pyspark.sql.window import Window
from utils.location_pipeline_config_manager import LocationPipelineConfig
from utils.time_zone_utils import get_timezone_offset

logger = logging.getLogger(__name__)


def sedona_distance_m(lat1_col: str, lon1_col: str, lat2_col: str, lon2_col: str):
    """
    Calculate geodesic distance between two points in meters using Sedona.
    Uses ST_DistanceSpheroid for WGS84 ellipsoid accuracy.
    """
    return expr(f"""
        CASE
            WHEN {lat1_col} IS NULL OR {lon1_col} IS NULL OR {lat2_col} IS NULL OR {lon2_col} IS NULL
            THEN NULL
            ELSE ST_DistanceSpheroid(
                ST_Point(CAST({lon1_col} AS DOUBLE), CAST({lat1_col} AS DOUBLE)),
                ST_Point(CAST({lon2_col} AS DOUBLE), CAST({lat2_col} AS DOUBLE))
            )
        END
    """)


class PointsQualifier:
    """
    Classifies location points as stay points or trajectory points.

    Uses anchor-based distance checking matching the legacy algorithm:
    - A group continues as long as all points are within dist_threshold_m
      of the FIRST point (anchor) of the group
    - When a point exceeds the threshold from the anchor, a new group starts

    Implementation uses iterative Spark SQL operations to simulate the
    sequential anchor-based algorithm without requiring Python UDFs on workers.
    """

    def __init__(self, config: LocationPipelineConfig):
        self.dist_threshold_m: int = int(config.dist_threshold_m)
        self.time_threshold_min: int = int(config.time_threshold_min)
        self.time_zone: str = str(config.time_zone)
        self.TIME_ZONE_OFFSET_HOURS = get_timezone_offset(self.time_zone)

    def get_stay_points(self, device_data: DataFrame) -> DataFrame:
        """
        Identify stay points and trajectory points using anchor-based algorithm.

        This implementation matches the legacy PointClassifier algorithm:
        - Distance is measured from the FIRST point of a potential group (anchor)
        - A group continues while points remain within dist_threshold_m of anchor
        - Stay points are created when duration >= time_threshold_min

        Args:
            device_data: Spark DataFrame with columns: event_ts, device_id, lat, lon

        Returns:
            Spark DataFrame with columns: device_id, lat, lon, arrival_time,
            departure_time, duration_minutes, num_points, point_type
        """
        # Ensure event_ts is timestamp type
        df = device_data.withColumn(
            'event_ts_parsed',
            col('event_ts').cast('timestamp')
        )

        # Add row numbers for ordering within each device
        window_ordered = Window.partitionBy('device_id').orderBy('event_ts_parsed')
        df = df.withColumn('row_num', row_number().over(window_ordered))

        # Step 1: Iteratively identify group anchors
        # An anchor is the first point of a new group
        df = self._identify_anchors_iteratively(df, window_ordered)

        # Step 2: Assign group IDs based on anchors
        df = df.withColumn(
            'group_id',
            spark_sum(col('is_anchor').cast('int')).over(window_ordered)
        )

        # Step 3: Calculate group statistics
        group_window = Window.partitionBy('device_id', 'group_id')

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

        # Step 4: Classify groups as stay_point or trajectory
        df = df.withColumn(
            'is_stay_point',
            (col('group_duration_minutes') >= self.time_threshold_min) &
            (col('group_num_points') > 1)
        )

        # Step 5: Create output - one row per stay point, individual rows for trajectory
        group_row_window = Window.partitionBy('device_id', 'group_id').orderBy('event_ts_parsed')
        df = df.withColumn('row_in_group', row_number().over(group_row_window))

        # Stay points (one per group, using aggregated values)
        stay_points = df.filter(
            (col('is_stay_point') == True) & (col('row_in_group') == 1)
        ).select(
            col('device_id'),
            col('group_lat_mean').alias('lat'),
            col('group_lon_mean').alias('lon'),
            col('group_arrival').alias('arrival_time'),
            col('group_departure').alias('departure_time'),
            col('group_duration_minutes').alias('duration_minutes'),
            col('group_num_points').cast('int').alias('num_points'),
            lit('stay_point').alias('point_type')
        )

        # Trajectory points (individual points from non-stay groups)
        trajectory_points = df.filter(
            col('is_stay_point') == False
        ).select(
            col('device_id'),
            col('lat'),
            col('lon'),
            col('event_ts_parsed').alias('arrival_time'),
            col('event_ts_parsed').alias('departure_time'),
            lit(0.0).alias('duration_minutes'),
            lit(1).alias('num_points'),
            lit('trajectory').alias('point_type')
        )

        # Union and order results
        result_df = stay_points.union(trajectory_points)
        result_df = result_df.orderBy('device_id', 'arrival_time')

        # Log summary statistics
        stay_count = stay_points.count()
        trajectory_count = trajectory_points.count()
        logger.info(f"Detected {stay_count} stay points and {trajectory_count} trajectory points")

        return result_df

    def _identify_anchors_iteratively(self, df: DataFrame, window_ordered: Window) -> DataFrame:
        """
        Iteratively identify anchor points for groups.

        The anchor-based algorithm requires knowing the anchor to check if a point
        exceeds the threshold. This is done iteratively:
        1. Start with first point of each device as anchor
        2. Propagate anchor coordinates forward
        3. Check distance from anchor; if exceeded, mark as new anchor
        4. Repeat until no new anchors are found

        Uses Sedona ST_DistanceSpheroid for geodesic distance calculation.
        Uses cache + count to materialize and break query lineage.
        """
        dist_threshold = self.dist_threshold_m

        # Initialize: first point of each device is an anchor
        df = df.withColumn(
            'is_anchor',
            when(col('row_num') == 1, lit(True)).otherwise(lit(False))
        )
        df = df.withColumn(
            'anchor_lat',
            when(col('is_anchor'), col('lat')).otherwise(lit(None))
        )
        df = df.withColumn(
            'anchor_lon',
            when(col('is_anchor'), col('lon')).otherwise(lit(None))
        )

        # Materialize initial state by caching and forcing evaluation
        df.cache()
        df.count()

        # Iteratively propagate anchors and find new ones
        max_iterations = 1000  # Safety limit
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            # Propagate anchor coordinates forward (fill nulls with last known anchor)
            # Using last() with ignorenulls to carry forward anchor position
            anchor_window = Window.partitionBy('device_id').orderBy('row_num').rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )

            df = df.withColumn(
                'current_anchor_lat',
                last('anchor_lat', ignorenulls=True).over(anchor_window)
            )
            df = df.withColumn(
                'current_anchor_lon',
                last('anchor_lon', ignorenulls=True).over(anchor_window)
            )

            # Calculate distance from current anchor
            df = df.withColumn(
                'dist_from_anchor',
                sedona_distance_m('lat', 'lon', 'current_anchor_lat', 'current_anchor_lon')
            )

            # Find points that should be new anchors:
            # - Not already an anchor
            # - Distance from current anchor exceeds threshold
            # - Previous point was still within threshold (first point to exceed)
            df = df.withColumn(
                'prev_dist',
                lag('dist_from_anchor').over(window_ordered)
            )

            df = df.withColumn(
                'should_be_anchor',
                (~col('is_anchor')) &
                (col('dist_from_anchor') > dist_threshold) &
                ((col('prev_dist').isNull()) | (col('prev_dist') <= dist_threshold))
            )

            # Count new anchors found
            new_anchor_count = df.filter(col('should_be_anchor')).count()

            if new_anchor_count == 0:
                # No new anchors - we're done
                break

            logger.info(f"Anchor iteration {iteration}: found {new_anchor_count} new anchors")

            # Update anchors
            df = df.withColumn(
                'is_anchor',
                col('is_anchor') | col('should_be_anchor')
            )
            df = df.withColumn(
                'anchor_lat',
                when(col('is_anchor'), col('lat')).otherwise(lit(None))
            )
            df = df.withColumn(
                'anchor_lon',
                when(col('is_anchor'), col('lon')).otherwise(lit(None))
            )

            # Clean up intermediate columns
            df = df.drop('current_anchor_lat', 'current_anchor_lon', 'dist_from_anchor',
                        'prev_dist', 'should_be_anchor')

            # Materialize to break query lineage - use localCheckpoint for in-memory
            df = df.localCheckpoint(eager=True)

        if iteration >= max_iterations:
            logger.warning(f"Anchor identification reached max iterations ({max_iterations})")

        # Final cleanup
        columns_to_drop = ['anchor_lat', 'anchor_lon']
        existing_cols = [c for c in columns_to_drop if c in df.columns]
        if existing_cols:
            df = df.drop(*existing_cols)

        return df
