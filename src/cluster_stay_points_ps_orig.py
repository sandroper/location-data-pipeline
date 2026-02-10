from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, row_number, monotonically_increasing_id,
    avg, sum as spark_sum, count, broadcast, floor, concat_ws,
    dense_rank, first
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType
from utils.location_pipeline_config_manager import LocationPipelineConfig
import logging

logger = logging.getLogger(__name__)


class ClusterStayPoints:
    def __init__(self, config: LocationPipelineConfig) -> None:
        self.centroid_method: str = str(config.centroid_method)
        self.clustering_eps: int = int(config.clustering_eps)
        self.clustering_min_samples: int = int(config.clustering_min_samples)

    def cluster_stay_points(self, device_data: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Cluster stay points using grid-based spatial clustering.

        Pure Spark implementation that clusters each device's stay points
        independently using partitionBy('device_id').

        Args:
            device_data: Spark DataFrame with columns including device_id, lat, lon, point_type

        Returns:
            Tuple of (clustered DataFrame, cluster labels DataFrame)
        """
        # Add unique point IDs
        df_complete = device_data.withColumn('point_id', monotonically_increasing_id())

        # Initialize cluster columns with null
        df_complete = df_complete.withColumn('cluster_id', lit(None).cast(LongType()))
        df_complete = df_complete.withColumn('centroid_lat', lit(None).cast(DoubleType()))
        df_complete = df_complete.withColumn('centroid_lon', lit(None).cast(DoubleType()))

        # Check if point_type column exists and filter stay points
        if 'point_type' in df_complete.columns:
            stay_points_df = df_complete.filter(col('point_type') == 'stay_point')
            trajectory_df = df_complete.filter(col('point_type') != 'stay_point')
            stay_count = stay_points_df.count()
            traj_count = trajectory_df.count()
            logger.info(f"Enhanced format detected: {stay_count} stay_points, {traj_count} trajectory points")
        else:
            stay_points_df = df_complete
            trajectory_df = None
            logger.info(f"Legacy format detected: {stay_points_df.count()} stay_points")

        if stay_points_df.count() == 0:
            logger.warning("Warning: No stay_points found in the dataset")
            labels_df = stay_points_df.select('point_id', lit(-1).alias('cluster_id'))
            return df_complete, labels_df

        # Perform grid-based clustering (pure Spark, partitioned by device_id)
        clustered_stay_points = self._cluster_with_grid(stay_points_df)

        # Calculate centroids for each cluster
        clustered_with_centroids = self._calculate_centroids(clustered_stay_points)

        # Combine clustered stay points with trajectory points
        if trajectory_df is not None:
            result_df = clustered_with_centroids.union(
                trajectory_df.select(clustered_with_centroids.columns)
            )
        else:
            result_df = clustered_with_centroids

        # Order by device_id and arrival_time for consistent output
        if 'arrival_time' in result_df.columns:
            result_df = result_df.orderBy('device_id', 'arrival_time')

        # Extract labels for return
        labels_df = clustered_with_centroids.select('point_id', 'cluster_id')

        return result_df, labels_df

    def _cluster_with_grid(self, stay_points_df: DataFrame) -> DataFrame:
        """
        Cluster stay points using grid-based spatial clustering.

        This is a pure Spark implementation that:
        1. Assigns each point to a grid cell based on lat/lon
        2. Groups points in the same cell as a cluster
        3. Filters clusters with fewer than min_samples as noise
        4. All operations are partitioned by device_id

        The grid cell size is determined by clustering_eps (in meters).
        """
        eps_meters = self.clustering_eps
        min_samples = self.clustering_min_samples

        # Drop existing cluster_id column if present
        if 'cluster_id' in stay_points_df.columns:
            stay_points_df = stay_points_df.drop('cluster_id')

        # Calculate grid cell size in degrees
        # At equator: 1 degree latitude ≈ 111,000 meters
        # 1 degree longitude ≈ 111,000 * cos(lat) meters
        # Using a simplified approximation for grid cell size
        meters_per_degree = 111000.0
        cell_size_deg = eps_meters / meters_per_degree

        # Assign each point to a grid cell
        stay_points_df = stay_points_df.withColumn(
            'grid_lat',
            floor(col('lat') / cell_size_deg)
        ).withColumn(
            'grid_lon',
            floor(col('lon') / cell_size_deg)
        ).withColumn(
            'grid_cell',
            concat_ws('_', col('grid_lat').cast('string'), col('grid_lon').cast('string'))
        )

        # Create cluster IDs per device based on grid cells
        # Points in the same grid cell (for the same device) get the same cluster
        if 'device_id' in stay_points_df.columns:
            # Create unique cluster ID per device and grid cell
            cluster_window = Window.partitionBy('device_id').orderBy('grid_cell')
            stay_points_df = stay_points_df.withColumn(
                'cluster_id',
                dense_rank().over(cluster_window) - 1
            )

            # Count points per cluster (per device)
            cluster_count_window = Window.partitionBy('device_id', 'cluster_id')
        else:
            cluster_window = Window.orderBy('grid_cell')
            stay_points_df = stay_points_df.withColumn(
                'cluster_id',
                dense_rank().over(cluster_window) - 1
            )
            cluster_count_window = Window.partitionBy('cluster_id')

        # Add cluster size
        stay_points_df = stay_points_df.withColumn(
            'cluster_size',
            count('*').over(cluster_count_window)
        )

        # Mark clusters with fewer than min_samples as noise (-1)
        stay_points_df = stay_points_df.withColumn(
            'cluster_id',
            when(col('cluster_size') < min_samples, lit(-1))
            .otherwise(col('cluster_id'))
        )

        # Renumber valid clusters to be consecutive (0, 1, 2, ...)
        # Get distinct valid clusters per device and assign new IDs
        if 'device_id' in stay_points_df.columns:
            valid_clusters = stay_points_df.filter(col('cluster_id') >= 0) \
                .select('device_id', 'cluster_id').distinct()

            renumber_window = Window.partitionBy('device_id').orderBy('cluster_id')
            cluster_mapping = valid_clusters.withColumn(
                'new_cluster_id',
                row_number().over(renumber_window) - 1
            )

            stay_points_df = stay_points_df.join(
                broadcast(cluster_mapping),
                ['device_id', 'cluster_id'],
                'left'
            ).withColumn(
                'cluster_id',
                when(col('cluster_id') == -1, lit(-1))
                .otherwise(col('new_cluster_id'))
            ).drop('new_cluster_id')
        else:
            valid_clusters = stay_points_df.filter(col('cluster_id') >= 0) \
                .select('cluster_id').distinct()

            renumber_window = Window.orderBy('cluster_id')
            cluster_mapping = valid_clusters.withColumn(
                'new_cluster_id',
                row_number().over(renumber_window) - 1
            )

            stay_points_df = stay_points_df.join(
                broadcast(cluster_mapping),
                'cluster_id',
                'left'
            ).withColumn(
                'cluster_id',
                when(col('cluster_id') == -1, lit(-1))
                .otherwise(col('new_cluster_id'))
            ).drop('new_cluster_id')

        # Clean up temporary columns
        stay_points_df = stay_points_df.drop('grid_lat', 'grid_lon', 'grid_cell', 'cluster_size')

        # Log clustering results
        if 'device_id' in stay_points_df.columns:
            num_clusters = stay_points_df.filter(col('cluster_id') >= 0) \
                .select('device_id', 'cluster_id').distinct().count()
        else:
            num_clusters = stay_points_df.filter(col('cluster_id') >= 0) \
                .select('cluster_id').distinct().count()

        noise_count = stay_points_df.filter(col('cluster_id') == -1).count()
        logger.info(f"Grid clustering found {num_clusters} clusters, {noise_count} noise points")

        return stay_points_df

    def _calculate_centroids(self, clustered_df: DataFrame) -> DataFrame:
        """
        Calculate centroids for each cluster using the specified method.
        Uses window functions partitioned by device_id and cluster_id.
        """
        # Define window partitioned by device and cluster
        if 'device_id' in clustered_df.columns:
            cluster_window = Window.partitionBy('device_id', 'cluster_id')
        else:
            cluster_window = Window.partitionBy('cluster_id')

        if self.centroid_method == 'average':
            # Simple mean of coordinates
            clustered_df = clustered_df.withColumn(
                'centroid_lat',
                when(col('cluster_id') != -1, avg('lat').over(cluster_window))
            ).withColumn(
                'centroid_lon',
                when(col('cluster_id') != -1, avg('lon').over(cluster_window))
            )

        elif self.centroid_method == 'weighted_average':
            # Weighted average using num_points as weights
            clustered_df = clustered_df.withColumn(
                'weighted_lat',
                col('lat') * col('num_points')
            ).withColumn(
                'weighted_lon',
                col('lon') * col('num_points')
            )

            clustered_df = clustered_df.withColumn(
                'total_weight',
                spark_sum('num_points').over(cluster_window)
            ).withColumn(
                'sum_weighted_lat',
                spark_sum('weighted_lat').over(cluster_window)
            ).withColumn(
                'sum_weighted_lon',
                spark_sum('weighted_lon').over(cluster_window)
            )

            clustered_df = clustered_df.withColumn(
                'centroid_lat',
                when(
                    (col('cluster_id') != -1) & (col('total_weight') > 0),
                    col('sum_weighted_lat') / col('total_weight')
                ).otherwise(
                    when(col('cluster_id') != -1, avg('lat').over(cluster_window))
                )
            ).withColumn(
                'centroid_lon',
                when(
                    (col('cluster_id') != -1) & (col('total_weight') > 0),
                    col('sum_weighted_lon') / col('total_weight')
                ).otherwise(
                    when(col('cluster_id') != -1, avg('lon').over(cluster_window))
                )
            )

            # Drop intermediate columns
            clustered_df = clustered_df.drop(
                'weighted_lat', 'weighted_lon', 'total_weight',
                'sum_weighted_lat', 'sum_weighted_lon'
            )

        elif self.centroid_method == 'max_points':
            # Use coordinates of the point with maximum num_points in each cluster
            if 'device_id' in clustered_df.columns:
                max_points_window = Window.partitionBy('device_id', 'cluster_id') \
                    .orderBy(col('num_points').desc())
            else:
                max_points_window = Window.partitionBy('cluster_id') \
                    .orderBy(col('num_points').desc())

            clustered_df = clustered_df.withColumn(
                'rank_by_points',
                row_number().over(max_points_window)
            )

            # Get the lat/lon of the max-points row for each cluster
            if 'device_id' in clustered_df.columns:
                max_point_centroids = clustered_df.filter(
                    (col('rank_by_points') == 1) & (col('cluster_id') != -1)
                ).select(
                    col('device_id').alias('d_id'),
                    col('cluster_id').alias('c_id'),
                    col('lat').alias('max_centroid_lat'),
                    col('lon').alias('max_centroid_lon')
                )

                clustered_df = clustered_df.join(
                    broadcast(max_point_centroids),
                    (clustered_df.device_id == max_point_centroids.d_id) &
                    (clustered_df.cluster_id == max_point_centroids.c_id),
                    'left'
                ).withColumn(
                    'centroid_lat',
                    when(col('cluster_id') != -1, col('max_centroid_lat'))
                ).withColumn(
                    'centroid_lon',
                    when(col('cluster_id') != -1, col('max_centroid_lon'))
                ).drop('d_id', 'c_id', 'max_centroid_lat', 'max_centroid_lon', 'rank_by_points')
            else:
                max_point_centroids = clustered_df.filter(
                    (col('rank_by_points') == 1) & (col('cluster_id') != -1)
                ).select(
                    col('cluster_id').alias('c_id'),
                    col('lat').alias('max_centroid_lat'),
                    col('lon').alias('max_centroid_lon')
                )

                clustered_df = clustered_df.join(
                    broadcast(max_point_centroids),
                    clustered_df.cluster_id == max_point_centroids.c_id,
                    'left'
                ).withColumn(
                    'centroid_lat',
                    when(col('cluster_id') != -1, col('max_centroid_lat'))
                ).withColumn(
                    'centroid_lon',
                    when(col('cluster_id') != -1, col('max_centroid_lon'))
                ).drop('c_id', 'max_centroid_lat', 'max_centroid_lon', 'rank_by_points')

        else:
            raise ValueError(
                f"Invalid centroid_method: {self.centroid_method}. "
                f"Must be one of: 'average', 'weighted_average', 'max_points'"
            )

        return clustered_df
        
        
        
