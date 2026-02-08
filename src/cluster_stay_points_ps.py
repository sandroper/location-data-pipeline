import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, row_number, monotonically_increasing_id,
    avg, sum as spark_sum, count, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
from utils.location_pipeline_config_manager import LocationPipelineConfig
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class ClusterStayPoints:
    """
    Cluster stay points using DBSCAN with haversine distance.

    This implementation matches the legacy sklearn DBSCAN behavior exactly by:
    1. Collecting stay points per device to the driver
    2. Running sklearn DBSCAN with haversine metric on each device's data
    3. Joining cluster labels back to the Spark DataFrame

    This approach works because:
    - Stay points are a small subset of all points
    - Per-device data is typically small (tens to hundreds of stay points)
    - DBSCAN runs on the driver, avoiding executor Python dependency issues
    """

    def __init__(self, config: LocationPipelineConfig) -> None:
        self.centroid_method: str = str(config.centroid_method)
        self.clustering_eps: int = int(config.clustering_eps)
        self.clustering_min_samples: int = int(config.clustering_min_samples)

    def cluster_stay_points(self, device_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Cluster stay points using DBSCAN with haversine distance.

        Matches legacy sklearn DBSCAN behavior exactly by processing each
        device's stay points on the driver.

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

        # Perform DBSCAN clustering on the driver (matches legacy exactly)
        clustered_stay_points = self._cluster_with_dbscan(stay_points_df)

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

    def _cluster_with_dbscan(self, stay_points_df: DataFrame) -> DataFrame:
        """
        Cluster stay points using sklearn DBSCAN with haversine distance.

        This method:
        1. Collects stay points to the driver (grouped by device)
        2. Runs sklearn DBSCAN for each device
        3. Creates a cluster mapping DataFrame
        4. Joins cluster labels back to the original DataFrame

        Uses haversine metric matching the legacy implementation exactly.
        """
        eps_meters = self.clustering_eps
        min_samples = self.clustering_min_samples

        # Convert eps from meters to radians for haversine
        eps_rad = eps_meters / 6371000.0  # Earth radius in meters

        # Drop existing cluster_id column if present
        if 'cluster_id' in stay_points_df.columns:
            stay_points_df = stay_points_df.drop('cluster_id')

        # Get the SparkSession
        spark = stay_points_df.sparkSession

        # Collect stay points to driver, grouped by device
        # Select only the columns we need for clustering
        columns_to_keep = stay_points_df.columns
        clustering_cols = ['device_id', 'point_id', 'lat', 'lon']

        # Collect to pandas for processing
        logger.info("Collecting stay points to driver for DBSCAN clustering...")
        stay_points_pd = stay_points_df.select(clustering_cols).toPandas()

        if len(stay_points_pd) == 0:
            logger.warning("No stay points to cluster")
            return stay_points_df.withColumn('cluster_id', lit(-1).cast(LongType()))

        # Run DBSCAN for each device
        cluster_results = []
        devices = stay_points_pd['device_id'].unique()

        total_clusters = 0
        total_noise = 0

        for device_id in devices:
            device_points = stay_points_pd[stay_points_pd['device_id'] == device_id].copy()

            if len(device_points) == 0:
                continue

            # Extract coordinates and convert to radians
            coords = device_points[['lat', 'lon']].to_numpy()
            coords_rad = np.radians(coords)

            # Run DBSCAN with haversine metric (matches legacy exactly)
            db = DBSCAN(eps=eps_rad, min_samples=min_samples, metric='haversine')
            labels = db.fit_predict(coords_rad)

            # Count clusters and noise for this device
            unique_labels = set(labels)
            n_clusters = len([l for l in unique_labels if l >= 0])
            n_noise = list(labels).count(-1)
            total_clusters += n_clusters
            total_noise += n_noise

            # Store results
            for point_id, cluster_id in zip(device_points['point_id'], labels):
                cluster_results.append({
                    'point_id': int(point_id),
                    'cluster_id': int(cluster_id)
                })

        logger.info(f"DBSCAN clustering found {total_clusters} clusters, {total_noise} noise points across {len(devices)} devices")

        # Create a Spark DataFrame with cluster assignments
        if cluster_results:
            cluster_schema = StructType([
                StructField('point_id', LongType(), False),
                StructField('cluster_id', LongType(), False)
            ])
            cluster_mapping_df = spark.createDataFrame(cluster_results, schema=cluster_schema)

            # Join cluster labels back to the original DataFrame
            result_df = stay_points_df.join(
                broadcast(cluster_mapping_df),
                'point_id',
                'left'
            )
        else:
            result_df = stay_points_df.withColumn('cluster_id', lit(-1).cast(LongType()))

        return result_df

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
