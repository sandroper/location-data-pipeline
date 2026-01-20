from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, row_number, monotonically_increasing_id,
    avg, sum as spark_sum, broadcast, radians, cos, sin, sqrt, asin
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import BisectingKMeans
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
        Cluster stay points using DBSCAN via applyInPandas for distributed processing.

        Each device's stay points are clustered independently in parallel across
        Spark workers using sklearn's DBSCAN.

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

        # Perform clustering using Spark MLlib BisectingKMeans
        clustered_stay_points = self._cluster_with_bisecting_kmeans(stay_points_df)

        # Calculate centroids for each cluster
        clustered_with_centroids = self._calculate_centroids(clustered_stay_points)

        # Combine clustered stay points with trajectory points
        if trajectory_df is not None:
            result_df = clustered_with_centroids.union(
                trajectory_df.select(clustered_with_centroids.columns)
            )
        else:
            result_df = clustered_with_centroids

        # Order by arrival_time for consistent output
        if 'arrival_time' in result_df.columns:
            result_df = result_df.orderBy('arrival_time')

        # Extract labels for return
        labels_df = clustered_with_centroids.select('point_id', 'cluster_id')

        return result_df, labels_df

    def _cluster_with_bisecting_kmeans(self, stay_points_df: DataFrame) -> DataFrame:
        """
        Cluster stay points using Spark MLlib's BisectingKMeans.

        This is a pure Spark solution that scales to large datasets.
        BisectingKMeans is a hierarchical clustering algorithm that works well
        for geographic clustering.

        The number of clusters is estimated based on the eps parameter:
        - Points within eps meters should ideally be in the same cluster
        - We estimate k based on the geographic spread of the data
        """
        eps_meters = self.clustering_eps
        min_samples = self.clustering_min_samples

        # Drop existing cluster_id column if present (will be recreated by BisectingKMeans)
        if 'cluster_id' in stay_points_df.columns:
            stay_points_df = stay_points_df.drop('cluster_id')

        # Convert lat/lon to Cartesian coordinates for better distance calculations
        # This provides more accurate clustering than using raw lat/lon
        earth_radius_m = 6371000

        stay_points_df = stay_points_df.withColumn(
            'lat_rad', radians(col('lat'))
        ).withColumn(
            'lon_rad', radians(col('lon'))
        ).withColumn(
            # Convert to 3D Cartesian coordinates
            'x', cos(col('lat_rad')) * cos(col('lon_rad')) * earth_radius_m
        ).withColumn(
            'y', cos(col('lat_rad')) * sin(col('lon_rad')) * earth_radius_m
        ).withColumn(
            'z', sin(col('lat_rad')) * earth_radius_m
        )

        # Create feature vector for clustering
        assembler = VectorAssembler(
            inputCols=['x', 'y', 'z'],
            outputCol='features'
        )
        feature_df = assembler.transform(stay_points_df)

        # Estimate number of clusters based on data spread and eps
        # Get approximate bounds to estimate k
        bounds = stay_points_df.agg(
            spark_sum(lit(1)).alias('count'),
            avg('lat').alias('avg_lat'),
            avg('lon').alias('avg_lon')
        ).collect()[0]

        point_count = bounds['count']

        # Estimate k: assume points spread over area, eps defines cluster radius
        # This is a heuristic - adjust based on your data characteristics
        # More conservative estimate to avoid over-clustering
        estimated_k = max(2, min(point_count // min_samples, int(point_count ** 0.5)))

        logger.info(f"Clustering {point_count} points with estimated k={estimated_k}")

        # Apply BisectingKMeans
        bisecting_kmeans = BisectingKMeans(
            k=estimated_k,
            featuresCol='features',
            predictionCol='cluster_id',
            minDivisibleClusterSize=min_samples,
            seed=42
        )

        model = bisecting_kmeans.fit(feature_df)
        clustered_df = model.transform(feature_df)

        # Post-process: mark small clusters as noise (-1)
        # Count points per cluster
        cluster_counts = clustered_df.groupBy('cluster_id').count()

        # Clusters with fewer than min_samples are noise
        noise_clusters = cluster_counts.filter(col('count') < min_samples) \
            .select('cluster_id')

        # Join to mark noise points
        clustered_df = clustered_df.join(
            broadcast(noise_clusters.withColumn('is_noise', lit(True))),
            'cluster_id',
            'left'
        ).withColumn(
            'cluster_id',
            when(col('is_noise') == True, lit(-1)).otherwise(col('cluster_id'))
        ).drop('is_noise')

        # Clean up temporary columns
        clustered_df = clustered_df.drop('lat_rad', 'lon_rad', 'x', 'y', 'z', 'features')

        # Log clustering results
        num_clusters = clustered_df.filter(col('cluster_id') != -1) \
            .select('cluster_id').distinct().count()
        noise_count = clustered_df.filter(col('cluster_id') == -1).count()
        logger.info(f"BisectingKMeans found {num_clusters} clusters, {noise_count} noise points")

        return clustered_df

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
        
        
        
