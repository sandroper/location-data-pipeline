from utils.location_pipeline_config_manager import LocationPipelineConfig 
import numpy as np
import pandas as pd
import logging
from sklearn.cluster import DBSCAN

class ClusterStayPoints:
    def __init__(self, config: LocationPipelineConfig) -> None:
        self.centroid_method: str = str(config.centroid_method) 
        self.clustering_eps: int = int(config.clustering_eps)
        self.clustering_min_samples: int = int(config.clustering_min_samples)


    def cluster_stay_points(self, device_data: pd.DataFrame):
        df_complete = device_data.copy()

        # Initialize cluster_id and centroid columns
        df_complete['cluster_id'] = np.nan
        df_complete['centroid_lat'] = np.nan
        df_complete['centroid_lon'] = np.nan

        if 'point_type' in df_complete.columns:
            # Enhanced format - filter only stay_points
            stay_points_mask = df_complete['point_type'] == 'stay_point'
            stay_points_df = df_complete[stay_points_mask].copy()
            logging.info(f"Enhanced format detected: {len(stay_points_df)} stay_points, {len(df_complete) - len(stay_points_df)} trajectory points")
        else:
            # Legacy format - all points are stay points
            stay_points_mask = df_complete.index
            stay_points_df = df_complete.copy()
            logging.info(f"Legacy format detected: {len(stay_points_df)} stay_points")

        if len(stay_points_df) == 0:
            logging.warning("Warning: No stay_points found in the dataset")
            return df_complete, np.array([])

        coords = stay_points_df[['lat', 'lon']].to_numpy()
        coords_rad = np.radians(coords)

        # Perform DBSCAN clustering
        # eps_rad = 100/ 6371000  # 100 meters in radians
        eps_rad = self.clustering_eps / 6371000
        db = DBSCAN(eps=eps_rad, min_samples=self.clustering_min_samples, metric='haversine')
        labels = db.fit_predict(coords_rad)

        # Add cluster information to stay_points
        if 'point_type' in df_complete.columns:
            # assign to stay_points only
            stay_points_indices = df_complete[stay_points_mask].index
            df_complete.loc[stay_points_indices, 'cluster_id'] = labels
        else:
            # Legacy format - assign to all points
            df_complete['cluster_id'] = labels

        # Calculate centroids for each cluster using specified method
        print("======= pd.Series(labels).unique: " + str(pd.Series(labels).unique()))
        for cluster_id in sorted(pd.Series(labels).unique()):
            print("======= Cluster ID: " + str(cluster_id))
            cluster_mask = df_complete['cluster_id'] == cluster_id
            print("======= Cluster mask: " + str(cluster_mask))
            cluster_data = df_complete[cluster_mask]

            if len(cluster_data) > 0:
                if self.centroid_method == 'average':
                    # Simple mean of coordinates
                    centroid_lat = cluster_data['lat'].mean()
                    centroid_lon = cluster_data['lon'].mean()

                elif self.centroid_method == 'weighted_average':
                    # Weighted average using num_points as weights
                    total_weight = cluster_data['num_points'].sum()
                    print("=============== Total weight: ", total_weight)
                    if total_weight > 0:
                        centroid_lat = (cluster_data['lat'] * cluster_data['num_points']).sum() / total_weight
                        centroid_lon = (cluster_data['lon'] * cluster_data['num_points']).sum() / total_weight
                        print("=============== Centroid Lat: ", centroid_lat)
                        print("=============== Centroid Lon: ", centroid_lon)
                    else:
                        # Fallback to simple mean if all weights are zero
                        centroid_lat = cluster_data['lat'].mean()
                        centroid_lon = cluster_data['lon'].mean()

                elif self.centroid_method == 'max_points':
                    # Use coordinates of the point with maximum num_points
                    max_points_idx = cluster_data['num_points'].idxmax()
                    centroid_lat = cluster_data.loc[max_points_idx, 'lat']
                    centroid_lon = cluster_data.loc[max_points_idx, 'lon']

                else:
                    raise ValueError(
                        f"Invalid centroid_method: {self.centroid_method}. Must be one of: 'average', 'weighted_average', 'max_points'")

                print("======= Cluster mask: " + str(cluster_mask))
                print("======= Centroid Lat: " + str(centroid_lat))
                print("======= Centroid Lon: " + str(centroid_lon))
                df_complete.loc[cluster_mask, 'centroid_lat'] = centroid_lat
                df_complete.loc[cluster_mask, 'centroid_lon'] = centroid_lon

        return df_complete, labels
        
        
        
