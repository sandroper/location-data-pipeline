import os
import logging
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    logger.info(f"Loaded environment variables from {env_path}")
else:
    # Fallback: try to load from current directory
    load_dotenv()

class LocationPipelineConfig():

    max_distance_threshold_km: int = os.getenv("MAX_DISTANCE_THRESHOLD_KM", 5)
    min_distance_threshold_km: int = os.getenv("MIN_DISTANCE_THRESHOLD_KM", 1)
    max_speed_kmh: int = os.getenv("MAX_SPEED_KMH", 200)
    
    dist_threshold_m: int = os.getenv("DIST_THRESHOLD_M", 100) 
    time_threshold_min: int = os.getenv("TIME_THRESHOLD_MIN", 10) 

    time_zone: str = os.getenv("TIME_ZONE", "UTC")

    clustering_eps: int = os.getenv("CLUSTERING_EPS", 100)
    clustering_min_samples: int = os.getenv("CLUSTERING_MIN_SAMPLES", 1)
    centroid_method: str = os.getenv("CENTROID_METHOD", "weighted_average")

    osrm_server_driving: str = os.getenv("OSRM_SERVER", "http://127.0.0.1:5000")
    osrm_server_foot: str = os.getenv("OSRM_SERVER", "http://127.0.0.1:5001")

    output_data_dir = os.getenv("OUTPUT_DATA_DIR", "./data/output")
    save_routes_json: bool = os.getenv("SAVE_ROUTES_JSON", "false").lower() == "true"

    def __str__(self) -> str:
        return (
            f"LocationPipelineConfig(\n"
            f"  max_distance_threshold_km={self.max_distance_threshold_km},\n"
            f"  min_distance_threshold_km={self.min_distance_threshold_km},\n"
            f"  max_speed_kmh={self.max_speed_kmh},\n"
            f"  dist_threshold_m={self.dist_threshold_m},\n"
            f"  time_threshold_min={self.time_threshold_min},\n"
            f"  clustering_eps={self.clustering_eps},\n"
            f"  clustering_min_samples={self.clustering_min_samples},\n"
            f"  centroid_method={self.centroid_method}\n"
            f"  osrm_server_driving={self.osrm_server_driving}\n"
            f"  osrm_server_foot={self.osrm_server_foot}\n"
            f")"
        )