import os
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from utils.location_pipeline_config_manager import LocationPipelineConfig
from utils.osrm.osrm_map_data_dumper import OSRMMapDataDumper

logger = logging.getLogger(__name__)


class OSRMRoutesWriter:
    """
    Handles writing route prediction data to JSON files.

    Creates one JSON file per device_id with the structure:
    {
        "metadata": { "device_id": "...", ... },
        "routes": [...],
        "stay_points": [...],
        "trajectory_points": [...]
    }
    """

    def __init__(self, config: LocationPipelineConfig):
        self.config = config
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        """Create output directory if it doesn't exist."""
        Path(self.config.output_data_dir).mkdir(parents=True, exist_ok=True)

    def save_device_routes(self, device_id: str, device_data: Dict[str, Any]) -> str:
        """
        Save route prediction data for a single device to a JSON file.

        Args:
            device_id: The device identifier
            device_data: Dictionary containing routes, stay_points, trajectory_points, and metadata info

        Returns:
            Path to the saved JSON file
        """
        start_date = device_data.get('start_date', '')
        end_date = device_data.get('end_date', '')
        prediction_data = {
            'metadata': {
                'device_id': device_id,
                'start_date': str(start_date),
                'end_date': str(end_date),
                'start_time': self._format_time(device_data.get('start_time')),
                'end_time': self._format_time(device_data.get('end_time')),
                'total_distance_km': round(device_data.get('total_distance', 0), 3),
                'total_journey_time': device_data.get('total_journey_time', ''),
                'total_segments': len(device_data.get('routes', [])),
                'total_stay_points': len(device_data.get('stay_points', [])),
                'total_trajectory_points': len(device_data.get('trajectory_points', [])),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'routing_engine': 'OSRM',
                'matching_mode': 'pairwise'
            },
            'routes': device_data.get('routes', []),
            'stay_points': device_data.get('stay_points', []),
            'trajectory_points': device_data.get('trajectory_points', [])
        }

        output_filename = f"predicted_routes_osrm_pairwise_{device_id}.json"
        output_path = os.path.join(self.config.output_data_dir, output_filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(prediction_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Routes data for device {device_id} saved to: {output_path}")

        # Create interactive HTML map visualization
        try:
            points_df = self._reconstruct_points_dataframe(device_data)
            if points_df is not None and len(points_df) >= 2:
                map_dumper = OSRMMapDataDumper(
                    device_id, points_df, start_date, end_date, config=self.config
                )
                map_dumper.create_enhanced_interactive_route_map()
            else:
                logger.warning(f"Insufficient points to create map for device {device_id}")
        except Exception as e:
            logger.error(f"Failed to create map for device {device_id}: {e}")
        return output_path

    def save_all_devices(self, all_device_results: Dict[str, Dict[str, Any]]) -> List[str]:
        """
        Save route prediction data for all devices.

        Args:
            all_device_results: Dictionary mapping device_id to device data

        Returns:
            List of paths to saved JSON files
        """
        saved_paths = []
        for device_id, device_data in all_device_results.items():
            path = self.save_device_routes(device_id, device_data)
            saved_paths.append(path)

        logger.info(f"Saved route data for {len(saved_paths)} devices")
        return saved_paths

    @staticmethod
    def _format_time(time_val) -> str:
        """Format time value to string."""
        if time_val is None:
            return ''
        if hasattr(time_val, 'strftime'):
            return time_val.strftime('%Y-%m-%d %H:%M:%S')
        return str(time_val)

    @staticmethod
    def _reconstruct_points_dataframe(device_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Reconstruct a DataFrame from processed device data.

        Combines stay_points and trajectory_points into a single DataFrame
        that can be used by OSRMMapDataDumper for visualization.

        Args:
            device_data: Dictionary containing stay_points and trajectory_points

        Returns:
            DataFrame with columns: lat, lon, point_type, arrival_time, departure_time, cluster_id, etc.
        """
        all_points = []

        # Add stay points
        for sp in device_data.get('stay_points', []):
            point = {
                'lat': sp['lat'],
                'lon': sp['lon'],
                'point_type': 'stay_point',
                'arrival_time': pd.to_datetime(sp['arrival_time']),
                'cluster_id': sp.get('cluster_id'),
                'centroid_lat': sp.get('centroid_lat', sp['lat']),
                'centroid_lon': sp.get('centroid_lon', sp['lon']),
            }
            # Handle departure_time
            if sp.get('departure_time') and sp['departure_time'] != 'Unknown':
                point['departure_time'] = pd.to_datetime(sp['departure_time'])
            else:
                point['departure_time'] = None
            all_points.append(point)

        # Add trajectory points
        for tp in device_data.get('trajectory_points', []):
            point = {
                'lat': tp['lat'],
                'lon': tp['lon'],
                'point_type': 'trajectory',
                'arrival_time': pd.to_datetime(tp['timestamp']),
                'departure_time': None,
                'cluster_id': None,
                'centroid_lat': None,
                'centroid_lon': None,
            }
            all_points.append(point)

        if not all_points:
            return None

        # Create DataFrame and sort by time
        points_df = pd.DataFrame(all_points)
        points_df = points_df.sort_values('arrival_time').reset_index(drop=True)

        return points_df

