import pandas as pd
import os
import logging
import json
from datetime import datetime

from utils.location_pipeline_config_manager import LocationPipelineConfig

logger = logging.getLogger(__name__)

class OSRMDataDumper:
    def __init__(self, config: LocationPipelineConfig, start_date, end_date):
        self.config = config
        self.device_id = '111111'
        self.start_date = start_date
        self.end_date = end_date

    def save_routes_data(self, routes_data, stay_points_data, trajectory_points_data, total_distance,
                         total_journey_time, start_time, end_time):
        """Save all route prediction data to a JSON file"""
        prediction_data = {
            'metadata': {
                'start_date': self.start_date,
                'end_date': self.end_date,
                'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_distance_km': round(total_distance, 3),
                'total_journey_time': total_journey_time,
                'total_segments': len(routes_data),
                'total_stay_points': len(stay_points_data),
                'total_trajectory_points': len(trajectory_points_data),
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'routing_engine': 'OSRM',
                'matching_mode': 'pairwise'
            },
            'routes': routes_data,
            'stay_points': stay_points_data,
            'trajectory_points': trajectory_points_data
        }

        output_filename = f"predicted_routes_osrm_pairwise_{self.device_id}.json"
        output_path = os.path.join(self.config.output_data_dir, output_filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(prediction_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Routes data saved to: {output_path}")

        return output_path

