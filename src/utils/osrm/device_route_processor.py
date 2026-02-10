"""
Device Route Processor - Handles route prediction for multiple devices with diagnostics.

This module provides a clean interface for processing routes across multiple devices,
tracking statistics, and providing diagnostic information about the processing results.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from utils.location_pipeline_config_manager import LocationPipelineConfig
from utils.osrm.osrm_route_predictor_pairwise import OSRMRoutePredictorPairwise

logger = logging.getLogger(__name__)


@dataclass
class RouteProcessingStats:
    """Statistics from route processing across devices."""
    devices_processed: int = 0
    devices_with_routes: int = 0
    devices_insufficient_points: int = 0
    devices_no_segments: int = 0
    devices_with_errors: int = 0
    total_routes: int = 0
    total_distance_km: float = 0.0
    error_device_ids: List[str] = field(default_factory=list)

    def log_summary(self):
        """Log a summary of the processing statistics."""
        logger.info(f"Route processing statistics:")
        logger.info(f"  - Devices processed: {self.devices_processed}")
        logger.info(f"  - Devices with routes: {self.devices_with_routes}")
        logger.info(f"  - Devices with insufficient points: {self.devices_insufficient_points}")
        logger.info(f"  - Devices with no route segments: {self.devices_no_segments}")
        logger.info(f"  - Devices with errors: {self.devices_with_errors}")
        logger.info(f"  - Total route segments: {self.total_routes}")
        logger.info(f"  - Total distance: {self.total_distance_km:.2f} km")


class DeviceRouteProcessor:
    """
    Processes route predictions for multiple devices from a clustered DataFrame.

    Provides clean separation of route processing logic from the main pipeline,
    with built-in diagnostics and statistics tracking.
    """

    def __init__(self, config: LocationPipelineConfig):
        self.config = config

    def process_all_devices(self, clustered_df: pd.DataFrame) -> Tuple[Dict[str, Any], RouteProcessingStats]:
        """
        Process route predictions for all devices in the DataFrame.

        Args:
            clustered_df: Pandas DataFrame with clustered location data.
                         Must contain 'device_id' and 'arrival_time' columns.

        Returns:
            Tuple of (device_results, stats):
                - device_results: Dict mapping device_id to route prediction results
                - stats: RouteProcessingStats with processing diagnostics
        """
        device_ids = clustered_df['device_id'].unique()
        stats = RouteProcessingStats(devices_processed=len(device_ids))

        logger.info(f"Processing routes for {len(device_ids)} device(s)")

        device_results = {}

        for device_id in device_ids:
            result = self._process_single_device(
                device_id=str(device_id),
                device_df=clustered_df[clustered_df['device_id'] == device_id],
                stats=stats
            )
            if result:
                device_results[str(device_id)] = result

        # Calculate totals from results
        for device_id, result in device_results.items():
            routes = result.get('routes', [])
            if routes:
                stats.devices_with_routes += 1
                stats.total_routes += len(routes)
                stats.total_distance_km += result.get('total_distance', 0)

        stats.log_summary()
        return device_results, stats

    def _process_single_device(
        self,
        device_id: str,
        device_df: pd.DataFrame,
        stats: RouteProcessingStats
    ) -> Optional[Dict[str, Any]]:
        """
        Process route prediction for a single device.

        Args:
            device_id: The device identifier
            device_df: DataFrame containing only this device's data
            stats: Stats object to update with results

        Returns:
            Device result dict or None if processing failed
        """
        start_date = device_df['arrival_time'].min()
        end_date = device_df['arrival_time'].max()

        try:
            predictor = OSRMRoutePredictorPairwise(
                config=self.config,
                trajectory_data_df=device_df,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date
            )

            device_result = predictor.predict_routes()

            if device_result is None:
                stats.devices_insufficient_points += 1
                return None

            if not device_result.get('routes'):
                stats.devices_no_segments += 1

            return device_result

        except Exception as e:
            stats.devices_with_errors += 1
            stats.error_device_ids.append(device_id)
            logger.error(f"Error processing device {device_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
