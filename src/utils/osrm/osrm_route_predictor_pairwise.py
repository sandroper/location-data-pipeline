"""
Route Prediction using OSRM (Open Source Routing Machine) - Pairwise Matching

This module uses OSRM's Map Matching API to match GPS trajectories to the road network.
Unlike the standard version, this one calls /match for each consecutive pair of points
within a segment, providing more granular matching.

This version outputs only JSON data, without HTML visualization.
Use route_visualization_osrm.py to generate HTML from the JSON output.

Transport Mode Detection:
    The module automatically detects the transport mode (driving/walking) based on
    average speed and uses the appropriate OSRM profile:
    - Driving (> 15 km/h): Uses driving profile on port 5000
    - Walking (< 10 km/h): Uses foot profile on port 5001

OSRM Servers should be running:
    - Driving: http://localhost:5000 (osrm-centro-italia)
    - Foot:    http://localhost:5001 (osrm-centro-italia-foot)

Start with: python utils/osrm_lazio_docker.py
"""

import pandas as pd
import logging
import math
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

from utils.location_pipeline_config_manager import LocationPipelineConfig
from utils.osrm.osrm_client import OSRMClient
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from utils.osrm.osrm_data_dumper import OSRMDataDumper

logger = logging.getLogger(__name__)


class OSRMRoutePredictorPairwise:
    """
        Route Predictor using OSRM with pairwise matching.

        This version calls /match for each consecutive pair of points within a segment,
        providing more granular matching and better handling of complex trajectories.

        Uses separate OSRM instances for driving (port 5000) and foot (port 5001) profiles,
        selecting the appropriate one based on the predicted transport mode.
        """

    def __init__(self, config: LocationPipelineConfig, trajectory_data_df, start_date, end_date):
        self.location_pipeline_config = config
        self.trajectory_data_df = trajectory_data_df
        self.start_date = start_date
        self.end_date = end_date

        # Create OSRM clients for both transport modes
        self.osrm_driving = OSRMClient(config, profile='driving')
        self.osrm_foot = OSRMClient(config, profile='foot')

        # Default to driving for backward compatibility
        self.osrm = self.osrm_driving

    def load_points_data(self):
        """Load points data with both stay_point and trajectory point types"""
        df = self.trajectory_data_df.copy()

        if 'point_type' not in df.columns:
            raise ValueError("Input CSV must contain a 'point_type' column with values 'stay_point' or 'trajectory'")

        required_columns = ['lat', 'lon', 'point_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        datetime_columns = ['arrival_time', 'departure_time']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if 'timestamp' not in df.columns:
            df['timestamp'] = df['arrival_time']
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        if 'arrival_time' in df.columns:
            df = df.sort_values('arrival_time')

        logger.info(f"Loaded {len(df)} total points:")
        logger.info(f"  - Stay points: {len(df[df['point_type'] == 'stay_point'])}")
        logger.info(f"  - Trajectory points: {len(df[df['point_type'] == 'trajectory'])}")

        return df

    def get_time_color(self, timestamp, start_time, end_time):
        """Generate color based on time progression"""
        total_duration = (end_time - start_time).total_seconds()
        elapsed = (timestamp - start_time).total_seconds()
        progress = elapsed / total_duration if total_duration > 0 else 0

        cmap = plt.cm.viridis
        color = cmap(progress)
        return mcolors.to_hex(color)

    def format_time_duration(self, start_time, end_time):
        """Format duration between two times"""
        duration = end_time - start_time
        hours = int(duration.total_seconds() // 3600)
        minutes = int((duration.total_seconds() % 3600) // 60)
        return f"{hours}h {minutes}m"

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate the great circle distance between two points on earth"""
        R = 6371
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        return R * c

    def detect_transportation_mode(self, distance_km, actual_duration_minutes, trajectory_points_count=None,
                                   estimated_duration_minutes=None):
        """Detect the likely transportation mode based on speed analysis."""
        if distance_km <= 0 or actual_duration_minutes <= 0:
            return 'unknown', 0, 0

        avg_speed_kmh = (distance_km / actual_duration_minutes) * 60

        insufficient_data = False

        if (trajectory_points_count is not None and trajectory_points_count < 5 and
                estimated_duration_minutes is not None and estimated_duration_minutes > 0):
            time_discrepancy_ratio = actual_duration_minutes / estimated_duration_minutes
            if time_discrepancy_ratio > 10:
                insufficient_data = True

        if trajectory_points_count is not None and distance_km > 2 and trajectory_points_count < 3:
            insufficient_data = True

        if actual_duration_minutes > 300:
            insufficient_data = True

        if insufficient_data:
            return 'unknown', avg_speed_kmh, 0.8

        if avg_speed_kmh <= 10:
            if 2 <= avg_speed_kmh <= 6:
                return 'walking', avg_speed_kmh, 0.9
            elif 0.3 <= avg_speed_kmh < 2:
                return 'walking', avg_speed_kmh, 0.8
            elif 6 < avg_speed_kmh <= 10:
                return 'walking', avg_speed_kmh, 0.7
            else:
                return 'walking', avg_speed_kmh, 0.5
        elif avg_speed_kmh >= 15:
            if 25 <= avg_speed_kmh <= 60:
                return 'driving', avg_speed_kmh, 0.9
            elif 15 <= avg_speed_kmh < 25:
                return 'driving', avg_speed_kmh, 0.7
            else:
                return 'driving', avg_speed_kmh, 0.8
        else:
            return 'unknown', avg_speed_kmh, 0.6

    def create_direct_segment(self, point1, point2):
        """Create a direct line segment between two points (fallback)."""
        lat1, lon1 = point1
        lat2, lon2 = point2
        distance = self.haversine_distance(lat1, lon1, lat2, lon2)

        return {
            'coords': [point1, point2],
            'distance': distance,
            'duration': distance / 20 * 60,  # Assume 20 km/h average
            'confidence': 0,
            'snapped_coords': [point1, point2],
            'route_type': 'direct'
        }

    def pre_detect_transport_mode(self, route_points, actual_duration_minutes):
        """
        Pre-detect transport mode based on straight-line distance and actual duration.
        This is used to select the appropriate OSRM profile BEFORE map matching.

        Args:
            route_points: List of (lat, lon) tuples representing the GPS trace
            actual_duration_minutes: Actual travel time in minutes

        Returns:
            tuple: (transport_mode, osrm_client)
                - transport_mode: 'driving', 'walking', or 'unknown'
                - osrm_client: the appropriate OSRMClient instance
        """
        if len(route_points) < 2 or actual_duration_minutes <= 0:
            return 'unknown', self.osrm_driving

        # Calculate straight-line distance between first and last points
        total_distance = sum(
            self.haversine_distance(route_points[i][0], route_points[i][1],
                                    route_points[i + 1][0], route_points[i + 1][1])
            for i in range(len(route_points) - 1)
        )

        # Calculate average speed based on straight-line distance
        avg_speed_kmh = (total_distance / actual_duration_minutes) * 60

        # Use speed thresholds to determine mode
        # Walking: typically 2-6 km/h, max ~10 km/h
        # Driving: typically > 15 km/h
        if avg_speed_kmh <= 10:
            logger.info(f"Pre-detected mode: walking (avg speed: {avg_speed_kmh:.1f} km/h, using foot profile)")
            return 'walking', self.osrm_foot
        elif avg_speed_kmh >= 15:
            logger.info(f"Pre-detected mode: driving (avg speed: {avg_speed_kmh:.1f} km/h, using driving profile)")
            return 'driving', self.osrm_driving
        else:
            # Ambiguous speed range (10-15 km/h) - could be fast walking, cycling, or slow driving
            # Default to driving as it's more common
            logger.info(f"Pre-detected mode: unknown (avg speed: {avg_speed_kmh:.1f} km/h, defaulting to driving profile)")
            return 'unknown', self.osrm_driving

    def get_osrm_client_for_mode(self, transport_mode):
        """
        Get the appropriate OSRM client for a given transport mode.

        Args:
            transport_mode: 'driving', 'walking', or 'unknown'

        Returns:
            OSRMClient instance
        """
        if transport_mode == 'walking':
            return self.osrm_foot
        else:
            return self.osrm_driving

    def match_pair(self, point1, point2, timestamp1=None, timestamp2=None, osrm_client=None):
        """
        Match a single pair of consecutive points using OSRM /match API.
        Falls back to /route, then to direct line if both fail.

        Args:
            point1: (lat, lon) tuple for first point
            point2: (lat, lon) tuple for second point
            timestamp1: Optional Unix timestamp for first point
            timestamp2: Optional Unix timestamp for second point
            osrm_client: Optional OSRMClient to use (defaults to self.osrm_driving)

        Returns:
            dict with matched route info
        """
        # Use provided client or default to driving
        client = osrm_client if osrm_client is not None else self.osrm_driving

        points = [point1, point2]
        timestamps = None
        if timestamp1 is not None and timestamp2 is not None:
            timestamps = [timestamp1, timestamp2]

        # Try /match first
        match_result = client.match(points, timestamps=timestamps)

        if match_result is not None:
            return {
                'coords': match_result['coords'],
                'distance': match_result['distance'],
                'duration': match_result['duration'],
                'confidence': match_result['confidence'],
                'snapped_coords': match_result.get('snapped_coords', [point1, point2]),
                'route_type': 'match'
            }

        # Fallback to /route
        route_result = client.route(points)

        if route_result is not None:
            snapped_coords = []
            for wp in route_result.get('waypoints', []):
                if wp and wp.get('location'):
                    snapped_coords.append((wp['location'][1], wp['location'][0]))
                else:
                    snapped_coords.append(None)

            if len(snapped_coords) < 2:
                snapped_coords = [point1, point2]

            return {
                'coords': route_result['coords'],
                'distance': route_result['distance'],
                'duration': route_result['duration'],
                'confidence': 0.5,  # Lower confidence for route fallback
                'snapped_coords': snapped_coords,
                'route_type': 'route'
            }

        # Final fallback: direct line
        return self.create_direct_segment(point1, point2)

    def create_route_with_pairwise_matching(self, route_points, actual_duration_minutes=None, timestamps=None):
        """
        Create a route by matching each consecutive pair of points using OSRM /match API.

        This provides more granular matching compared to matching all points at once.
        Each pair is matched independently, and results are combined.

        The method pre-detects the transport mode (driving/walking) based on speed,
        and uses the appropriate OSRM server (driving on port 5000, foot on port 5001).

        Args:
            route_points: List of (lat, lon) tuples representing the GPS trace
            actual_duration_minutes: Actual travel time in minutes
            timestamps: Optional list of Unix timestamps for each point

        Returns:
            Tuple of (route_coords, distance, orig_coords, dest_coords, route_info)
        """
        if len(route_points) < 2:
            return None, None, None, None, None

        try:
            # Pre-detect transport mode to select appropriate OSRM server
            pre_detected_mode = 'unknown'
            osrm_client = self.osrm_driving  # Default to driving

            if actual_duration_minutes and actual_duration_minutes > 0:
                pre_detected_mode, osrm_client = self.pre_detect_transport_mode(
                    route_points, actual_duration_minutes
                )

            profile_name = 'foot' if osrm_client == self.osrm_foot else 'driving'
            logger.info(
                f"Pairwise matching {len(route_points)} points ({len(route_points) - 1} pairs) using {profile_name} profile...")

            # Store original trajectory points
            original_trajectory_coords = list(route_points)

            # Match each consecutive pair using the selected OSRM client
            all_coords = []
            all_snapped = []
            total_distance = 0
            total_duration = 0
            confidences = []
            route_types = []

            for i in range(len(route_points) - 1):
                point1 = route_points[i]
                point2 = route_points[i + 1]

                ts1 = timestamps[i] if timestamps else None
                ts2 = timestamps[i + 1] if timestamps else None

                pair_result = self.match_pair(point1, point2, ts1, ts2, osrm_client=osrm_client)

                # Add coords (avoid duplicating junction points)
                if i == 0:
                    all_coords.extend(pair_result['coords'])
                else:
                    # Skip first point as it's the same as last point of previous pair
                    all_coords.extend(pair_result['coords'][1:])

                # Track snapped coordinates
                if i == 0:
                    all_snapped.append(pair_result['snapped_coords'][0] if pair_result['snapped_coords'] else point1)
                all_snapped.append(pair_result['snapped_coords'][-1] if pair_result['snapped_coords'] else point2)

                total_distance += pair_result['distance']
                total_duration += pair_result['duration']
                confidences.append(pair_result['confidence'])
                route_types.append(pair_result['route_type'])

            # Calculate average confidence
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0

            # Count route types
            match_count = route_types.count('match')
            route_count = route_types.count('route')
            direct_count = route_types.count('direct')

            logger.info(
                f"Pairwise result: {total_distance:.3f}km, {total_duration:.1f}min, avg confidence: {avg_confidence:.2f}")
            logger.info(f"  Pair results: {match_count} matched, {route_count} routed, {direct_count} direct")

            # Detect transportation mode based on matched distance (more accurate than pre-detection)
            transport_mode = pre_detected_mode
            mode_confidence = 0
            actual_speed = 0

            if actual_duration_minutes and actual_duration_minutes > 0:
                trajectory_count = len(route_points)
                transport_mode, actual_speed, mode_confidence = self.detect_transportation_mode(
                    total_distance, actual_duration_minutes, trajectory_count, total_duration
                )
                logger.info(
                    f"Transport mode: {transport_mode} ({actual_speed:.1f} km/h, confidence: {mode_confidence:.1f}, profile used: {profile_name})")

            # Determine overall route type
            if direct_count == len(route_types):
                overall_route_type = 'Pairwise Direct (All Fallback)'
            elif match_count == len(route_types):
                overall_route_type = 'Pairwise OSRM Matched'
            elif match_count > 0:
                overall_route_type = f'Pairwise Mixed ({match_count} matched, {route_count} routed, {direct_count} direct)'
            else:
                overall_route_type = f'Pairwise Routed ({route_count} routed, {direct_count} direct)'

            route_info = {
                'coords': all_coords,
                'original_trajectory_coords': original_trajectory_coords,
                'snapped_coords': all_snapped,
                'distance': total_distance,
                'estimated_time_minutes': total_duration,
                'route_type': overall_route_type,
                'score': 0,
                'divergence_detected': direct_count > 0,
                'transport_mode': transport_mode,
                'actual_speed_kmh': actual_speed,
                'mode_confidence': mode_confidence,
                'osrm_confidence': avg_confidence,
                'osrm_profile': profile_name,  # Track which OSRM profile was used
                'pair_count': len(route_points) - 1,
                'match_count': match_count,
                'route_count': route_count,
                'direct_count': direct_count
            }

            orig_coords = all_coords[0] if all_coords else route_points[0]
            dest_coords = all_coords[-1] if all_coords else route_points[-1]

            return (all_coords, total_distance, orig_coords, dest_coords,
                    ('pairwise_match', 'pairwise_match', route_info))

        except Exception as e:
            logger.error(f"Error in pairwise matching: {e}")
            import traceback
            logger.error(traceback.format_exc())

            # Fallback: create direct lines between all points
            all_coords = list(route_points)
            total_distance = sum(
                self.haversine_distance(route_points[i][0], route_points[i][1],
                                        route_points[i + 1][0], route_points[i + 1][1])
                for i in range(len(route_points) - 1)
            )

            route_info = {
                'coords': all_coords,
                'original_trajectory_coords': list(route_points),
                'snapped_coords': list(route_points),
                'distance': total_distance,
                'estimated_time_minutes': total_distance / 20 * 60,
                'route_type': 'Trajectory-Only (Error Fallback)',
                'score': 0,
                'divergence_detected': True,
                'transport_mode': 'unknown',
                'actual_speed_kmh': 0,
                'mode_confidence': 0,
                'osrm_confidence': 0
            }

            return (all_coords, total_distance, route_points[0], route_points[-1],
                    ('fallback', 'fallback', route_info))

    def group_points_into_segments(self, points_df, time_threshold_minutes=30):
        """Group consecutive points into route segments based on time differences."""
        segments = []
        current_segment = []
        time_col = 'arrival_time'

        for i, row in points_df.iterrows():
            row_copy = row.copy()

            if row_copy['point_type'] == 'stay_point':
                row_copy['is_segment_boundary'] = True

                if current_segment:
                    current_segment.append(row_copy)
                    if len(current_segment) >= 2:
                        segments.append(current_segment)
                    current_segment = [row_copy]
                else:
                    current_segment = [row_copy]
            else:
                row_copy['is_segment_boundary'] = False

                if not current_segment:
                    current_segment = [row_copy]
                else:
                    current_time = row_copy[time_col]
                    previous_point = current_segment[-1]

                    if previous_point['point_type'] == 'stay_point' and pd.notna(previous_point.get('departure_time')):
                        previous_time = previous_point['departure_time']
                    else:
                        previous_time = previous_point[time_col]

                    time_diff_minutes = (current_time - previous_time).total_seconds() / 60

                    if time_diff_minutes <= time_threshold_minutes:
                        current_segment.append(row_copy)
                    else:
                        if len(current_segment) >= 2:
                            segments.append(current_segment)
                        current_segment = [row_copy]

        if current_segment and len(current_segment) >= 2:
            segments.append(current_segment)

        return segments

    def predict_routes(self):
        """
        Main method to predict routes using pairwise matching and generate JSON output.
        Returns the path to the generated JSON file.
        """
        points_df = self.load_points_data()

        time_col = 'arrival_time'
        if time_col not in points_df.columns:
            raise ValueError("No 'arrival_time' column found in the data")

        # Filter by date range
        if self.start_date is not None:
            start_datetime = pd.to_datetime(self.start_date)
        else:
            start_datetime = points_df[time_col].min()
        if self.end_date is not None:
            end_datetime = pd.to_datetime(self.end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        else:
            end_datetime = points_df[time_col].max()

        # logger actual data date range before filtering
        data_min_date = points_df[time_col].min()
        data_max_date = points_df[time_col].max()
        logger.info(f"Data date range: {data_min_date} to {data_max_date}")
        logger.info(f"Filter date range: {start_datetime} to {end_datetime}")

        original_count = len(points_df)
        points_df = points_df[points_df[time_col] >= start_datetime]
        points_df = points_df[points_df[time_col] <= end_datetime]

        if len(points_df) < 2:
            logger.warning(
                f"Need at least 2 points to create routes (filtered {original_count} -> {len(points_df)} points)")
            logger.warning(
                f"Data date range ({data_min_date} to {data_max_date}) does not overlap with filter range ({start_datetime} to {end_datetime})")
            return None

        logger.info(
            f"Found {len(points_df)} points ({len(points_df[points_df['point_type'] == 'stay_point'])} stay points, {len(points_df[points_df['point_type'] == 'trajectory'])} trajectory points)")

        # Group points into segments
        segments = self.group_points_into_segments(points_df, time_threshold_minutes=30)
        logger.info(f"Created {len(segments)} route segments")

        # logger segment details
        for i, segment in enumerate(segments):
            first_point = segment[0]
            last_point = segment[-1]
            segment_duration = (last_point['arrival_time'] - first_point['arrival_time']).total_seconds() / 60
            stay_count = sum(1 for p in segment if p['point_type'] == 'stay_point')
            traj_count = sum(1 for p in segment if p['point_type'] == 'trajectory')
            logger.info(
                f"Segment {i + 1}: {len(segment)} points ({stay_count} stay, {traj_count} trajectory), duration: {segment_duration:.1f}min")

        # Prepare data
        start_time = points_df[time_col].min()
        end_time = points_df[time_col].max()
        total_journey_time = self.format_time_duration(start_time, end_time)

        stay_points_data = []
        routes_data = []
        trajectory_points_data = []
        total_distance = 0

        # Build segment boundary coordinates
        segment_boundary_coords = set()
        for segment in segments:
            for point in segment:
                if point.get('is_segment_boundary', False):
                    segment_boundary_coords.add((round(point['lat'], 6), round(point['lon'], 6)))

        # Process stay points
        stay_points_in_data = points_df[points_df['point_type'] == 'stay_point']
        for idx, point in stay_points_in_data.iterrows():
            is_boundary = (round(point['lat'], 6), round(point['lon'], 6)) in segment_boundary_coords

            if is_boundary:
                point_color = '#9B59B6'
            else:
                point_color = self.get_time_color(point[time_col], start_time, end_time)

            if 'departure_time' in point and pd.notna(point['departure_time']):
                stay_duration = self.format_time_duration(point['arrival_time'], point['departure_time'])
                duration_minutes = (point['departure_time'] - point['arrival_time']).total_seconds() / 60
            else:
                stay_duration = "Unknown"
                duration_minutes = 0

            radius = min(15, max(8, duration_minutes / 10)) if duration_minutes > 0 else 8

            stay_point_info = {
                'lat': point['lat'],
                'lon': point['lon'],
                'cluster_id': point.get('cluster_id', f'SP_{len(stay_points_data)}'),
                'visit_number': len(stay_points_data) + 1,
                'color': point_color,
                'radius': radius,
                'arrival_time': point[time_col].strftime('%Y-%m-%d %H:%M:%S'),
                'departure_time': point.get('departure_time', point[time_col]).strftime(
                    '%Y-%m-%d %H:%M:%S') if pd.notna(point.get('departure_time', point[time_col])) else 'Unknown',
                'stay_duration': stay_duration,
                'duration_minutes': int(duration_minutes),
                'centroid_lat': point.get('centroid_lat', point['lat']),
                'centroid_lon': point.get('centroid_lon', point['lon']),
                'is_segment_boundary': is_boundary
            }

            stay_points_data.append(stay_point_info)

        # Process each segment
        for segment_idx, segment in enumerate(segments):
            if len(segment) < 2:
                continue

            first_point = segment[0]
            last_point = segment[-1]

            has_start_stay = first_point['point_type'] == 'stay_point'
            has_end_stay = last_point['point_type'] == 'stay_point'

            # Extract trajectory points
            if has_start_stay and has_end_stay:
                trajectory_points = [point for point in segment[1:-1] if point['point_type'] == 'trajectory']
            elif has_start_stay and not has_end_stay:
                trajectory_points = [point for point in segment[1:] if point['point_type'] == 'trajectory']
            elif not has_start_stay and has_end_stay:
                trajectory_points = [point for point in segment[:-1] if point['point_type'] == 'trajectory']
            else:
                trajectory_points = [point for point in segment if point['point_type'] == 'trajectory']

            # Add trajectory points to data
            for traj_point in trajectory_points:
                point_color = self.get_time_color(traj_point[time_col], start_time, end_time)

                traj_point_info = {
                    'lat': traj_point['lat'],
                    'lon': traj_point['lon'],
                    'timestamp': traj_point[time_col].strftime('%Y-%m-%d %H:%M:%S'),
                    'color': point_color,
                    'segment_id': segment_idx,
                    'duration_minutes': traj_point.get('duration_minutes', 0)
                }
                trajectory_points_data.append(traj_point_info)

            # Create route for this segment using PAIRWISE matching
            if len(segment) >= 2:
                route_points = [
                    (point.get('centroid_lat', point['lat']), point.get('centroid_lon', point['lon']))
                    if point['point_type'] == 'stay_point'
                    else (point['lat'], point['lon'])
                    for point in segment
                ]

                # Get timestamps for better matching
                timestamps = None
                if time_col in segment[0]:
                    try:
                        timestamps = [point[time_col].timestamp() for point in segment]
                    except:
                        timestamps = None

                first_point_time = first_point.get('departure_time', first_point[time_col])
                last_point_time = last_point[time_col]

                departure_time = first_point_time
                arrival_time = last_point_time
                travel_time = self.format_time_duration(departure_time, arrival_time)
                actual_travel_minutes = (arrival_time - departure_time).total_seconds() / 60

                segment_type = "Mixed"
                if not has_start_stay and not has_end_stay:
                    segment_type = "Trajectory-Only"
                elif has_start_stay and has_end_stay:
                    segment_type = "Stay-to-Stay"
                elif has_start_stay:
                    segment_type = "Stay-to-Trajectory"
                else:
                    segment_type = "Trajectory-to-Stay"

                logger.info(
                    f"Creating route {segment_idx + 1} [{segment_type}]: {len(route_points)} points, actual time: {actual_travel_minutes:.1f}min")

                # Use PAIRWISE OSRM matching for the route
                route_coords, route_distance, orig_node_coords, dest_node_coords, node_ids = self.create_route_with_pairwise_matching(
                    route_points, actual_travel_minutes, timestamps
                )

                if route_coords is None:
                    logger.warning(f"Could not create route for segment {segment_idx + 1}")
                    continue

                route_color = self.get_time_color(departure_time, start_time, end_time)
                total_distance += route_distance

                route_analysis = node_ids[2] if len(node_ids) > 2 and isinstance(node_ids[2], dict) else {}
                estimated_time = route_analysis.get('estimated_time_minutes', 0)
                route_type = route_analysis.get('route_type', 'Pairwise OSRM')
                time_accuracy = abs(estimated_time - actual_travel_minutes) if estimated_time > 0 else 0
                divergence_detected = route_analysis.get('divergence_detected', False)
                transport_mode = route_analysis.get('transport_mode', 'unknown')
                actual_speed = route_analysis.get('actual_speed_kmh', 0)
                mode_confidence = route_analysis.get('mode_confidence', 0)
                osrm_confidence = route_analysis.get('osrm_confidence', 0)

                original_trajectory_coords = [list(coord) for coord in
                                              route_analysis.get('original_trajectory_coords', [])]
                snapped_coords = [list(coord) for coord in route_analysis.get('snapped_coords', [])]

                mid_lat = (route_points[0][0] + route_points[-1][0]) / 2
                mid_lon = (route_points[0][1] + route_points[-1][1]) / 2

                route_info = {
                    'route_number': segment_idx + 1,
                    'coords': route_coords,
                    'original_trajectory_coords': original_trajectory_coords,
                    'snapped_coords': snapped_coords,
                    'color': route_color,
                    'distance': round(route_distance, 3),
                    'departure_time': departure_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'arrival_time': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'travel_time': travel_time,
                    'departure_time_short': departure_time.strftime('%H:%M'),
                    'actual_duration': round(actual_travel_minutes, 1),
                    'estimated_duration': round(estimated_time, 1),
                    'route_type': route_type,
                    'time_accuracy': round(time_accuracy, 1),
                    'trajectory_points_count': len(trajectory_points),
                    'divergence_detected': divergence_detected,
                    'transport_mode': transport_mode,
                    'actual_speed_kmh': round(actual_speed, 1),
                    'mode_confidence': round(mode_confidence, 2),
                    'osrm_confidence': round(osrm_confidence, 2),
                    'osrm_profile': route_analysis.get('osrm_profile', 'driving'),  # Track which OSRM profile was used
                    'segment_type': segment_type,
                    'has_start_stay': has_start_stay,
                    'has_end_stay': has_end_stay,
                    'start_cluster_id': first_point.get('cluster_id', 'N/A') if has_start_stay else None,
                    'end_cluster_id': last_point.get('cluster_id', 'N/A') if has_end_stay else None,
                    'connection_coords': [
                        [(route_points[0][0], route_points[0][1]), orig_node_coords],
                        [(route_points[-1][0], route_points[-1][1]), dest_node_coords]
                    ],
                    'mid_lat': mid_lat,
                    'mid_lon': mid_lon,
                    # Pairwise-specific metadata
                    'pair_count': route_analysis.get('pair_count', 0),
                    'match_count': route_analysis.get('match_count', 0),
                    'route_count': route_analysis.get('route_count', 0),
                    'direct_count': route_analysis.get('direct_count', 0)
                }

                routes_data.append(route_info)

        if self.location_pipeline_config.save_routes_json:
            dumper = OSRMDataDumper(self.location_pipeline_config, self.start_date, self.end_date)
            dumper.save_routes_data(routes_data, stay_points_data, trajectory_points_data, total_distance,
                                                  total_journey_time, start_time, end_time)

        logger.info(f"{'=' * 60}")
        logger.info(f"OSRM Pairwise Route Prediction Complete!")
        logger.info(f"{'=' * 60}")
        logger.info(f"Stay Points: {len(stay_points_data)}")
        logger.info(f"Trajectory Points: {len(trajectory_points_data)}")
        logger.info(f"Route Segments: {len(routes_data)}")
        logger.info(f"Total Distance: {total_distance:.3f} km")
        logger.info(f"Total Journey Time: {total_journey_time}")

        df_schema = StructType([StructField("route_number", IntegerType(), True),
                                   StructField("coords", StringType(), True)])

        return routes_data, df_schema
