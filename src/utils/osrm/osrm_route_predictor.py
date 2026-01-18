import math
import pandas as pd
import os
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import json
from utils.osrm.osrm_client import OSRMClient
from utils.location_pipeline_config_manager import LocationPipelineConfig
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

logger = logging.getLogger(__name__)


class RoutePredictorOSRM:
    """
    Route Predictor using OSRM for map matching and routing.

    This class uses OSRM's /match API for trajectory matching which provides:
    - Better accuracy than point-to-point routing
    - Confidence scores for matches
    - Handles GPS noise and gaps
    - Returns the actual road geometry
    """

    def __init__(self, config: LocationPipelineConfig, trajectory_data_df, start_date, end_date):
        self.trajectory_data_df = trajectory_data_df
        self.start_date = start_date
        self.end_date = end_date
        self.osrm = OSRMClient(config)

    def load_points_data(self):
        """Load points data with both stay_point and trajectory point types"""

        df = self.trajectory_data_df.copy()

        # Check if point_type column exists
        if 'point_type' not in df.columns:
            raise ValueError("Input must contain a 'point_type' column with values 'stay_point' or 'trajectory'")

        # Ensure required columns exist
        required_columns = ['lat', 'lon', 'point_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Handle datetime columns
        datetime_columns = ['arrival_time', 'departure_time']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # For trajectory points, use arrival_time as timestamp if no separate timestamp column
        if 'timestamp' not in df.columns:
            df['timestamp'] = df['arrival_time']
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        # Sort by arrival_time (which contains timestamps for both stay_points and trajectory points)
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
        R = 6371  # Earth's radius in kilometers
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        return R * c

    def calculate_route_divergence(self, route_coords, trajectory_points, max_distance_threshold=0.5):
        """
        Calculate how much a generated route diverges from the actual trajectory points.

        Args:
            route_coords: List of (lat, lon) tuples representing the generated street route
            trajectory_points: List of (lat, lon) tuples representing actual trajectory points
            max_distance_threshold: Maximum allowed distance in km for a point to be considered "close"

        Returns:
            tuple: (max_divergence_km, divergent_percentage, avg_divergence_km)
        """
        if not route_coords or not trajectory_points:
            return 0, 0, 0

        divergent_points = 0
        total_divergence = 0
        max_divergence = 0

        for traj_lat, traj_lon in trajectory_points:
            min_distance = float('inf')

            for route_lat, route_lon in route_coords:
                distance = self.haversine_distance(traj_lat, traj_lon, route_lat, route_lon)
                min_distance = min(min_distance, distance)

            total_divergence += min_distance
            max_divergence = max(max_divergence, min_distance)

            if min_distance > max_distance_threshold:
                divergent_points += 1

        divergent_percentage = (divergent_points / len(trajectory_points)) * 100
        avg_divergence = total_divergence / len(trajectory_points)

        return max_divergence, divergent_percentage, avg_divergence

    def detect_transportation_mode(self, distance_km, actual_duration_minutes, trajectory_points_count=None,
                                   estimated_duration_minutes=None):
        """
        Detect the likely transportation mode based on speed analysis.

        Returns:
            tuple: (mode, avg_speed_kmh, confidence)
        """
        if distance_km <= 0 or actual_duration_minutes <= 0:
            return 'unknown', 0, 0

        avg_speed_kmh = (distance_km / actual_duration_minutes) * 60

        # Check for insufficient data
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

    def create_trajectory_only_route(self, trajectory_points, actual_duration_minutes=None):
        """
        Create a route that simply connects trajectory points directly without street routing.
        Used as fallback when OSRM matching fails.
        """
        if len(trajectory_points) < 2:
            return None, None, None, None, None

        try:
            route_coords = trajectory_points.copy()
            total_distance = 0

            for i in range(len(trajectory_points) - 1):
                lat1, lon1 = trajectory_points[i]
                lat2, lon2 = trajectory_points[i + 1]
                segment_distance = self.haversine_distance(lat1, lon1, lat2, lon2)
                total_distance += segment_distance

            transport_mode = 'unknown'
            mode_confidence = 0
            actual_speed = 0

            if actual_duration_minutes and actual_duration_minutes > 0:
                trajectory_count = len(trajectory_points)
                transport_mode, actual_speed, mode_confidence = self.detect_transportation_mode(
                    total_distance, actual_duration_minutes, trajectory_count, None
                )

                if mode_confidence >= 0.7:
                    if transport_mode == 'walking':
                        estimated_time_minutes = total_distance / 4 * 60
                    elif transport_mode == 'driving':
                        estimated_time_minutes = total_distance / 25 * 60
                    else:
                        estimated_time_minutes = total_distance / 20 * 60
                else:
                    if actual_speed > 0:
                        estimated_time_minutes = total_distance / actual_speed * 60
                    else:
                        estimated_time_minutes = total_distance / 20 * 60
            else:
                estimated_time_minutes = total_distance / 20 * 60

            route_info = {
                'coords': route_coords,
                'original_trajectory_coords': trajectory_points.copy(),
                'snapped_coords': trajectory_points.copy(),
                'distance': total_distance,
                'estimated_time_minutes': estimated_time_minutes,
                'route_type': 'Trajectory-Only Route',
                'score': 0,
                'divergence_detected': True,
                'transport_mode': transport_mode,
                'actual_speed_kmh': actual_speed,
                'mode_confidence': mode_confidence,
                'osrm_confidence': 0
            }

            orig_coords = trajectory_points[0]
            dest_coords = trajectory_points[-1]

            logger.info(
                f"Trajectory-only route: {total_distance:.3f}km, {estimated_time_minutes:.1f}min, {len(route_coords)} points")

            return (route_coords, total_distance, orig_coords, dest_coords,
                    ('trajectory_only', 'trajectory_only', route_info))

        except Exception as e:
            logger.error(f"Error creating trajectory-only route: {e}")
            return None, None, None, None, None

    def create_route_with_osrm_match(self, route_points, actual_duration_minutes=None, timestamps=None):
        """
        Create a route using OSRM's map matching API.
        This is the primary routing method that matches GPS traces to the road network.

        Fallback chain:
        1. Try OSRM /match API (best for GPS traces)
        2. If match fails, try OSRM /route API (point-to-point routing)
        3. If route fails, use trajectory-only (direct lines)

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
            logger.info(f"OSRM matching {len(route_points)} points...")

            # Store original trajectory points
            original_trajectory_coords = list(route_points)

            # Call OSRM match API
            match_result = self.osrm.match(route_points, timestamps=timestamps)

            if match_result is None:
                logger.info("OSRM match failed, trying route API...")
                # Fallback to route API
                route_result = self.osrm.route(route_points)

                if route_result is not None:
                    logger.info(f"OSRM route successful: {route_result['distance']:.3f}km")

                    # Get snapped coords from waypoints
                    snapped_coords = []
                    for wp in route_result.get('waypoints', []):
                        if wp and wp.get('location'):
                            snapped_coords.append((wp['location'][1], wp['location'][0]))
                        else:
                            snapped_coords.append(None)

                    # Fill missing snapped coords
                    while len(snapped_coords) < len(original_trajectory_coords):
                        snapped_coords.append(original_trajectory_coords[len(snapped_coords)])

                    route_coords = route_result['coords']
                    total_distance = route_result['distance']
                    estimated_time_minutes = route_result['duration']

                    # Detect transportation mode
                    transport_mode, actual_speed, mode_confidence = 'unknown', 0, 0
                    if actual_duration_minutes and actual_duration_minutes > 0:
                        transport_mode, actual_speed, mode_confidence = self.detect_transportation_mode(
                            total_distance, actual_duration_minutes
                        )

                    route_info = {
                        'coords': route_coords,
                        'original_trajectory_coords': original_trajectory_coords,
                        'snapped_coords': snapped_coords,
                        'distance': total_distance,
                        'estimated_time_minutes': estimated_time_minutes,
                        'route_type': 'OSRM Route (Fallback)',
                        'score': 0,
                        'divergence_detected': False,
                        'transport_mode': transport_mode,
                        'actual_speed_kmh': actual_speed,
                        'mode_confidence': mode_confidence,
                        'osrm_confidence': 0.5  # Lower confidence for route vs match
                    }

                    orig_coords = route_coords[0] if route_coords else (0, 0)
                    dest_coords = route_coords[-1] if route_coords else (0, 0)

                    return (route_coords, total_distance, orig_coords, dest_coords,
                            ('osrm_route', 'osrm_route', route_info))

                logger.info("OSRM route also failed, falling back to trajectory-only")
                return self.create_trajectory_only_route(route_points, actual_duration_minutes)

            route_coords = match_result['coords']
            total_distance = match_result['distance']
            estimated_time_minutes = match_result['duration']
            confidence = match_result['confidence']

            # Get snapped coordinates (matched positions for original points)
            snapped_coords = []
            for i, sc in enumerate(match_result.get('snapped_coords', [])):
                if sc is not None:
                    snapped_coords.append(sc)
                else:
                    # If point couldn't be matched, use original
                    snapped_coords.append(
                        original_trajectory_coords[i] if i < len(original_trajectory_coords) else None)

            # Fill in any missing snapped coords
            while len(snapped_coords) < len(original_trajectory_coords):
                snapped_coords.append(original_trajectory_coords[len(snapped_coords)])

            logger.info(
                f"OSRM match result: {total_distance:.3f}km, {estimated_time_minutes:.1f}min, confidence: {confidence:.2f}")
            logger.info(f"Matched route has {len(route_coords)} points (from {len(route_points)} input points)")

            # Calculate divergence for quality assessment
            if len(route_points) > 2:
                max_div, div_pct, avg_div = self.calculate_route_divergence(
                    route_coords, route_points[1:-1]
                )
                logger.info(f"Route divergence: max={max_div:.3f}km, avg={avg_div:.3f}km, divergent={div_pct:.1f}%")

            # Detect transportation mode
            transport_mode = 'unknown'
            mode_confidence = 0
            actual_speed = 0

            if actual_duration_minutes and actual_duration_minutes > 0:
                trajectory_count = len(route_points)
                transport_mode, actual_speed, mode_confidence = self.detect_transportation_mode(
                    total_distance, actual_duration_minutes, trajectory_count, estimated_time_minutes
                )
                logger.info(
                    f"Transport mode: {transport_mode} ({actual_speed:.1f} km/h, confidence: {mode_confidence:.1f})")

            route_info = {
                'coords': route_coords,
                'original_trajectory_coords': original_trajectory_coords,
                'snapped_coords': snapped_coords,
                'distance': total_distance,
                'estimated_time_minutes': estimated_time_minutes,
                'route_type': 'OSRM Matched Route',
                'score': 0,
                'divergence_detected': False,
                'transport_mode': transport_mode,
                'actual_speed_kmh': actual_speed,
                'mode_confidence': mode_confidence,
                'osrm_confidence': confidence
            }

            orig_coords = route_coords[0] if route_coords else (0, 0)
            dest_coords = route_coords[-1] if route_coords else (0, 0)

            return (route_coords, total_distance, orig_coords, dest_coords,
                    ('osrm_match', 'osrm_match', route_info))

        except Exception as e:
            logger.error(f"Error in OSRM match: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return self.create_trajectory_only_route(route_points, actual_duration_minutes)

    def create_route_between_stay_points(self, lat1, lon1, lat2, lon2, actual_duration_minutes=None):
        """
        Create a route between two stay points using OSRM routing.
        Used when there are no intermediate trajectory points.
        """
        try:
            route_result = self.osrm.route([(lat1, lon1), (lat2, lon2)])

            if route_result is None:
                # Fallback to direct line
                return self.create_trajectory_only_route([(lat1, lon1), (lat2, lon2)], actual_duration_minutes)

            route_coords = route_result['coords']
            total_distance = route_result['distance']
            estimated_time_minutes = route_result['duration']

            # Detect transportation mode
            transport_mode = 'unknown'
            mode_confidence = 0
            actual_speed = 0

            if actual_duration_minutes and actual_duration_minutes > 0:
                transport_mode, actual_speed, mode_confidence = self.detect_transportation_mode(
                    total_distance, actual_duration_minutes
                )

            route_info = {
                'coords': route_coords,
                'original_trajectory_coords': [(lat1, lon1), (lat2, lon2)],
                'snapped_coords': [route_coords[0], route_coords[-1]] if route_coords else [],
                'distance': total_distance,
                'estimated_time_minutes': estimated_time_minutes,
                'route_type': 'OSRM Route',
                'score': 0,
                'divergence_detected': False,
                'transport_mode': transport_mode,
                'actual_speed_kmh': actual_speed,
                'mode_confidence': mode_confidence,
                'osrm_confidence': 1.0
            }

            orig_coords = route_coords[0] if route_coords else (lat1, lon1)
            dest_coords = route_coords[-1] if route_coords else (lat2, lon2)

            return (route_coords, total_distance, orig_coords, dest_coords,
                    ('osrm_route', 'osrm_route', route_info))

        except Exception as e:
            logger.error(f"Error in OSRM route: {e}")
            return self.create_trajectory_only_route([(lat1, lon1), (lat2, lon2)], actual_duration_minutes)

    def group_points_into_segments(self, points_df, time_threshold_minutes=30):
        """
        Group consecutive points into route segments based on time differences.
        A new segment starts when:
        1. The time difference between consecutive points exceeds the threshold
        2. A stay point is encountered
        """
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
                'routing_engine': 'OSRM'
            },
            'routes': routes_data,
            'stay_points': stay_points_data,
            'trajectory_points': trajectory_points_data
        }

        output_filename = f"predicted_routes_osrm_{self.start_date}_{self.end_date}_{self.device_id}.json"
        output_path = os.path.join(self.device_data_folder, output_filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(prediction_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Routes data saved to: {output_path}")

        return output_path

    def create_enhanced_interactive_route_map(self):
        """
        Create interactive route map using OSRM for routing.
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

        logger.info(f"Start datetime: {start_datetime}")
        logger.info(f"End datetime: {end_datetime}")
        points_df = points_df[points_df[time_col] >= start_datetime]
        points_df = points_df[points_df[time_col] <= end_datetime]

        if len(points_df) < 2:
            logger.warning("Need at least 2 points to create routes")
            return "Need at least 2 points to create routes"

        logger.info(
            f"Found {len(points_df)} points ({len(points_df[points_df['point_type'] == 'stay_point'])} stay points, {len(points_df[points_df['point_type'] == 'trajectory'])} trajectory points)")

        # Group points into segments
        segments = self.group_points_into_segments(points_df, time_threshold_minutes=30)
        logger.info(f"Created {len(segments)} route segments")

        # logging segment details
        for i, segment in enumerate(segments):
            first_point = segment[0]
            last_point = segment[-1]
            segment_duration = (last_point['arrival_time'] - first_point['arrival_time']).total_seconds() / 60
            stay_count = sum(1 for p in segment if p['point_type'] == 'stay_point')
            traj_count = sum(1 for p in segment if p['point_type'] == 'trajectory')
            logger.info(
                f"Segment {i + 1}: {len(segment)} points ({stay_count} stay, {traj_count} trajectory), duration: {segment_duration:.1f}min")

        # Prepare visualization data
        start_time = points_df[time_col].min()
        end_time = points_df[time_col].max()

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

            # Add trajectory points to visualization
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

            # Create route for this segment
            if len(segment) >= 2:
                route_points = [(point['lat'], point['lon']) for point in segment]

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

                # Use OSRM map matching for the route
                route_coords, route_distance, orig_node_coords, dest_node_coords, node_ids = self.create_route_with_osrm_match(
                    route_points, actual_travel_minutes, timestamps
                )

                if route_coords is None:
                    logger.warning(f"Could not create route for segment {segment_idx + 1}")
                    continue

                route_color = self.get_time_color(departure_time, start_time, end_time)
                total_distance += route_distance

                route_analysis = node_ids[2] if len(node_ids) > 2 and isinstance(node_ids[2], dict) else {}
                estimated_time = route_analysis.get('estimated_time_minutes', 0)
                route_type = route_analysis.get('route_type', 'OSRM Route')
                time_accuracy = abs(estimated_time - actual_travel_minutes) if estimated_time > 0 else 0
                divergence_detected = route_analysis.get('divergence_detected', False)
                transport_mode = route_analysis.get('transport_mode', 'unknown')
                actual_speed = route_analysis.get('actual_speed_kmh', 0)
                mode_confidence = route_analysis.get('mode_confidence', 0)
                osrm_confidence = route_analysis.get('osrm_confidence', 0)

                original_trajectory_coords = [list(coord) for coord in
                                              route_analysis.get('original_trajectory_coords', [])]
                snapped_coords = [list(coord) for coord in route_analysis.get('snapped_coords', [])]

                route_info = [
                    segment_idx + 1,
                    route_coords
                    # 'original_trajectory_coords': original_trajectory_coords,
                    # 'snapped_coords': snapped_coords,
                    # 'color': route_color,
                    # 'distance': round(route_distance, 3),
                    # 'departure_time': departure_time.strftime('%Y-%m-%d %H:%M:%S'),
                    # 'arrival_time': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
                    # 'travel_time': travel_time,
                    # 'departure_time_short': departure_time.strftime('%H:%M'),
                    # 'actual_duration': round(actual_travel_minutes, 1),
                    # 'estimated_duration': round(estimated_time, 1),
                    # 'route_type': route_type,
                    # 'time_accuracy': round(time_accuracy, 1),
                    # 'trajectory_points_count': len(trajectory_points),
                    # 'divergence_detected': divergence_detected,
                    # 'transport_mode': transport_mode,
                    # 'actual_speed_kmh': round(actual_speed, 1),
                    # 'mode_confidence': round(mode_confidence, 2),
                    # 'osrm_confidence': round(osrm_confidence, 2),
                    # 'segment_type': segment_type,
                    # 'has_start_stay': has_start_stay,
                    # 'has_end_stay': has_end_stay,
                    # 'start_cluster_id': first_point.get('cluster_id', 'N/A') if has_start_stay else None,
                    # 'end_cluster_id': last_point.get('cluster_id', 'N/A') if has_end_stay else None,
                    # 'connection_coords': [
                    #     [(route_points[0][0], route_points[0][1]), orig_node_coords],
                    #     [(route_points[-1][0], route_points[-1][1]), dest_node_coords]
                    # ],
                    # 'mid_lat': mid_lat,
                    # 'mid_lon': mid_lon
                ]

                routes_data.append(route_info)

        columns = ['route_number', 'coords']
        df_schema = StructType([StructField("route_number", IntegerType(), True) \
                                   , StructField("coords", StringType(), True)])
        return routes_data, df_schema
