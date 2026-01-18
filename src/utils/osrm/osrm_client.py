"""
Route Prediction using OSRM (Open Source Routing Machine)

This module uses OSRM's Map Matching API to match GPS trajectories to the road network.
The /match endpoint is specifically designed for trajectory matching and provides:
- Better accuracy than point-to-point routing
- Confidence scores for matches
- Handles GPS noise and gaps
- Returns the actual road geometry

OSRM Server should be running at http://localhost:5000
"""

import logging
import requests
from utils.location_pipeline_config_manager import LocationPipelineConfig

class OSRMClient:
    def __init__(self, config: LocationPipelineConfig):
        self.server_url = config.osrm_server.rstrip('/')
        self._check_server()

    def _check_server(self):
        """Check if OSRM server is running"""
        try:
            # Test with a simple nearest query
            response = requests.get(f"{self.server_url}/nearest/v1/driving/12.4964,41.9028", timeout=5)
            if response.status_code != 200:
                logging.warning(f"OSRM server returned status {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Could not connect to OSRM server at {self.server_url}")
            logging.warning(f"Error: {e}")
            logging.warning("Make sure OSRM server is running: python utils/osrm_lazio_docker.py")

    def nearest(self, lat, lon, number=1):
        """
        Find the nearest road point(s) to a given coordinate.

        Args:
            lat: Latitude
            lon: Longitude
            number: Number of nearest points to return

        Returns:
            dict with 'location' (lon, lat), 'distance', 'name' or None if failed
        """
        url = f"{self.server_url}/nearest/v1/driving/{lon},{lat}"
        params = {"number": number}

        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if data.get('code') == 'Ok' and data.get('waypoints'):
                waypoint = data['waypoints'][0]
                return {
                    'location': waypoint['location'],  # [lon, lat]
                    'distance': waypoint['distance'],  # meters
                    'name': waypoint.get('name', ''),
                    'snapped_lat': waypoint['location'][1],
                    'snapped_lon': waypoint['location'][0]
                }
        except Exception as e:
            logging.error(f"OSRM nearest error: {e}")

        return None

    def route(self, points, alternatives=False, steps=False, annotations=True):
        """
        Calculate route between two or more points.

        Args:
            points: List of (lat, lon) tuples
            alternatives: Return alternative routes
            steps: Return turn-by-turn instructions
            annotations: Return additional metadata (duration, distance per segment)

        Returns:
            dict with route info or None if failed
        """
        if len(points) < 2:
            return None

        # OSRM expects lon,lat order
        coords = ";".join(f"{lon},{lat}" for lat, lon in points)
        url = f"{self.server_url}/route/v1/driving/{coords}"

        params = {
            "geometries": "geojson",
            "overview": "full",
            "alternatives": str(alternatives).lower(),
            "steps": str(steps).lower(),
            "annotations": str(annotations).lower()
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()

            if data.get('code') == 'Ok' and data.get('routes'):
                route = data['routes'][0]
                # Convert GeoJSON coordinates from [lon, lat] to (lat, lon)
                coords_geojson = route['geometry']['coordinates']
                route_coords = [(coord[1], coord[0]) for coord in coords_geojson]

                return {
                    'coords': route_coords,
                    'distance': route['distance'] / 1000,  # Convert to km
                    'duration': route['duration'] / 60,  # Convert to minutes
                    'waypoints': data.get('waypoints', [])
                }
        except Exception as e:
            logging.error(f"OSRM route error: {e}")

        return None

    def match(self, points, timestamps=None, radiuses=None, gaps='split'):
        """
        Match GPS trajectory to road network using OSRM's map matching.
        This is the core function for trajectory matching.

        Args:
            points: List of (lat, lon) tuples representing the GPS trace
            timestamps: Optional list of Unix timestamps for each point
            radiuses: Optional list of GPS accuracy in meters for each point (default 25m)
            gaps: How to handle gaps - 'split' (separate matchings) or 'ignore'

        Returns:
            dict with matched route info, confidence, and waypoints or None if failed
        """
        if len(points) < 2:
            return None

        # OSRM expects lon,lat order
        coords = ";".join(f"{lon},{lat}" for lat, lon in points)
        url = f"{self.server_url}/match/v1/driving/{coords}"

        params = {
            "geometries": "geojson",
            "overview": "full",
            "annotations": "true",
            "gaps": gaps
        }

        # Add timestamps if provided (helps with matching accuracy)
        if timestamps:
            params["timestamps"] = ";".join(str(int(t)) for t in timestamps)

        # Add radiuses (GPS accuracy) - use larger default (50m) for better matching
        if radiuses:
            params["radiuses"] = ";".join(str(r) for r in radiuses)
        else:
            # Default GPS accuracy of 50 meters for better matching tolerance
            params["radiuses"] = ";".join("50" for _ in points)

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()

            if data.get('code') == 'Ok' and data.get('matchings'):
                # Process all matchings (there may be multiple if gaps='split')
                all_coords = []
                total_distance = 0
                total_duration = 0
                confidences = []

                for matching in data['matchings']:
                    # Convert GeoJSON coordinates from [lon, lat] to (lat, lon)
                    coords_geojson = matching['geometry']['coordinates']
                    route_coords = [(coord[1], coord[0]) for coord in coords_geojson]
                    all_coords.extend(route_coords)

                    total_distance += matching['distance'] / 1000  # km
                    total_duration += matching['duration'] / 60  # minutes
                    confidences.append(matching.get('confidence', 0))

                # Get snapped waypoints (matched positions for input points)
                waypoints = data.get('tracepoints', [])
                snapped_coords = []
                for wp in waypoints:
                    if wp is not None and wp.get('location'):
                        snapped_coords.append((wp['location'][1], wp['location'][0]))  # lat, lon
                    else:
                        snapped_coords.append(None)

                return {
                    'coords': all_coords,
                    'distance': total_distance,
                    'duration': total_duration,
                    'confidence': sum(confidences) / len(confidences) if confidences else 0,
                    'snapped_coords': snapped_coords,
                    'waypoints': waypoints,
                    'num_matchings': len(data['matchings'])
                }

            # If match failed, try with larger radiuses
            elif data.get('code') == 'NoMatch':
                logging.info("OSRM match failed with default radius, trying with 100m radius...")
                params["radiuses"] = ";".join("100" for _ in points)
                response = requests.get(url, params=params, timeout=30)
                data = response.json()

                if data.get('code') == 'Ok' and data.get('matchings'):
                    all_coords = []
                    total_distance = 0
                    total_duration = 0
                    confidences = []

                    for matching in data['matchings']:
                        coords_geojson = matching['geometry']['coordinates']
                        route_coords = [(coord[1], coord[0]) for coord in coords_geojson]
                        all_coords.extend(route_coords)
                        total_distance += matching['distance'] / 1000
                        total_duration += matching['duration'] / 60
                        confidences.append(matching.get('confidence', 0))

                    waypoints = data.get('tracepoints', [])
                    snapped_coords = []
                    for wp in waypoints:
                        if wp is not None and wp.get('location'):
                            snapped_coords.append((wp['location'][1], wp['location'][0]))
                        else:
                            snapped_coords.append(None)

                    return {
                        'coords': all_coords,
                        'distance': total_distance,
                        'duration': total_duration,
                        'confidence': sum(confidences) / len(confidences) if confidences else 0,
                        'snapped_coords': snapped_coords,
                        'waypoints': waypoints,
                        'num_matchings': len(data['matchings'])
                    }

        except Exception as e:
            logging.error(f"OSRM match error: {e}")

        return None