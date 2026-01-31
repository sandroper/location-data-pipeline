import folium
from datetime import datetime
import pandas as pd
import os
import json
import logging


logger = logging.getLogger(__name__)

class RouteJsonOSRM:

    def save_routes_data(self, routes_data, stay_points_data, trajectory_points_data, total_distance, total_journey_time, start_time, end_time):
        """Save all route prediction data to a JSON file"""
        prediction_data = {
            'metadata': {
                'device_id': self.device_id,
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

    def create_enhanced_interactive_route_map(self, points_df: pd.DataFrame):
        """
        Create interactive route map using OSRM for routing.
        """
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

        logger.info \
            (f"Found {len(points_df)} points ({len(points_df[points_df['point_type'] == 'stay_point'])} stay points, {len(points_df[points_df['point_type'] == 'trajectory'])} trajectory points)")

        # Calculate map center
        center_lat = points_df['lat'].mean()
        center_lon = points_df['lon'].mean()

        # Create map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=13,
            tiles='OpenStreetMap'
        )

        # Group points into segments
        segments = self.group_points_into_segments(points_df, time_threshold_minutes=30)
        logger.info(f"Created {len(segments)} route segments")

        # Log segment details
        for i, segment in enumerate(segments):
            first_point = segment[0]
            last_point = segment[-1]
            segment_duration = (last_point['arrival_time'] - first_point['arrival_time']).total_seconds() / 60
            stay_count = sum(1 for p in segment if p['point_type'] == 'stay_point')
            traj_count = sum(1 for p in segment if p['point_type'] == 'trajectory')
            logger.info \
                (f"Segment { i +1}: {len(segment)} points ({stay_count} stay, {traj_count} trajectory), duration: {segment_duration:.1f}min")

        # Prepare visualization data
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
                'departure_time': point.get('departure_time', point[time_col]).strftime('%Y-%m-%d %H:%M:%S') if pd.notna
                    (point.get('departure_time', point[time_col])) else 'Unknown',
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

                logger.info \
                    (f"Creating route {segment_idx + 1} [{segment_type}]: {len(route_points)} points, actual time: {actual_travel_minutes:.1f}min")

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

                original_trajectory_coords = [list(coord) for coord in route_analysis.get('original_trajectory_coords', [])]
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
                    'mid_lon': mid_lon
                }

                routes_data.append(route_info)

        # Convert data to JSON for JavaScript
        stay_points_json = json.dumps(stay_points_data)
        routes_json = json.dumps(routes_data)
        trajectory_points_json = json.dumps(trajectory_points_data)

        # JavaScript for interactivity (same as original)
        interactive_js = f"""
        <script>
        var stayPoints = {stay_points_json};
        var routes = {routes_json};
        var trajectoryPoints = {trajectory_points_json};
    
        var currentSegmentIndex = 0;
        var currentDisplayMode = 0;
        var addedLayers = [];
        var myMap = null;
        var showingAllSegments = false;
        var segmentPoints = [];
    
        function initializeInteractivity() {{
            for (var key in window) {{
                if (key.startsWith('map_') && window[key] && window[key].on) {{
                    myMap = window[key];
                    break;
                }}
            }}
    
            if (!myMap) {{
                setTimeout(initializeInteractivity, 100);
                return;
            }}
    
            buildSegmentPointsData();
            myMap.on('click', function(e) {{ nextStep(); }});
            updateStatus();
        }}
    
        function buildSegmentPointsData() {{
            segmentPoints = [];
            routes.forEach(function(route, routeIdx) {{
                var points = [];
                route.coords.forEach(function(coord, idx) {{
                    points.push({{ lat: coord[0], lon: coord[1], index: idx, isRoutePoint: true }});
                }});
    
                var sumLat = 0, sumLon = 0;
                points.forEach(function(p) {{ sumLat += p.lat; sumLon += p.lon; }});
                var centerLat = sumLat / points.length;
                var centerLon = sumLon / points.length;
    
                segmentPoints.push({{
                    routeIndex: routeIdx, route: route, points: points,
                    centerLat: centerLat, centerLon: centerLon, totalPoints: points.length
                }});
            }});
        }}
    
        function clearAllLayers() {{
            if (!myMap) return;
            addedLayers.forEach(function(layer) {{ myMap.removeLayer(layer); }});
            addedLayers = [];
        }}
    
        function centerMapOnSegment(segmentIdx) {{
            if (!myMap || segmentIdx >= segmentPoints.length) return;
            var segment = segmentPoints[segmentIdx];
    
            var minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity;
            segment.points.forEach(function(p) {{
                minLat = Math.min(minLat, p.lat); maxLat = Math.max(maxLat, p.lat);
                minLon = Math.min(minLon, p.lon); maxLon = Math.max(maxLon, p.lon);
            }});
    
            var latPadding = (maxLat - minLat) * 0.2 || 0.005;
            var lonPadding = (maxLon - minLon) * 0.2 || 0.005;
            var bounds = [[minLat - latPadding, minLon - lonPadding], [maxLat + latPadding, maxLon + lonPadding]];
            myMap.fitBounds(bounds, {{ animate: true, duration: 0.5 }});
        }}
    
        function isStayPointAtCoords(lat, lon) {{
            var tolerance = 0.00001;
            return stayPoints.some(function(sp) {{
                return Math.abs(sp.lat - lat) < tolerance && Math.abs(sp.lon - lon) < tolerance;
            }});
        }}
    
        function showEntireSegment(segmentIdx) {{
            if (!myMap || segmentIdx >= segmentPoints.length) return;
            var segment = segmentPoints[segmentIdx];
            var route = segment.route;
    
            var routeCoords = segment.points.map(function(p) {{ return [p.lat, p.lon]; }});
            var routeLine = L.polyline(routeCoords, {{
                color: '#5B2C6F', weight: 6, opacity: 0.9,
                dashArray: route.divergence_detected ? '8, 4' : null
            }}).bindPopup('<b>Segment ' + (segmentIdx + 1) + '</b><br>Distance: ' + route.distance + ' km<br>OSRM Confidence: ' + (route.osrm_confidence * 100).toFixed(0) + '%');
            routeLine.addTo(myMap);
            addedLayers.push(routeLine);
    
            var startIsStayPoint = isStayPointAtCoords(segment.points[0].lat, segment.points[0].lon);
            var startMarker = L.circleMarker([segment.points[0].lat, segment.points[0].lon], {{
                radius: startIsStayPoint ? 12 : 10,
                color: startIsStayPoint ? '#6B2D8B' : 'darkgreen',
                weight: startIsStayPoint ? 3 : 2,
                fillColor: startIsStayPoint ? '#9B59B6' : '#28a745',
                fillOpacity: 0.9
            }}).bindTooltip('Segment ' + (segmentIdx + 1) + ' Start');
            startMarker.addTo(myMap);
            addedLayers.push(startMarker);
    
            var lastPoint = segment.points[segment.points.length - 1];
            var endIsStayPoint = isStayPointAtCoords(lastPoint.lat, lastPoint.lon);
            var endMarker = L.circleMarker([lastPoint.lat, lastPoint.lon], {{
                radius: endIsStayPoint ? 12 : 10,
                color: endIsStayPoint ? '#6B2D8B' : 'darkred',
                weight: endIsStayPoint ? 3 : 2,
                fillColor: endIsStayPoint ? '#9B59B6' : '#dc3545',
                fillOpacity: 0.9
            }}).bindTooltip('Segment ' + (segmentIdx + 1) + ' End');
            endMarker.addTo(myMap);
            addedLayers.push(endMarker);
        }}
    
        function showAllSegments() {{
            if (!myMap) return;
            clearAllLayers();
    
            segmentPoints.forEach(function(segment, idx) {{ showEntireSegment(idx); }});
    
            stayPoints.forEach(function(sp) {{
                var markerColor = sp.is_segment_boundary ? '#6B2D8B' : sp.color;
                var fillColor = sp.is_segment_boundary ? '#9B59B6' : sp.color;
                var radius = sp.is_segment_boundary ? Math.max(sp.radius, 12) : sp.radius;
    
                var stayMarker = L.circleMarker([sp.lat, sp.lon], {{
                    radius: radius, color: markerColor, weight: sp.is_segment_boundary ? 3 : 2,
                    fillColor: fillColor, fillOpacity: 0.85
                }}).bindPopup('<b>Stay Point ' + sp.visit_number + '</b><br>Duration: ' + sp.stay_duration);
                stayMarker.addTo(myMap);
                addedLayers.push(stayMarker);
            }});
    
            var allCoords = [];
            segmentPoints.forEach(function(segment) {{
                segment.points.forEach(function(p) {{ allCoords.push([p.lat, p.lon]); }});
            }});
    
            if (allCoords.length > 0) {{
                var bounds = L.latLngBounds(allCoords);
                myMap.fitBounds(bounds.pad(0.1), {{ animate: true }});
            }}
    
            showingAllSegments = true;
        }}
    
        function nextStep() {{
            if (showingAllSegments) {{
                clearAllLayers();
                currentSegmentIndex = 0;
                currentDisplayMode = 0;
                showingAllSegments = false;
                updateStatus();
                return;
            }}
    
            if (segmentPoints.length === 0) return;
    
            if (currentDisplayMode === 0) {{
                clearAllLayers();
                centerMapOnSegment(currentSegmentIndex);
                showInterpolatedRoute(currentSegmentIndex);
                currentDisplayMode = 1;
                updateStatus();
            }} else if (currentDisplayMode === 1) {{
                showGPSAndSnappedPoints(currentSegmentIndex);
                currentDisplayMode = 2;
                updateStatus();
            }} else if (currentDisplayMode === 2) {{
                currentSegmentIndex++;
                currentDisplayMode = 0;
    
                if (currentSegmentIndex >= segmentPoints.length) {{
                    showAllSegments();
                    updateStatus();
                    return;
                }}
    
                clearAllLayers();
                centerMapOnSegment(currentSegmentIndex);
                showInterpolatedRoute(currentSegmentIndex);
                currentDisplayMode = 1;
                updateStatus();
            }}
        }}
    
        function showInterpolatedRoute(segmentIdx) {{
            if (!myMap || segmentIdx >= segmentPoints.length) return;
            var segment = segmentPoints[segmentIdx];
            var route = segment.route;
    
            var routeCoords = segment.points.map(function(p) {{ return [p.lat, p.lon]; }});
            var routeLine = L.polyline(routeCoords, {{
                color: '#5B2C6F', weight: 6, opacity: 0.9,
                dashArray: route.divergence_detected ? '8, 4' : null
            }}).bindPopup('<b>Segment ' + (segmentIdx + 1) + ' - OSRM Matched Route</b><br>Distance: ' + route.distance + ' km<br>OSRM Confidence: ' + (route.osrm_confidence * 100).toFixed(0) + '%');
            routeLine.addTo(myMap);
            addedLayers.push(routeLine);
    
            var startIsStayPoint = isStayPointAtCoords(segment.points[0].lat, segment.points[0].lon);
            var startMarker = L.circleMarker([segment.points[0].lat, segment.points[0].lon], {{
                radius: startIsStayPoint ? 12 : 10,
                color: startIsStayPoint ? '#6B2D8B' : 'darkgreen',
                weight: startIsStayPoint ? 3 : 2,
                fillColor: startIsStayPoint ? '#9B59B6' : '#28a745',
                fillOpacity: 0.9
            }}).bindPopup('<b>Segment ' + (segmentIdx + 1) + ' Start</b><br>Departure: ' + route.departure_time);
            startMarker.addTo(myMap);
            addedLayers.push(startMarker);
    
            var lastPoint = segment.points[segment.points.length - 1];
            var endIsStayPoint = isStayPointAtCoords(lastPoint.lat, lastPoint.lon);
            var endMarker = L.circleMarker([lastPoint.lat, lastPoint.lon], {{
                radius: endIsStayPoint ? 12 : 10,
                color: endIsStayPoint ? '#6B2D8B' : 'darkred',
                weight: endIsStayPoint ? 3 : 2,
                fillColor: endIsStayPoint ? '#9B59B6' : '#dc3545',
                fillOpacity: 0.9
            }}).bindPopup('<b>Segment ' + (segmentIdx + 1) + ' End</b><br>Arrival: ' + route.arrival_time);
            endMarker.addTo(myMap);
            addedLayers.push(endMarker);
        }}
    
        function showGPSAndSnappedPoints(segmentIdx) {{
            if (!myMap || segmentIdx >= segmentPoints.length) return;
            var segment = segmentPoints[segmentIdx];
            var route = segment.route;
    
            if (route.original_trajectory_coords && route.original_trajectory_coords.length > 0) {{
                var origLine = L.polyline(route.original_trajectory_coords, {{
                    color: '#0066CC', weight: 4, opacity: 0.8, dashArray: '8, 6'
                }}).bindPopup('Original GPS Trajectory');
                origLine.addTo(myMap);
                addedLayers.push(origLine);
    
                var trajPoints = trajectoryPoints.filter(function(tp) {{ return tp.segment_id === segmentIdx; }});
    
                route.original_trajectory_coords.forEach(function(coord, idx) {{
                    var timeInfo = trajPoints[idx] ? '<br>Time: ' + trajPoints[idx].timestamp : '';
                    var origMarker = L.circleMarker([coord[0], coord[1]], {{
                        radius: 10, color: '#003366', weight: 3, fillColor: '#3399FF', fillOpacity: 0.9
                    }}).bindPopup('<b>GPS Point ' + (idx + 1) + '</b>' + timeInfo);
                    origMarker.addTo(myMap);
                    addedLayers.push(origMarker);
                }});
            }}
    
            if (route.snapped_coords && route.snapped_coords.length > 0) {{
                route.snapped_coords.forEach(function(coord, idx) {{
                    if (!coord) return;
                    var snappedMarker = L.circleMarker([coord[0], coord[1]], {{
                        radius: 7, color: '#994400', weight: 2, fillColor: '#FF9933', fillOpacity: 0.9
                    }}).bindPopup('<b>OSRM Snapped Point ' + (idx + 1) + '</b>');
                    snappedMarker.addTo(myMap);
                    addedLayers.push(snappedMarker);
    
                    if (route.original_trajectory_coords && route.original_trajectory_coords[idx]) {{
                        var origCoord = route.original_trajectory_coords[idx];
                        var connectionLine = L.polyline([[origCoord[0], origCoord[1]], [coord[0], coord[1]]], {{
                            color: '#9933FF', weight: 3, opacity: 0.9, dashArray: '4, 4'
                        }});
                        connectionLine.addTo(myMap);
                        addedLayers.push(connectionLine);
                    }}
                }});
            }}
    
            var startMarker = L.circleMarker([segment.points[0].lat, segment.points[0].lon], {{
                radius: 16, color: '#155724', weight: 4, fillColor: '#28a745', fillOpacity: 0.95
            }}).bindPopup('<b>START</b>').bindTooltip('START');
            startMarker.addTo(myMap);
            addedLayers.push(startMarker);
    
            var lastPoint = segment.points[segment.points.length - 1];
            var endMarker = L.circleMarker([lastPoint.lat, lastPoint.lon], {{
                radius: 16, color: '#721c24', weight: 4, fillColor: '#dc3545', fillOpacity: 0.95
            }}).bindPopup('<b>END</b>').bindTooltip('END');
            endMarker.addTo(myMap);
            addedLayers.push(endMarker);
        }}
    
        function updateStatus() {{
            var statusDiv = document.getElementById('status');
            if (!statusDiv) return;
    
            if (showingAllSegments) {{
                statusDiv.innerHTML = '<b>All Segments Displayed!</b> (' + segmentPoints.length + ' segments) - Click to restart';
            }} else if (segmentPoints.length === 0) {{
                statusDiv.innerHTML = '<b>No segments to display</b>';
            }} else if (currentSegmentIndex >= segmentPoints.length) {{
                statusDiv.innerHTML = '<b>Journey Complete!</b> - Click to see all segments';
            }} else {{
                var segment = segmentPoints[currentSegmentIndex];
                var modeText = '', nextAction = '';
    
                if (currentDisplayMode === 0) {{
                    modeText = 'Ready';
                    nextAction = 'Click to show OSRM matched route';
                }} else if (currentDisplayMode === 1) {{
                    modeText = '<span style="color: ' + segment.route.color + ';">OSRM Route</span> (Conf: ' + (segment.route.osrm_confidence * 100).toFixed(0) + '%)';
                    nextAction = 'Click to show GPS points & snapped points';
                }} else if (currentDisplayMode === 2) {{
                    modeText = '<span style="color: #3399FF;">GPS</span> + <span style="color: #FF9933;">Snapped</span>';
                    nextAction = currentSegmentIndex < segmentPoints.length - 1 ? 'Click for next segment' : 'Click to see all segments';
                }}
    
                statusDiv.innerHTML = '<b>Segment ' + (currentSegmentIndex + 1) + '/' + segmentPoints.length + '</b> | ' + 
                    modeText + ' | Distance: ' + segment.route.distance + ' km<br>' +
                    '<span style="font-size: 12px; color: #666;">' + nextAction + '</span>';
            }}
        }}
    
        setTimeout(initializeInteractivity, 500);
        </script>
        """

        status_html = """
        <div id="status" style="position: fixed; top: 20px; left: 50%; transform: translateX(-50%); 
                                background-color: rgba(255, 255, 255, 0.9); padding: 10px 20px; 
                                border-radius: 5px; border: 2px solid #333; z-index: 9999; 
                                font-size: 16px; text-align: center; box-shadow: 0 2px 10px rgba(0,0,0,0.3);">
            <b>OSRM Route Prediction - Click anywhere to start</b>
        </div>
        """

        # Create route legend
        route_details = ""
        for i, route in enumerate(routes_data):
            accuracy_color = "#28a745" if route.get('time_accuracy', 999) < 10 else "#ffc107" if route.get('time_accuracy', 999) < 30 else "#dc3545"
            accuracy_icon = "✓" if route.get('time_accuracy', 999) < 10 else "~" if route.get('time_accuracy', 999) < 30 else "✗"

            osrm_conf = route.get('osrm_confidence', 0)
            conf_color = "#28a745" if osrm_conf > 0.8 else "#ffc107" if osrm_conf > 0.5 else "#dc3545"

            origin_label = f"POI {route['start_cluster_id']}" if route.get('start_cluster_id') else "Trajectory Start"
            dest_label = f"POI {route['end_cluster_id']}" if route.get('end_cluster_id') else "Trajectory End"

            route_details += f'''
            <div style="margin: 2px 0; padding: 3px; border-left: 3px solid {route['color']}; font-size: 11px;">
                <b>Route {route['route_number']}:</b> {origin_label} → {dest_label}
                <span style="color: {accuracy_color}; font-weight: bold;">{accuracy_icon}</span>
                <span style="color: {conf_color}; font-size: 10px;">[OSRM: {osrm_conf *100:.0f}%]</span><br>
                <span style="color: #666;">Distance: {route['distance']} km | Speed: {route.get('actual_speed_kmh', 0)} km/h</span><br>
                <span style="color: #888; font-size: 10px;">Actual: {route.get('actual_duration', 0)}min | Est: {route.get('estimated_duration', 0)}min</span>
            </div>'''

        legend_html = f'''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 380px; max-height: 350px;
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:12px; padding: 10px;">
        <div style="margin-bottom: 5px;"><b>OSRM Route Prediction</b></div>
        <div style="margin-bottom: 5px; color: #333; font-size: 11px;">
            <b>Period:</b> {self.start_date} to {self.end_date}
        </div>
        <div style="margin-bottom: 8px; color: #666; font-size: 10px; border-bottom: 1px solid #ddd; padding-bottom: 5px;">
            <span style="color: #9B59B6;">●</span> Stay Points (purple)<br>
            <span style="color: #3399FF;">●</span> Original GPS Points (blue)<br>
            <span style="color: #FF9933;">●</span> OSRM Snapped Points (orange)<br>
            <span style="color: #5B2C6F;">───</span> OSRM Matched Route
        </div>
        <div style="max-height: 200px; overflow-y: auto;">
            {route_details}
        </div>
        </div>
        '''

        # Add HTML elements to map
        m.get_root().html.add_child(folium.Element(status_html))
        m.get_root().html.add_child(folium.Element(legend_html))
        m.get_root().html.add_child(folium.Element(interactive_js))

        # Save map
        m.save(self.output_file)

        # Save routes data
        routes_output = self.save_routes_data(routes_data, stay_points_data, trajectory_points_data, total_distance, total_journey_time, start_time, end_time)

        logger.info(f"{'= ' * 60}")
        logger.info(f"OSRM Route Prediction Complete!")
        logger.info(f"{'= ' * 60}")
        logger.info(f"Device ID: {self.device_id}")
        logger.info(f"Stay Points: {len(stay_points_data)}")
        logger.info(f"Trajectory Points: {len(trajectory_points_data)}")
        logger.info(f"Route Segments: {len(routes_data)}")
        logger.info(f"Total Distance: {total_distance:.3f} km")
        logger.info(f"Total Journey Time: {total_journey_time}")
        logger.info(f"Map saved to: {self.output_file}")
        logger.info(f"Data saved to: {routes_output}")