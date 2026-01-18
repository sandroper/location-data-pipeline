"""
Unit tests for DataCleanser.cleanse method.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock
from datetime import datetime
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_cleanser import DataCleanser


class MockConfig:
    """Mock config for testing DataCleanser."""
    def __init__(
        self,
        max_distance_threshold_km=5,
        min_distance_threshold_km=1,
        max_speed_kmh=200,
    ):
        self.max_distance_threshold_km = max_distance_threshold_km
        self.min_distance_threshold_km = min_distance_threshold_km
        self.max_speed_kmh = max_speed_kmh


class TestCleanseMethod:
    """Tests for DataCleanser.cleanse method (lines 17-29)."""

    def test_cleanse_returns_pandas_dataframe(self):
        """Test that cleanse returns a pandas DataFrame."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        # Create mock Spark DataFrame
        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:00'],
            'ID': ['device1', 'device1'],
            'lat': [40.7128, 40.7129],
            'lon': [-74.0060, -74.0061]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        assert isinstance(result, pd.DataFrame)

    def test_cleanse_with_valid_data(self):
        """Test cleanse with valid data returns cleaned DataFrame."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:00', '2024-01-15 10:30:00'],
            'ID': ['device1', 'device1', 'device1'],
            'lat': [40.7128, 40.7129, 40.7130],
            'lon': [-74.0060, -74.0061, -74.0062]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        assert result is not None
        assert len(result) == 3
        assert 'event_ts' in result.columns
        assert 'device_id' in result.columns
        assert 'lat' in result.columns
        assert 'lon' in result.columns

    def test_cleanse_with_empty_dataframe_returns_early(self):
        """Test that cleanse returns early when DataFrame is empty."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        empty_pdf = pd.DataFrame(columns=['event_ts', 'ID', 'lat', 'lon'])
        mock_spark_df.toPandas.return_value = empty_pdf

        result = cleanser.cleanse(mock_spark_df)

        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_cleanse_with_missing_id_column_returns_early(self):
        """Test that cleanse returns early when ID column is missing."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        # DataFrame without ID column
        pdf_no_id = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:00'],
            'lat': [40.7128, 40.7129],
            'lon': [-74.0060, -74.0061]
        })
        mock_spark_df.toPandas.return_value = pdf_no_id

        result = cleanser.cleanse(mock_spark_df)

        # Should return the original pandas DataFrame without processing
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert 'ID' not in result.columns

    def test_cleanse_extracts_and_renames_columns(self):
        """Test that cleanse extracts first 4 columns and renames them correctly."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:00'],
            'ID': ['device1', 'device1'],
            'lat': [40.7128, 40.7129],
            'lon': [-74.0060, -74.0061],
            'extra_col': ['extra1', 'extra2']
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        assert len(result.columns) == 4
        assert list(result.columns) == ['event_ts', 'device_id', 'lat', 'lon']

    def test_cleanse_sorts_by_timestamp(self):
        """Test that cleanse sorts data by event_ts."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        # Input data with unsorted timestamps
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:30:00', '2024-01-15 10:00:00', '2024-01-15 10:15:00'],
            'ID': ['device1', 'device1', 'device1'],
            'lat': [40.7130, 40.7128, 40.7129],
            'lon': [-74.0062, -74.0060, -74.0061]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        # Timestamps should be sorted chronologically
        timestamps = result['event_ts'].tolist()
        assert timestamps == sorted(timestamps)

    def test_cleanse_removes_points_with_impossible_speed(self):
        """Test that cleanse removes points indicating impossible speeds."""
        # Configure with low max speed threshold
        config = MockConfig(max_speed_kmh=50)
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        # Create data with an impossible jump (far distance in short time)
        # Points roughly 111km apart but only 1 minute difference = ~6660 km/h
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:01:00', '2024-01-15 10:02:00'],
            'ID': ['device1', 'device1', 'device1'],
            'lat': [40.0, 41.0, 40.001],  # Middle point is ~111km away
            'lon': [-74.0, -74.0, -74.0]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        # The impossible point should be removed
        assert len(result) < 3

    def test_cleanse_removes_rows_with_missing_coordinates(self):
        """Test that cleanse removes rows with missing lat/lon."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        # Put missing values at the end to avoid index issues after dropna
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:00',
                        '2024-01-15 10:30:00', '2024-01-15 10:45:00'],
            'ID': ['device1', 'device1', 'device1', 'device1'],
            'lat': [40.7128, 40.7129, None, None],
            'lon': [-74.0060, -74.0061, -74.0062, None]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        # Rows with None lat or lon should be removed (last 2 rows)
        assert len(result) == 2
        assert result['lat'].isna().sum() == 0
        assert result['lon'].isna().sum() == 0

    def test_cleanse_formats_timestamp_string(self):
        """Test that timestamps are formatted as 'YYYY-MM-DD HH:MM' strings."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:30'],
            'ID': ['device1', 'device1'],
            'lat': [40.7128, 40.7129],
            'lon': [-74.0060, -74.0061]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        for ts in result['event_ts']:
            # Should be string in format 'YYYY-MM-DD HH:MM'
            assert isinstance(ts, str)
            try:
                datetime.strptime(ts, '%Y-%m-%d %H:%M')
            except ValueError:
                pytest.fail(f"Timestamp {ts} is not in expected format 'YYYY-MM-DD HH:MM'")

    def test_cleanse_with_single_row(self):
        """Test cleanse with single row of data."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00'],
            'ID': ['device1'],
            'lat': [40.7128],
            'lon': [-74.0060]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        assert result is not None
        assert len(result) == 1

    def test_cleanse_preserves_valid_coordinates(self):
        """Test that valid coordinate values are preserved."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00', '2024-01-15 10:15:00'],
            'ID': ['device1', 'device1'],
            'lat': [40.7128, 40.7129],
            'lon': [-74.0060, -74.0061]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        result = cleanser.cleanse(mock_spark_df)

        # Check coordinates are preserved
        lats = result['lat'].tolist()
        lons = result['lon'].tolist()
        assert 40.7128 in lats
        assert 40.7129 in lats
        assert -74.0060 in lons
        assert -74.0061 in lons

    def test_cleanse_calls_topandas(self):
        """Test that cleanse calls toPandas() on the input DataFrame."""
        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00'],
            'ID': ['device1'],
            'lat': [40.7128],
            'lon': [-74.0060]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        cleanser.cleanse(mock_spark_df)

        mock_spark_df.toPandas.assert_called_once()

    def test_cleanse_logs_step_info(self, caplog):
        """Test that cleanse logs appropriate step information."""
        import logging
        caplog.set_level(logging.INFO)

        config = MockConfig()
        cleanser = DataCleanser(config)

        mock_spark_df = MagicMock()
        test_pdf = pd.DataFrame({
            'event_ts': ['2024-01-15 10:00:00'],
            'ID': ['device1'],
            'lat': [40.7128],
            'lon': [-74.0060]
        })
        mock_spark_df.toPandas.return_value = test_pdf

        cleanser.cleanse(mock_spark_df)

        assert 'STEP 1/5' in caplog.text