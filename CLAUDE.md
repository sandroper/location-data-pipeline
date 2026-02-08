# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A PySpark 4.1 pipeline for processing location data from Snowflake. Processes raw GPS coordinates through cleansing, stay point detection, clustering, and route prediction stages.

## Build & Run Commands

```bash
# Install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate

# Start Spark cluster (from docker/ directory)
cd docker && docker compose up -d

# Run pure Spark pipeline (recommended for large datasets)
./spark/spark-submit-location-pipeline_ps.sh

# Run Pandas-based pipeline (smaller datasets)
./spark/spark-submit-location-pipeline.sh

# Use different environment file
ENV_FILE=.env.prod ./spark/spark-submit-location-pipeline_ps.sh
```

## Testing

```bash
# Run all tests
uv run pytest

# Run single test file
uv run pytest tests/test_data_cleanser_cleanse.py

# Run with coverage
uv run pytest --cov=src
```

## Architecture

### Two Pipeline Implementations

The codebase has parallel implementations - Pure Spark (`*_ps.py`) and Pandas-based versions:

| Stage | Pure Spark | Pandas-based |
|-------|-----------|--------------|
| Orchestration | `spark_location_pipeline_ps.py` | `spark_location_pipeline.py` |
| Data Cleansing | `data_cleanser_ps.py` | `data_cleanser.py` |
| Stay Point Detection | `points_qualifier_ps.py` | `points_qualifier.py` |
| Clustering | `cluster_stay_points_ps.py` | `cluster_stay_points.py` |

Pure Spark versions use window functions with `partitionBy('device_id')` throughout. Pandas versions collect data to driver.

### Pipeline Flow

1. **Data Cleansing** - Removes invalid coordinates, duplicates, stationary outliers
2. **Stay Point Detection** - Identifies locations where device remained stationary (uses `DIST_THRESHOLD_M` and `TIME_THRESHOLD_MIN`)
3. **Clustering** - Groups nearby stay points using grid-based spatial clustering (uses `CLUSTERING_EPS`)
4. **Route Prediction** - Generates routes between clusters via OSRM

### Key Patterns

**Device Isolation**: All Pure Spark operations partition by `device_id` to prevent cross-contamination:
```python
window_spec = Window.partitionBy('device_id').orderBy('event_ts')
```

**Configuration**: Pipeline parameters loaded from environment via `LocationPipelineConfig` (`src/utils/location_pipeline_config_manager.py`). Snowflake credentials via `SnowflakeConfig` (`src/snowflake_config_manager.py`).

**Debug Output**: Use `DataFrameDumper` to dump intermediate DataFrames:
```python
from utils.dataframe_dumper import DataFrameDumper
dumper = DataFrameDumper()
dumper.dump(df, "step_name")  # Writes to PIPELINE_OUTPUT_DIR
```

### Infrastructure

- **Spark Cluster**: Docker Compose with master + 2 workers (`docker/docker-compose.yml`)
- **OSRM Server**: Routing engine for trajectory prediction (port 5001)
- **Spark History Server**: Job history at http://localhost:18080

## Remote Deployment

```bash
# Configure remote server (or edit scripts/deploy.sh)
export DEPLOY_USER=myuser
export DEPLOY_HOST=myserver.com
export DEPLOY_PATH=/home/myuser/location-data-pipeline

# Pack virtual environment (includes all Python dependencies)
./scripts/pack-venv.sh

# Sync source files + packed venv to remote
./scripts/deploy.sh

# Sync and run pipeline
./scripts/deploy.sh --run

# Sync with specific env file (deployed as .env on remote)
./scripts/deploy.sh --env .env --run
./scripts/deploy.sh --env .env.prod --run

# Watch mode - auto-sync on file changes
./scripts/deploy.sh --watch

# Dry run - see what would be transferred
./scripts/deploy.sh --dry-run
```

The packed venv (`pyspark_venv.tar.gz`) is shipped to executors via `--archives`, eliminating the need to install dependencies on remote servers.

## Configuration

Environment variables in `.env` (copy from `.env.example`):

- `SNOWFLAKE_*` - Database connection credentials
- `DIST_THRESHOLD_M` (100) - Stay point distance threshold in meters
- `TIME_THRESHOLD_MIN` (10) - Minimum stay duration in minutes
- `CLUSTERING_EPS` (100) - Grid cell size for clustering in meters
- `OSRM_SERVER` - Routing server URL
- `PIPELINE_OUTPUT_DIR` - Debug CSV output location

## Commit Preferences

- Do not add "Co-Authored-By" lines to commit messages
