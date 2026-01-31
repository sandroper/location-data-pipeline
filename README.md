# Location Data Pipeline

A PySpark-based pipeline for processing location data from Snowflake, built on **Apache Spark 4.1**. The pipeline cleanses raw location data, identifies stay points vs trajectory points, clusters stay points, and generates route predictions using OSRM.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [Configuration](#configuration)
- [Starting the Spark Cluster](#starting-the-spark-cluster)
- [Running the Pipeline](#running-the-pipeline)
- [Remote Deployment](#remote-deployment)
- [Pipeline Architecture](#pipeline-architecture)
- [Debugging and Output](#debugging-and-output)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

### 1. UV Package Manager

This project uses [uv](https://github.com/astral-sh/uv) for Python dependency management.

**Check if uv is installed:**
```bash
uv --version
```

**Install uv (if not installed):**
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or via Homebrew (macOS)
brew install uv
```

### 2. Docker

Docker is required to run the Spark cluster.

**Check if Docker is installed and running:**
```bash
docker --version
docker info
```

**Install Docker:**
- macOS: [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
- Linux: [Docker Engine](https://docs.docker.com/engine/install/)
- Windows: [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

### 3. Java 21

Java 21 is required by Apache Spark 4.1.

**Check Java version:**
```bash
java -version
```

Ensure you have Java 21 installed. The Docker containers use Java 21 (`java21-ubuntu` base image).

### 4. Apache Spark 4.1 (Local Installation)

This project requires **Apache Spark 4.1.x**. Spark must be installed locally to use `spark-submit` from your host machine.

**Check if Spark is installed and verify version:**
```bash
spark-submit --version
# Should show version 4.1.x
```

**Install Spark 4.1:**
```bash
# macOS via Homebrew
brew install apache-spark

# Or download Spark 4.1.0 from https://spark.apache.org/downloads.html
# and add to PATH
```

> **Note:** The Docker containers use `apache/spark:4.1.0-scala2.13-java21-ubuntu`. Ensure your local Spark installation is compatible (4.1.x recommended).

---

## Project Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd location-data-pipeline
```

### 2. Create Virtual Environment and Install Dependencies

```bash
# Create virtual environment and install all dependencies
uv sync

# Activate the virtual environment
source .venv/bin/activate
```

### 3. Verify Installation

```bash
# Check that PySpark is available
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
```

---

## Configuration

### Environment Variables

The pipeline configuration is stored in a `.env` file in the project root.

**Create your `.env` file from the example:**
```bash
cp .env.example .env
```

**Edit `.env` with your settings:**

```bash
# Snowflake Connection Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_URL=your_url
SNOWFLAKE_USER=your_username
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_TABLE=your_table

# Private key authentication
SNOWFLAKE_KEY_PATH=path/to/your/key.p8

# Pipeline Configuration (optional - defaults shown)
MAX_DISTANCE_THRESHOLD_KM=5
MIN_DISTANCE_THRESHOLD_KM=1
MAX_SPEED_KMH=200
DIST_THRESHOLD_M=100
TIME_THRESHOLD_MIN=10
TIME_ZONE=UTC
CLUSTERING_EPS=100
CLUSTERING_MIN_SAMPLES=1
CENTROID_METHOD=weighted_average
OSRM_SERVER=http://127.0.0.1:5001

# Output Configuration
OUTPUT_DATA_DIR=./data/output
SAVE_ROUTES_JSON=false

# Debug output directory (optional)
PIPELINE_OUTPUT_DIR=/tmp/spark-pipeline-output
```

### Snowflake Private Key

Place your Snowflake private key file (`.p8`) in one of these locations:
- Project root: `./sf_k2ib_key.p8`
- Spark directory: `./spark/sf_k2ib_key.p8`
- Custom path specified in `SNOWFLAKE_KEY_PATH`

### Pipeline Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DIST_THRESHOLD_M` | 100 | Distance threshold (meters) for stay point detection |
| `TIME_THRESHOLD_MIN` | 10 | Minimum duration (minutes) to qualify as a stay point |
| `CLUSTERING_EPS` | 100 | Grid cell size (meters) for clustering |
| `CLUSTERING_MIN_SAMPLES` | 1 | Minimum points per cluster |
| `CENTROID_METHOD` | weighted_average | Centroid calculation: `average`, `weighted_average`, or `max_points` |
| `OSRM_SERVER` | http://osrm:5000 | OSRM routing server URL |
| `SAVE_ROUTES_JSON` | false | Save route prediction results to JSON file |
| `OUTPUT_DATA_DIR` | ./data/output | Directory for JSON output files |

---

## Starting the Spark Cluster

The Spark cluster runs in Docker containers managed by Docker Compose, using the official **Apache Spark 4.1.0** image with Scala 2.13 and Java 21.

### 1. Navigate to Docker Directory

```bash
cd docker
```

### 2. Build the Docker Image

```bash
docker compose build
```

### 3. Start the Cluster

```bash
docker compose up -d
```

This starts:
- **spark-master**: Spark Master node (Web UI: http://localhost:8080)
- **spark-worker-1**: Spark Worker node (Web UI: http://localhost:8081)
- **spark-worker-2**: Spark Worker node (Web UI: http://localhost:8082)
- **spark-history**: Spark History Server (Web UI: http://localhost:18080)
- **osrm**: OSRM routing server (API: http://localhost:5001)

### 4. Verify the Cluster is Running

```bash
# Check container status
docker compose ps

# Check Spark Master logs
docker compose logs spark-master

# Open Spark Master Web UI
open http://localhost:8080
```

### 5. Stop the Cluster

```bash
docker compose down
```

---

## Running the Pipeline

There are two versions of the pipeline:

| Script | Pipeline File | Description |
|--------|---------------|-------------|
| `spark-submit-location-pipeline_ps.sh` | `spark_location_pipeline_ps.py` | **Pure Spark** - Uses only PySpark DataFrame operations |
| `spark-submit-location-pipeline.sh` | `spark_location_pipeline.py` | **Pandas-based** - Uses Pandas for some transformations |

### Option 1: Pure Spark Pipeline (Recommended for Large Datasets)

```bash
# From project root
./spark/spark-submit-location-pipeline_ps.sh
```

This pipeline uses pure PySpark operations throughout, making it suitable for large-scale distributed processing. All operations are partitioned by `device_id` to ensure data isolation between devices.

### Option 2: Pandas-based Pipeline

```bash
# From project root
./spark/spark-submit-location-pipeline.sh
```

This pipeline uses Pandas DataFrames for some transformations, which may be more familiar but collects data to the driver. Suitable for smaller datasets.

### Using Different Environment Files

You can specify a different `.env` file for different environments:

```bash
# Use production configuration
ENV_FILE=.env.prod ./spark/spark-submit-location-pipeline_ps.sh

# Use development configuration
ENV_FILE=.env.dev ./spark/spark-submit-location-pipeline.sh
```

---

## Remote Deployment

This section covers deploying and running the pipeline on a remote Spark cluster without manual dependency installation on remote servers.

### Packaging Dependencies with venv-pack

The project uses [venv-pack](https://jcrist.github.io/venv-pack/) to create a portable archive of the Python virtual environment. This archive is shipped to Spark executors via the `--archives` option, eliminating the need to install dependencies on remote servers.

#### 1. Install Development Dependencies

```bash
uv sync --group dev
```

#### 2. Pack the Virtual Environment

```bash
./scripts/pack-venv.sh
```

This creates `pyspark_venv.tar.gz` in the project root, containing all Python dependencies (~200-400MB depending on packages).

#### 3. When to Re-pack

Re-run `./scripts/pack-venv.sh` after:
- Adding or updating dependencies in `pyproject.toml`
- Running `uv sync` to update packages

### Deploy Script

The `scripts/deploy.sh` script syncs code and the packed virtual environment to a remote server via rsync.

#### Setup

```bash
# Configure remote server (one-time, or add to shell profile)
export DEPLOY_USER=your_username
export DEPLOY_HOST=your_server.com
export DEPLOY_PATH=/home/your_username/location-data-pipeline
```

#### Usage

```bash
# Sync source files and packed venv to remote
./scripts/deploy.sh

# Sync with a specific env file (deployed as .env on remote)
./scripts/deploy.sh --env .env.prod

# Sync and run the pipeline
./scripts/deploy.sh --env .env.prod --run

# Watch mode - auto-sync on file changes (requires fswatch)
./scripts/deploy.sh --watch

# Dry run - see what would be transferred
./scripts/deploy.sh --dry-run
```

### Running on Remote Cluster

For remote Spark clusters (not local Docker), use the cluster-specific submit script:

```bash
# On the remote server
./spark/spark-submit-location-pipeline_ps_remote.sh
```

This script:
- Requires `pyspark_venv.tar.gz` to exist (will error if missing)
- Uses `--deploy-mode cluster` (driver runs on a worker node)
- Ships the packed venv via `--archives`
- Configures `spark.pyspark.python` and `spark.pyspark.driver.python` to use the unpacked venv

### Complete Deployment Workflow

```bash
# 1. Make code changes locally

# 2. Pack venv (only needed after dependency changes)
./scripts/pack-venv.sh

# 3. Deploy to remote server
./scripts/deploy.sh --env .env.prod

# 4. SSH to remote and run (or use --run flag in step 3)
ssh your_server
cd location-data-pipeline
./spark/spark-submit-location-pipeline_ps_remote.sh
```

### Iterative Development

For rapid iteration during development:

```bash
# Terminal 1: Watch for changes and auto-sync
./scripts/deploy.sh --watch

# Terminal 2: Run pipeline on remote after each sync
ssh your_server "cd location-data-pipeline && ./spark/spark-submit-location-pipeline_ps_remote.sh"
```

---

## Pipeline Architecture

### Pipeline Stages

```
┌─────────────────┐
│   Snowflake     │
│   (Source)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 1. Data         │  Removes invalid coordinates, duplicates,
│    Cleansing    │  and stationary outliers
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Stay Point   │  Identifies locations where device
│    Detection    │  remained stationary
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Clustering   │  Groups nearby stay points into
│                 │  location clusters
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Route        │  Generates routes between clusters
│    Prediction   │  using OSRM
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Output        │
│   (Snowflake)   │
└─────────────────┘
```

### Key Components

| Component | Pure Spark (`_ps.py`) | Pandas-based |
|-----------|----------------------|--------------|
| `DataCleanser` | `data_cleanser_ps.py` | `data_cleanser.py` |
| `PointsQualifier` | `points_qualifier_ps.py` | `points_qualifier.py` |
| `ClusterStayPoints` | `cluster_stay_points_ps.py` | `cluster_stay_points.py` |
| `RoutePredictorOSRM` | `osrm_route_predictor.py` | `osrm_route_predictor.py` |

### Data Partitioning

The Pure Spark pipeline partitions all operations by `device_id` to ensure:
- Each device's data is processed independently
- No cross-contamination between devices
- Efficient distributed processing across workers

---

## Debugging and Output

### Route Prediction JSON Output

The pipeline can save route prediction results to a JSON file for visualization or further analysis.

**Enable JSON output:**
```bash
# In .env file
SAVE_ROUTES_JSON=true
OUTPUT_DATA_DIR=./docker/data
```

This generates a file like `predicted_routes_osrm_pairwise_<device_id>.json` containing:
- **metadata**: Date range, total distance, journey time, routing engine info
- **routes**: Matched route coordinates, transport mode, confidence scores
- **stay_points**: Detected stay locations with arrival/departure times
- **trajectory_points**: GPS points between stay locations

### DataFrame Dumper

The pipeline includes a `DataFrameDumper` utility for debugging intermediate results:

```python
from utils.dataframe_dumper import DataFrameDumper

dumper = DataFrameDumper()
dumper.dump(df, "step_name")  # Outputs to /tmp/spark-pipeline-output/step_name.csv
```

**Configure output directory:**
```bash
export PIPELINE_OUTPUT_DIR=/path/to/output
```

### Viewing Spark Job History

Access the Spark History Server at http://localhost:18080 to view:
- Completed job details
- Stage execution times
- Task metrics and logs

### Checking Logs

```bash
# Spark Master logs
docker compose logs -f spark-master

# Worker logs
docker compose logs -f spark-worker-1
docker compose logs -f spark-worker-2
```

---

## Troubleshooting

### Common Issues

#### 1. `spark-submit: command not found`

Ensure Spark is installed and in your PATH:
```bash
export SPARK_HOME=/opt/homebrew/opt/apache-spark/libexec  # Adjust path as needed
export PATH=$SPARK_HOME/bin:$PATH
```

#### 2. Cannot connect to Spark Master

Verify the cluster is running:
```bash
docker compose ps
curl http://localhost:8080  # Should return HTML
```

#### 3. Snowflake connection errors

- Verify your `.env` file has correct credentials
- Ensure the private key file exists and is readable
- Check that your IP is whitelisted in Snowflake

#### 4. OSRM routing errors

The OSRM server needs map data. Check if it's running:
```bash
curl "http://localhost:5001/route/v1/driving/13.388860,52.517037;13.397634,52.529407"
```

#### 5. Out of memory errors

Adjust worker memory in `docker/docker-compose.yml`:
```yaml
environment:
  - SPARK_WORKER_MEMORY=4g  # Increase from 2g
```

#### 6. Missing environment variables

The submit script will report missing variables:
```bash
Error: The following required environment variables are not set:
  - SNOWFLAKE_ACCOUNT
  - SNOWFLAKE_USER
```

Ensure your `.env` file is properly configured.

---

## Project Structure

```
location-data-pipeline/
├── .env                    # Environment configuration (create from .env.example)
├── .env.example            # Example configuration template
├── pyproject.toml          # UV/Python project configuration
├── uv.lock                 # UV lock file
├── pyspark_venv.tar.gz     # Packed virtual environment (generated)
├── README.md               # This file
│
├── docker/
│   ├── docker-compose.yml  # Spark 4.1 cluster configuration
│   ├── Dockerfile          # Spark 4.1 worker image (based on apache/spark:4.1.0)
│   └── osrm/               # OSRM routing server data
│
├── scripts/
│   ├── deploy.sh           # Remote deployment via rsync
│   └── pack-venv.sh        # Pack virtual environment for distribution
│
├── spark/
│   ├── spark-submit-location-pipeline_ps.sh     # Pure Spark launcher (local)
│   ├── spark-submit-location-pipeline.sh        # Pandas-based launcher (local)
│   └── spark-submit-location-pipeline_ps_remote.sh  # Pure Spark launcher (remote cluster)
│
└── src/
    ├── spark_location_pipeline_ps.py   # Pure Spark pipeline
    ├── spark_location_pipeline.py      # Pandas-based pipeline
    ├── data_cleanser_ps.py             # Data cleansing (Pure Spark)
    ├── data_cleanser.py                # Data cleansing (Pandas)
    ├── points_qualifier_ps.py          # Stay point detection (Pure Spark)
    ├── points_qualifier.py             # Stay point detection (Pandas)
    ├── cluster_stay_points_ps.py       # Clustering (Pure Spark)
    ├── cluster_stay_points.py          # Clustering (Pandas)
    ├── snowflake_config_manager.py     # Snowflake configuration
    └── utils/
        ├── location_pipeline_config_manager.py  # Pipeline configuration
        ├── dataframe_dumper.py                  # Debug output utility
        └── osrm/
            ├── osrm_client.py           # OSRM API client
            └── osrm_route_predictor.py  # Route generation
```

---
