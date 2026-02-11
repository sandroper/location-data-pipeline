# Location Data Pipeline

A PySpark-based pipeline for processing location data from Snowflake, built on **Apache Spark 4.0.1**. The pipeline cleanses raw location data, identifies stay points vs trajectory points, clusters stay points using grid-based spatial clustering, and generates route predictions using OSRM with geodesic distance calculations powered by **Apache Sedona**.

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

Docker is required to run the local Spark cluster.

**Check if Docker is installed and running:**
```bash
docker --version
docker info
```

**Install Docker:**
- macOS: [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
- Linux: [Docker Engine](https://docs.docker.com/engine/install/)
- Windows: [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)

### 3. Java 17

Java 17 is recommended for compatibility with Snowflake JDBC driver and Apache Spark 4.0.1.

**Check Java version:**
```bash
java -version
```

> **Note:** The Docker containers use Java 17 (`java17-ubuntu` base image) for Snowflake JDBC Arrow compatibility. Java 21 has known issues with the Snowflake JDBC driver's Arrow implementation.

### 4. Apache Spark 4.0.1 (Local Installation)

This project requires **Apache Spark 4.0.x**. Spark must be installed locally to use `spark-submit` from your host machine.

**Check if Spark is installed and verify version:**
```bash
spark-submit --version
# Should show version 4.0.x
```

**Install Spark 4.0.1:**
```bash
# macOS via Homebrew
brew install apache-spark

# Or download Spark 4.0.1 from https://spark.apache.org/downloads.html
# and add to PATH
```

> **Note:** The Docker containers use `apache/spark:4.0.1-scala2.13-java17-ubuntu`. Ensure your local Spark installation is compatible (4.0.x recommended).

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

### 3. Download Required JARs

The pipeline requires JARs for Snowflake connectivity and Apache Sedona geospatial operations:

```bash
./scripts/download-jars.sh
```

This downloads:
- `snowflake-jdbc-3.24.2.jar` - Snowflake JDBC driver
- `spark-snowflake_2.13-3.1.6.jar` - Spark Snowflake connector
- `sedona-spark-shaded-4.0_2.13-1.8.1.jar` - Apache Sedona geospatial library
- `geotools-wrapper-1.8.1-33.1.jar` - GeoTools for coordinate transformations

### 4. Verify Installation

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
# Spark cluster configuration
SPARK_MASTER_HOST=127.0.0.1

# Logging configuration
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_OUTPUT=console                # console, file, both
LOG_FORMAT=standard               # standard, detailed, json
# LOG_FILE=./logs/pipeline.log    # path when LOG_OUTPUT includes file

# Snowflake Connection Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_URL=your_url
SNOWFLAKE_USER=your_username
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_TABLE=your_table
# Custom query (optional) - overrides default SELECT * FROM table
# SNOWFLAKE_QUERY=SELECT * FROM my_table WHERE date > '2024-01-01'

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

The Spark cluster runs in Docker containers managed by Docker Compose, using the official **Apache Spark 4.0.1** image with Scala 2.13 and Java 17.

### 1. Navigate to Docker Directory

```bash
cd docker
```

### 2. Build the Docker Image

```bash
docker compose build
```

This builds a custom image based on `apache/spark:4.0.1-scala2.13-java17-ubuntu` with:
- Python 3.14 with required dependencies
- Apache Sedona and GeoTools for geospatial operations
- JVM options for Snowflake JDBC compatibility

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
# From project root (for local Docker cluster)
SPARK_MASTER_HOST=127.0.0.1 ./spark/spark-submit-location-pipeline_ps.sh
```

This pipeline uses pure PySpark operations throughout, making it suitable for large-scale distributed processing. All operations are partitioned by `device_id` to ensure data isolation between devices.

### Option 2: Pandas-based Pipeline

```bash
# From project root
SPARK_MASTER_HOST=127.0.0.1 ./spark/spark-submit-location-pipeline.sh
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

This section covers deploying and running the pipeline on a remote standalone Spark cluster.

### Prerequisites for Remote Cluster

- Remote server with Spark 4.0.x installed
- Java 17 on remote workers (recommended for Snowflake JDBC compatibility)
- Python 3.14 available as `python3.14` or `python3`
- SSH access to the remote server
- Internet access on remote server (for initial setup)

### Step 1: Configure Deployment Settings

```bash
# Configure remote server (one-time, or add to shell profile)
export DEPLOY_USER=your_username
export DEPLOY_HOST=your_server.com
export DEPLOY_PATH=/home/your_username/location-data-pipeline
# Optional: specify SSH key
export DEPLOY_SSH_KEY=/path/to/ssh/key
```

### Step 2: Deploy Source Code

```bash
# Sync source files to remote (excludes local venv and packed venv)
./scripts/deploy.sh

# Sync with a specific env file (deployed as .env on remote)
./scripts/deploy.sh --env .env.prod
```

### Step 3: Setup Remote Environment (First Time Only)

The virtual environment must be created **on the remote server** to ensure Linux-compatible binaries:

```bash
# Run remote-setup.sh on the remote server
./scripts/deploy.sh --setup
```

This runs `scripts/remote-setup.sh` on the remote server which:
- Creates a Python virtual environment using `python3.14`
- Installs all required dependencies (PySpark, Sedona, etc.)
- Packs the venv into `pyspark_venv.tar.gz` for distribution to workers

> **Important:** Do NOT sync `pyspark_venv.tar.gz` from your local macOS machine - it contains macOS binaries that won't work on Linux workers. The packed venv must be created on the Linux remote server.

### Step 4: Download JARs on Remote

SSH to the remote server and download required JARs:

```bash
ssh your_server
cd location-data-pipeline
./scripts/download-jars.sh
```

Alternatively, you can sync your local `jars/` directory to the remote server.

### Step 5: Run the Pipeline

```bash
# On the remote server
./spark/spark-submit-location-pipeline_ps.sh
```

Or deploy and run in one command:

```bash
./scripts/deploy.sh --env .env.prod --run
```

### Deploy Script Options

| Option | Description |
|--------|-------------|
| `--run` | Run the Pure Spark pipeline after syncing |
| `--run-pandas` | Run the Pandas-based pipeline after syncing |
| `--setup` | Run `remote-setup.sh` to create/update the virtual environment |
| `--env <file>` | Sync specified env file to remote as `.env` |
| `--watch` | Watch for changes and auto-sync (requires `fswatch`) |
| `--dry-run` | Show what would be transferred without actually syncing |

### Complete Deployment Workflow

```bash
# 1. Configure deployment (first time)
export DEPLOY_USER=your_username
export DEPLOY_HOST=your_server.com

# 2. Deploy code and setup remote environment (first time)
./scripts/deploy.sh --env .env.prod --setup

# 3. Download JARs on remote (first time)
ssh your_server "cd location-data-pipeline && ./scripts/download-jars.sh"

# 4. Run the pipeline
./scripts/deploy.sh --run
```

### Updating Dependencies

When you add or update dependencies in `pyproject.toml`:

```bash
# Re-run setup on remote to update the packed venv
./scripts/deploy.sh --setup
```

### Local Docker vs Remote Cluster

The spark-submit script automatically detects the environment:

| Environment | Detection | Python on Executors | venv Archive |
|-------------|-----------|---------------------|--------------|
| Local Docker | `SPARK_MASTER_HOST=127.0.0.1` | System Python in Docker image | Not used |
| Remote Cluster | Any other host | Packed venv via `--archives` | Required |

For local Docker, the script uses the Python installation in the Docker image.
For remote clusters, it ships the packed virtual environment to executors.

### Iterative Development

For rapid iteration during development:

```bash
# Terminal 1: Watch for changes and auto-sync
./scripts/deploy.sh --watch

# Terminal 2: Run pipeline on remote after each sync
ssh your_server "cd location-data-pipeline && ./spark/spark-submit-location-pipeline_ps.sh"
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
│    Detection    │  remained stationary (uses Sedona for
│                 │  geodesic distance calculations)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Clustering   │  Groups nearby stay points into
│                 │  location clusters using grid-based
│                 │  spatial clustering
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Route        │  Generates routes between clusters
│    Prediction   │  using OSRM, predicts transport mode
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Output        │
│   (JSON/Map)    │
└─────────────────┘
```

### Key Components

| Component | Pure Spark (`_ps.py`) | Pandas-based |
|-----------|----------------------|--------------|
| `DataCleanser` | `data_cleanser_ps.py` | `data_cleanser.py` |
| `PointsQualifier` | `points_qualifier_ps.py` | `points_qualifier.py` |
| `ClusterStayPoints` | `cluster_stay_points_ps.py` | `cluster_stay_points.py` |
| `RoutePredictorOSRM` | `osrm_route_predictor.py` | `osrm_route_predictor.py` |

### Geospatial Operations

The pipeline uses **Apache Sedona** for accurate geodesic distance calculations:

- `ST_DistanceSphere()` - Calculates geodesic distance between coordinates
- Used in stay point detection and clustering
- Operates directly on Spark DataFrames for distributed processing

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

### Logging Configuration

The pipeline logging is configurable via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | INFO | DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `LOG_OUTPUT` | console | console, file, both |
| `LOG_FORMAT` | standard | standard, detailed, json |
| `LOG_FILE` | ./logs/pipeline.log | Path when LOG_OUTPUT includes file |

**Format options:**
- `standard`: `timestamp - logger - level - message`
- `detailed`: Includes filename, line number, and function name
- `json`: Structured JSON for log aggregation tools (ELK, Datadog, etc.)

**Example for production:**
```bash
LOG_LEVEL=INFO
LOG_OUTPUT=both
LOG_FORMAT=json
LOG_FILE=./logs/pipeline.log
```

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
- Verify you're using Snowflake JDBC 3.24.2 (check with `ls jars/`)

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

#### 7. `sun.misc.Unsafe` or Arrow errors (Java 21)

This occurs when using Java 21 with the Snowflake JDBC driver. Solutions:
- **Recommended:** Use Java 17 (the Docker image uses Java 17 by default)
- **Alternative:** Ensure JVM options are set in `spark-defaults.conf`:
  ```
  spark.executor.extraJavaOptions --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED
  ```

#### 8. Remote cluster: `ModuleNotFoundError: No module named 'sedona'`

The packed virtual environment is missing or outdated:
```bash
# Re-run setup on remote to create/update the packed venv
./scripts/deploy.sh --setup
```

#### 9. Remote cluster: Python path not found

Ensure `python3.14` or `python3` is available on remote workers. The script auto-detects the Python version.

#### 10. `Cannot run program "./venv/bin/python"` on Docker

This happens when the macOS packed venv is accidentally used on Docker. For local Docker:
```bash
# Use explicit host specification
SPARK_MASTER_HOST=127.0.0.1 ./spark/spark-submit-location-pipeline_ps.sh
```

---

## Project Structure

```
location-data-pipeline/
├── .env                    # Environment configuration (create from .env.example)
├── .env.example            # Example configuration template
├── pyproject.toml          # UV/Python project configuration
├── uv.lock                 # UV lock file
├── pyspark_venv.tar.gz     # Packed virtual environment (generated on remote)
├── README.md               # This file
│
├── docker/
│   ├── docker-compose.yml  # Spark 4.0.1 cluster configuration
│   ├── Dockerfile          # Custom Spark image (Java 17, Python 3.14, Sedona)
│   ├── spark/
│   │   └── spark-defaults.conf  # Spark configuration with JVM options
│   └── osrm/               # OSRM routing server data
│
├── jars/                   # Downloaded JARs (Snowflake, Sedona, GeoTools)
│
├── scripts/
│   ├── deploy.sh           # Remote deployment via rsync
│   ├── download-jars.sh    # Download required JARs from Maven Central
│   ├── remote-setup.sh     # Setup virtual environment on remote server
│   └── pack-venv.sh        # Pack virtual environment for distribution
│
├── spark/
│   ├── spark-submit-location-pipeline_ps.sh  # Pure Spark launcher
│   └── spark-submit-location-pipeline.sh     # Pandas-based launcher
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
        ├── logging_config.py                    # Centralized logging setup
        ├── dataframe_dumper.py                  # Debug output utility
        └── osrm/
            ├── osrm_client.py           # OSRM API client
            └── osrm_route_predictor.py  # Route generation
```

---
