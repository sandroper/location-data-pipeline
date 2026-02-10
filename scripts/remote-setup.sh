#!/bin/bash

# Setup script for remote server
# Creates a Python virtual environment and packs it for Spark
# Run this on the remote server after deploying source files

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Setting up Python environment on remote server..."

# Determine which Python to use (prefer 3.14, fall back to python3)
if command -v python3.14 &> /dev/null; then
    PYTHON_CMD="python3.14"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
else
    echo "ERROR: No suitable Python found"
    exit 1
fi
echo "Using Python: $PYTHON_CMD ($($PYTHON_CMD --version))"

# Create virtual environment if it doesn't exist
if [ ! -d "$PROJECT_ROOT/.venv" ]; then
    echo "Creating virtual environment..."
    $PYTHON_CMD -m venv "$PROJECT_ROOT/.venv"
fi

# Activate and install dependencies
echo "Installing dependencies..."
source "$PROJECT_ROOT/.venv/bin/activate"

# Upgrade pip
pip install --upgrade pip

# Install project dependencies (from pyproject.toml manually since we don't have uv)
pip install \
    'apache-sedona[spark]>=1.6.1' \
    'cryptography>=46.0.3' \
    'geopy>=2.4.0' \
    'pandas>=2.2.0' \
    'pyspark==4.0.1' \
    'python-dotenv>=1.2.1' \
    'setuptools>=80.9.0' \
    'pytz>=2024.1' \
    'pyarrow>=15.0.0' \
    'scikit-learn>=1.4.0' \
    'requests>=2.32.5' \
    'numpy>=1.26.0' \
    'matplotlib>=3.8.0' \
    'venv-pack>=0.2.0'

# Pack the virtual environment
echo "Packing virtual environment..."
python -m venv_pack -o "$PROJECT_ROOT/pyspark_venv.tar.gz" --force

echo "Setup complete!"
echo "Packed venv: $PROJECT_ROOT/pyspark_venv.tar.gz"
echo "Size: $(du -h "$PROJECT_ROOT/pyspark_venv.tar.gz" | cut -f1)"
