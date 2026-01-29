#!/bin/bash

# Pack the virtual environment for distribution with Spark jobs
# This creates a portable archive that can be shipped to remote executors

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_FILE="${1:-$PROJECT_ROOT/pyspark_venv.tar.gz}"

echo "Packing virtual environment..."

# Ensure venv-pack is installed
if ! python -c "import venv_pack" 2>/dev/null; then
    echo "Installing venv-pack..."
    uv pip install venv-pack
fi

# Pack the virtual environment
cd "$PROJECT_ROOT"
python -m venv_pack -o "$OUTPUT_FILE" --force

echo "Virtual environment packed to: $OUTPUT_FILE"
echo "Size: $(du -h "$OUTPUT_FILE" | cut -f1)"
