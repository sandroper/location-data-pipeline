#!/bin/bash

# Deploy script for syncing local changes to remote server
# Usage: ./scripts/deploy.sh [--run] [--env <file>]
#
# Options:
#   --run         Run the pipeline after syncing
#   --env <file>  Sync specified env file as .env on remote (default: .env)
#   --watch       Watch for changes and auto-sync (requires fswatch)

set -e

# ============================================================
# CONFIGURATION - Edit these values for your remote server
# ============================================================
REMOTE_USER="${DEPLOY_USER:-your_username}"
REMOTE_HOST="${DEPLOY_HOST:-your_server.com}"
SSH_KEY="${DEPLOY_SSH_KEY:-}"  # Optional: path to SSH key

# Default path uses the remote username (must be set after REMOTE_USER)
if [ -n "$DEPLOY_PATH" ]; then
    REMOTE_PATH="$DEPLOY_PATH"
else
    REMOTE_PATH="/home/${REMOTE_USER}/location-data-pipeline"
fi

# ============================================================
# Script setup
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Build SSH options
SSH_OPTS=""
if [ -n "$SSH_KEY" ]; then
    SSH_OPTS="-i $SSH_KEY"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================
# Functions
# ============================================================
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --run            Run the pipeline after syncing"
    echo "  --run-pandas     Run the Pandas-based pipeline after syncing"
    echo "  --env <file>     Sync specified env file to remote as .env (default: .env)"
    echo "  --watch          Watch for changes and auto-sync (requires fswatch)"
    echo "  --dry-run        Show what would be transferred without actually syncing"
    echo "  --help           Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  DEPLOY_USER      Remote username (default: your_username)"
    echo "  DEPLOY_HOST      Remote hostname (default: your_server.com)"
    echo "  DEPLOY_PATH      Remote path (default: /home/\$USER/location-data-pipeline)"
    echo "  DEPLOY_SSH_KEY   Path to SSH key (optional)"
    echo ""
    echo "Examples:"
    echo "  $0                              # Sync source files only"
    echo "  $0 --run                        # Sync and run Pure Spark pipeline"
    echo "  $0 --env .env --run             # Sync .env and run"
    echo "  $0 --env .env.prod --run        # Sync .env.prod as remote .env and run"
    echo "  DEPLOY_HOST=server.com $0       # Override remote host"
}

do_sync() {
    local dry_run_flag=""
    if [ "$DRY_RUN" = true ]; then
        dry_run_flag="--dry-run"
        log_info "DRY RUN - showing what would be transferred:"
    fi

    log_info "Syncing to ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"

    # Build rsync exclude list
    EXCLUDES=(
        --exclude='.venv/'
        --exclude='__pycache__/'
        --exclude='*.pyc'
        --exclude='.git/'
        --exclude='.idea/'
        --exclude='*.egg-info/'
        --exclude='.gitignore'
        --exclude='.claude'
        --exclude='.pytest_cache/'
        --exclude='docker/'
        --exclude='.env'
        --exclude='*.p8'
        --exclude='uv.lock'
        --exclude='CLAUDE.md'
    )


    # Sync main project files
    rsync -avz --progress $dry_run_flag \
        "${EXCLUDES[@]}" \
        -e "ssh $SSH_OPTS" \
        "$PROJECT_ROOT/" \
        "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/"

    # Optionally sync env file
    if [ -n "$ENV_FILE" ]; then
        local local_env_path="$PROJECT_ROOT/$ENV_FILE"
        if [ -f "$local_env_path" ]; then
            log_info "Syncing $ENV_FILE as .env on remote..."
            rsync -avz --progress $dry_run_flag \
                -e "ssh $SSH_OPTS" \
                "$local_env_path" \
                "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/.env"
        else
            log_error "Env file not found: $local_env_path"
            exit 1
        fi
    fi

    if [ "$DRY_RUN" != true ]; then
        log_info "Sync complete!"
    fi
}

do_run() {
    local pipeline_script="$1"
    log_info "Running pipeline on remote server..."

    ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" \
        "cd ${REMOTE_PATH} && ./spark/${pipeline_script}"
}

do_watch() {
    log_info "Watching for changes... (Ctrl+C to stop)"

    if ! command -v fswatch &> /dev/null; then
        log_error "fswatch is required for --watch mode"
        log_info "Install with: brew install fswatch (macOS) or apt install fswatch (Linux)"
        exit 1
    fi

    # Watch for changes and sync
    fswatch -o \
        --exclude='.venv' \
        --exclude='__pycache__' \
        --exclude='.git' \
        --exclude='.idea' \
        "$PROJECT_ROOT/src" \
        "$PROJECT_ROOT/spark" \
        "$PROJECT_ROOT/docker" \
    | while read -r; do
        log_info "Changes detected, syncing..."
        do_sync
    done
}

# ============================================================
# Parse arguments
# ============================================================
RUN_PIPELINE=false
RUN_PANDAS=false
ENV_FILE=""
WATCH_MODE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --run)
            RUN_PIPELINE=true
            shift
            ;;
        --run-pandas)
            RUN_PANDAS=true
            shift
            ;;
        --env)
            if [ -n "$2" ] && [ "${2:0:1}" != "-" ]; then
                ENV_FILE="$2"
                shift 2
            else
                log_error "--env requires a filename argument (e.g., --env .env.prod)"
                exit 1
            fi
            ;;
        --watch)
            WATCH_MODE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# ============================================================
# Validate configuration
# ============================================================
if [ "$REMOTE_USER" = "your_username" ] || [ "$REMOTE_HOST" = "your_server.com" ]; then
    log_error "Please configure DEPLOY_USER and DEPLOY_HOST"
    log_info "Edit this script or set environment variables:"
    log_info "  export DEPLOY_USER=myuser"
    log_info "  export DEPLOY_HOST=myserver.com"
    log_info "  export DEPLOY_PATH=/path/to/project"
    exit 1
fi

# ============================================================
# Main execution
# ============================================================
if [ "$WATCH_MODE" = true ]; then
    do_watch
else
    do_sync

    if [ "$RUN_PIPELINE" = true ]; then
        do_run "spark-submit-location-pipeline_ps.sh"
    elif [ "$RUN_PANDAS" = true ]; then
        do_run "spark-submit-location-pipeline.sh"
    fi
fi
