"""
Centralized logging configuration for the location data pipeline.

Configurable via environment variables:
- LOG_LEVEL: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)
- LOG_OUTPUT: console, file, both (default: console)
- LOG_FILE: path to log file (default: ./logs/pipeline.log)
- LOG_FORMAT: standard, detailed, json (default: standard)
"""

import os
import sys
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime


# Predefined log formats
LOG_FORMATS = {
    'standard': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'detailed': '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s',
    'minimal': '%(levelname)s - %(message)s',
}


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging():
    """
    Configure logging based on environment variables.

    Environment variables:
        LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        LOG_OUTPUT: Output destination (console, file, both)
        LOG_FILE: Path to log file when using file output
        LOG_FORMAT: Format style (standard, detailed, json)

    Returns:
        logging.Logger: The configured root logger
    """
    # Read configuration from environment
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_output = os.getenv('LOG_OUTPUT', 'console').lower()
    log_file = os.getenv('LOG_FILE', './logs/pipeline.log')
    log_format_name = os.getenv('LOG_FORMAT', 'standard').lower()

    # Parse log level
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Get formatter
    if log_format_name == 'json':
        formatter = JsonFormatter()
    else:
        format_string = LOG_FORMATS.get(log_format_name, LOG_FORMATS['standard'])
        formatter = logging.Formatter(format_string)

    # Get root logger and clear existing handlers
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    # Console handler
    if log_output in ('console', 'both'):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # File handler with rotation
    if log_output in ('file', 'both'):
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Reduce verbosity of noisy libraries
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    return root_logger
