"""
Logging and terminal output utilities
"""

import logging
from typing import Optional

# Color codes for terminal output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    MAGENTA = '\033[0;35m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color

def setup_logging(level: int = logging.INFO, logger_name: Optional[str] = None) -> logging.Logger:
    """
    Setup logging configuration
    
    Args:
        level: Logging level (default: INFO)
        logger_name: Name of the logger (default: None for root logger)
    
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if logger_name:
        return logging.getLogger(logger_name)
    return logging.getLogger()

def print_colored(message: str, color: str = Colors.NC) -> None:
    """Print colored message to terminal"""
    print(f"{color}{message}{Colors.NC}")

def print_info(message: str) -> None:
    """Print info message in blue"""
    print_colored(f"[INFO] {message}", Colors.BLUE)

def print_success(message: str) -> None:
    """Print success message in green"""
    print_colored(f"[SUCCESS] {message}", Colors.GREEN)

def print_warning(message: str) -> None:
    """Print warning message in yellow"""
    print_colored(f"[WARNING] {message}", Colors.YELLOW)

def print_error(message: str) -> None:
    """Print error message in red"""
    print_colored(f"[ERROR] {message}", Colors.RED)

def print_header(message: str) -> None:
    """Print header message in bold cyan"""
    print_colored(f"\n{message}", Colors.BOLD + Colors.CYAN)

def print_subheader(message: str) -> None:
    """Print subheader message in magenta"""
    print_colored(f"\n{message}", Colors.MAGENTA)