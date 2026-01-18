from datetime import datetime
import pytz
import logging

logger = logging.getLogger(__name__)


def get_timezone_offset(timezone_str):
    try:
        tz = pytz.timezone(timezone_str)
        now = datetime.now(tz)
        offset_hours = now.utcoffset().total_seconds() / 3600
        return int(offset_hours)
    except Exception as e:
        logger.warning(f"Warning: Could not determine timezone offset for {timezone_str}: {e}. Setting time zone to UTC.")
        return 0  # Default for UTC