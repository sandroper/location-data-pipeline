"""
Snowflake private key management utilities
Handles reading, validation, and formatting of private keys for Snowflake authentication
"""

import os
from pathlib import Path
from typing import Tuple

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import re


def load_snowflake_private_key(key_path: str = None) -> Tuple[str, bool]:
    """
    Load and validate a Snowflake private key from a file.
    
    Args:
        key_path: Path to the private key file. If None, uses default location
                 (sf_k2ib_key.p8 in the script directory) or SNOWFLAKE_KEY_PATH env var.
    
    Returns:
        Private key in hex64 format
    
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key format is invalid
        IOError: If there's an error reading the file
    """
    # Determine key file path
    script_dir = Path(__file__).parent
    
    if key_path is None:
        # Try environment variable first, then default location
        env_key_path = os.getenv("SNOWFLAKE_KEY_PATH")
        if env_key_path:
            key_path = env_key_path
        else:
            key_path = str(script_dir / "sf_k2ib_key.p8")

    with open(key_path, "rb") as key_file:
        pkey = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    pkey_pem = pkey.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode("utf-8")
    pkey_hex64 = re.sub(r"-----.*-----|\n", "", pkey_pem)
    
    return pkey_hex64


def get_snowflake_key_options() -> dict:
    """
    Get Snowflake connection options including private key authentication.
    
    Returns:
        Dictionary of Snowflake connection options with private key configured
    
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key format is invalid or required env vars are missing
    """
    
    # Build connection options
    options = {
        "pem_private_key": load_snowflake_private_key()
    }
    
    return options

