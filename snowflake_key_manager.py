# -*- coding: utf-8 -*-
"""
Snowflake private key management utilities
Handles reading, validation, and formatting of private keys for Snowflake authentication
"""

import os
from pathlib import Path
from typing import Tuple


def load_snowflake_private_key(key_path: str = None) -> Tuple[str, bool]:
    """
    Load and validate a Snowflake private key from a file.
    
    Args:
        key_path: Path to the private key file. If None, uses default location
                 (sf_k2ib_key.p8 in the script directory) or SNOWFLAKE_KEY_PATH env var.
    
    Returns:
        Tuple of (private_key_string, is_encrypted)
    
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key format is invalid
        IOError: If there's an error reading the file
    """
    # Determine key file path
    if key_path is None:
        # Try environment variable first, then default location
        script_dir = Path(__file__).parent
        key_path = os.getenv("SNOWFLAKE_KEY_PATH", str(script_dir / "sf_k2ib_key.p8"))
    
    key_file = Path(key_path)
    
    if not key_file.exists():
        raise FileNotFoundError(f"Snowflake private key file not found: {key_file}")
    
    # Read the private key content
    # Read in binary mode first to handle any encoding issues, then decode
    try:
        with open(key_file, 'rb') as f:
            private_key_bytes = f.read()
        # Try to decode as UTF-8, fallback to latin-1 if needed
        try:
            private_key = private_key_bytes.decode('utf-8')
        except UnicodeDecodeError:
            private_key = private_key_bytes.decode('latin-1')
    except Exception as e:
        raise IOError(f"Failed to read private key file: {e}")
    
    # Normalize line endings to Unix format (LF) - Snowflake connector is sensitive to this
    # Replace Windows line endings (CRLF) and old Mac line endings (CR) with Unix (LF)
    private_key = private_key.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove only trailing whitespace/newlines, preserve the exact format
    private_key = private_key.rstrip()
    
    # Verify basic PEM format
    if '-----BEGIN' not in private_key or '-----END' not in private_key:
        raise ValueError("Private key does not appear to be in PEM format (missing BEGIN/END markers)")
    
    # Check if key is encrypted
    is_encrypted = '-----BEGIN ENCRYPTED PRIVATE KEY-----' in private_key
    
    if is_encrypted:
        key_passphrase = os.getenv("SNOWFLAKE_KEY_PASSPHRASE")
        if not key_passphrase:
            raise ValueError("Private key appears to be encrypted but SNOWFLAKE_KEY_PASSPHRASE is not set")
        print("Encrypted private key detected. Using passphrase for decryption.")
    else:
        # For unencrypted keys, ensure it's in PKCS#8 format (required by Snowflake)
        if '-----BEGIN RSA PRIVATE KEY-----' in private_key:
            raise ValueError(
                "Private key appears to be in PKCS#1 format (RSA). "
                "Snowflake requires PKCS#8 format. Please convert the key using: "
                "openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -in <key_file> -out <output_file>"
            )
        
        # Verify it's PKCS#8 format
        if '-----BEGIN PRIVATE KEY-----' not in private_key:
            print(f"Warning: Key format may not be standard PKCS#8. First 100 chars: {private_key[:100]}")
        
        print("Using unencrypted private key for authentication.")
    
    return private_key, is_encrypted


def get_snowflake_key_options() -> dict:
    """
    Get Snowflake connection options including private key authentication.
    
    Returns:
        Dictionary of Snowflake connection options with private key configured
    
    Raises:
        FileNotFoundError: If the key file doesn't exist
        ValueError: If the key format is invalid or required env vars are missing
    """
    # Load the private key
    private_key, is_encrypted = load_snowflake_private_key()
    
    # Build connection options
    options = {
        "pem_private_key": private_key,
    }
    
    # Add passphrase if key is encrypted
    if is_encrypted:
        key_passphrase = os.getenv("SNOWFLAKE_KEY_PASSPHRASE")
        if key_passphrase:
            options["pem_private_key_passphrase"] = key_passphrase
    
    return options

