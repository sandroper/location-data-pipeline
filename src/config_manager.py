import os
from typing import Optional
from pydantic import BaseSettings, SecretStr
import boto3
from botocore.exceptions import ClientError
import json

class ConfigManager(BaseSettings):
    """
    Unified configuration class using Pydantic BaseSettings.
    """
    ENV: str = "dev"  # Default to "dev" environment
    DEBUG: bool = False
    model
    
    