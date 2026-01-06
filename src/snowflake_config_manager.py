import os
from pathlib import Path
from dotenv import load_dotenv


from snowflake_key_manager import get_snowflake_key_options

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded environment variables from {env_path}")
else:
    # Fallback: try to load from current directory
    load_dotenv()

class SnowflakeConfig():
    """
    Snowflake configuration class using Pydantic BaseSettings.
    """
    # Snowflake connection options
    # These are loaded from .env file or environment variables
    # See .env.example for the required variables
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "your_account")
    # Construct full URL if not provided
    snowflake_url: str = os.getenv("SNOWFLAKE_URL", "Your URL")

    # Get private key authentication options from key manager
    key_options: dict = get_snowflake_key_options()

    #Build Snowflake connection options
    snowflake_options: dict = {
        "sfURL": snowflake_url,
        "sfUser": os.getenv("SNOWFLAKE_USER", "your_username"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "your_database"),
        "sfSchema": os.getenv("SNOWFLAKE_SCHEMA", "your_schema"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "your_warehouse"),
        "sfTable": os.getenv("SNOWFLAKE_TABLE", "your_table")
    }

    # Add private key authentication options
    snowflake_options.update(key_options)

    # Optional role
    if os.getenv("SNOWFLAKE_ROLE"):
        snowflake_options["sfRole"] = os.getenv("SNOWFLAKE_ROLE")

    # Table name - can be set via environment variable
    table_name = os.getenv("SNOWFLAKE_TABLE", "your_table")

    def __str__(self) -> str:
        # Mask sensitive information in snowflake_options
        masked_options = self.snowflake_options.copy()
        # Mask private key if present
        if "pem_private_key" in masked_options:
            masked_options["pem_private_key"] = "***MASKED***"
        if "private_key" in masked_options:
            masked_options["private_key"] = "***MASKED***"
        
        return (
            f"SnowflakeConfig(\n"
            f"  snowflake_account={self.snowflake_account},\n"
            f"  snowflake_url={self.snowflake_url},\n"
            f"  table_name={self.table_name},\n"
            f"  snowflake_options={masked_options}\n"
            f")"
        )

