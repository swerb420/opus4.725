import os

def get_config(key: str, default: str = None) -> str:
    """Retrieve configuration value from environment variables."""
    return os.getenv(key, default)
