import os

_DEFAULTS = {
    "DB_PATH": "opus.db",
}


def get_config(key: str, default: str | None = None) -> str:
    """Retrieve configuration value from environment variables."""
    if default is None:
        default = _DEFAULTS.get(key)
    return os.getenv(key, default)
