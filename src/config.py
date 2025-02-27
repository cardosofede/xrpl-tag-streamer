"""
Configuration module for the XRPL Tag Streamer application.
Loads settings from environment variables with sensible defaults.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# XRPL Node Configuration
XRPL_WS_URL = os.getenv("XRPL_WS_URL", "wss://s.altnet.rippletest.net/")
XRPL_RPC_URL = os.getenv("XRPL_RPC_URL", "https://s.altnet.rippletest.net:51234/")

# Database Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(DATA_DIR / "xrpl_transactions.duckdb"))

# Ensure data directory exists
DATA_DIR.mkdir(exist_ok=True, parents=True)

# Streaming Configuration
TARGET_TAG = os.getenv("TARGET_TAG", "hummingbot")
HISTORY_BACKFILL_DAYS = int(os.getenv("HISTORY_BACKFILL_DAYS", "1"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

def get_db_path() -> Path:
    """Get the path to the DuckDB database file, ensuring parent directory exists."""
    db_path = Path(DUCKDB_PATH)
    db_path.parent.mkdir(exist_ok=True, parents=True)
    return db_path 