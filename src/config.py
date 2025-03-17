"""
Configuration module for the XRPL Tag Streamer application.
Loads settings from environment variables with sensible defaults.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# XRPL Node Configuration
XRPL_WS_URL = os.getenv("XRPL_WS_URL", "https://xrplcluster.com/")
XRPL_RPC_URL = os.getenv("XRPL_RPC_URL", "https://xrplcluster.com/")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://wiz-ai:wiz-ai@localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "xrpl_transactions")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "transactions")

# Collection frequency in seconds
COLLECTION_FREQUENCY = int(os.getenv("COLLECTION_FREQUENCY", "300"))  # Default: 5 minutes

# How often to refresh the user configuration from the database (in seconds)
USER_CONFIG_REFRESH_INTERVAL = int(os.getenv("USER_CONFIG_REFRESH_INTERVAL", "60"))  # Default: 1 minute

# Source tag to filter transactions
SOURCE_TAG = int(os.getenv("SOURCE_TAG", "19089388"))

FROM_LEDGER = int(os.getenv("FROM_LEDGER", "94700993"))

# Default user configuration 
# This is used to initialize the MongoDB users collection if it's empty
# After initialization, the application will load users from MongoDB
DEFAULT_USERS = [
    {
        "id": "david",
        "wallets": ["rJtj42u8QPQWcPiwF3B8sNPb2GMo9gmNub"]
    }
]

# other wallet: rBev9xk8HJJTi4aeLtPjnBMwxnXuHaWaBh
# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
