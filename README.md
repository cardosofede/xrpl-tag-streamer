# XRPL Tag Collector

A simplified utility for collecting XRPL transactions filtered by source tag.

## Overview

This application periodically fetches transactions for a list of wallets from the XRPL, filters them by source tag, and stores matching transactions in MongoDB. User configurations are stored in MongoDB and can be updated dynamically without restarting the application.

## Features

- Configurable collection frequency
- Filtering by source tag
- Support for multiple users with multiple wallets
- MongoDB storage for matching transactions
- Dynamic user configuration (add/remove users and wallets without restarting)
- Periodic refresh of user configurations
- Automatic collection creation and initialization

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/xrpl-tag-collector.git
cd xrpl-tag-collector
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up MongoDB:
   - Install MongoDB locally, use a cloud service like MongoDB Atlas, or run with Docker

### Running MongoDB with Docker

If you don't have MongoDB installed locally, you can easily run it using Docker:

```bash
docker run --name mongo -d -p 27017:27017 mongo:latest
```

This command will:
- Start a MongoDB container named "mongo"
- Run it in the background (-d)
- Map port 27017 on your local machine to port 27017 in the container

You can then connect to this MongoDB instance at `mongodb://localhost:27017/`.

## Configuration

Create a `.env` file in the root directory with the following variables (or set environment variables):

```
# XRPL Node Configuration
XRPL_RPC_URL=https://s.altnet.rippletest.net:51234/

# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=xrpl_transactions
MONGO_COLLECTION=transactions

# Collection frequency in seconds
COLLECTION_FREQUENCY=300

# How often to refresh the user configuration from the database (in seconds)
USER_CONFIG_REFRESH_INTERVAL=60

# Source tag to filter transactions
SOURCE_TAG=12345
```

## Usage

### Running the Collector

```bash
python -m src.main
```

When the collector starts, it:
1. Verifies the MongoDB connection
2. Creates the necessary collections if they don't exist
3. Sets up the required indexes
4. Initializes the users collection with default users if it's empty

### Managing Users

User configuration is stored in the MongoDB database in a collection called `users`. You can use the MongoDB shell, a GUI tool like MongoDB Compass, or programmatically add/edit users.

The user document structure is:

```json
{
  "id": "user1",
  "wallets": ["rPEPPER7kfTD9w2To4CQk6UCfuHM9c6GDY", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"]
}
```

Default users from `config.py` are used to initialize the database if it's empty:

```python
DEFAULT_USERS = [
    {
        "id": "user1",
        "wallets": ["rPEPPER7kfTD9w2To4CQk6UCfuHM9c6GDY", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"]
    },
    {
        "id": "user2",
        "wallets": ["rBepJuTLFJt3WmtLXYAxSjtBWAeQxVbncv"]
    }
]
```

The application will automatically refresh the user configuration every `USER_CONFIG_REFRESH_INTERVAL` seconds.

## How It Works

1. The collector verifies MongoDB connection and creates collections if they don't exist
2. It initializes the MongoDB `users` collection with default users if it's empty
3. It periodically checks for changes in the user configuration
4. For each user's wallet, it fetches transactions from the XRPL
5. Transactions are filtered by the specified source tag
6. Matching transactions are stored in MongoDB with the user's ID
7. The process repeats after sleeping for the configured duration

## Development

- `src/collector.py` - Main transaction collector logic
- `src/db/database.py` - MongoDB database interface
- `src/config.py` - Configuration
- `src/utils/transaction_utils.py` - Transaction processing utilities

## MongoDB Collections

The application uses the following MongoDB collections:

1. `transactions` - Stores filtered XRPL transactions
2. `users` - Stores user configurations with wallet addresses

## MongoDB Management Tools

For managing your MongoDB data, you can use:

- **MongoDB Compass**: A GUI for MongoDB that allows you to explore and modify your data
- **MongoDB Shell**: Command-line tool for working with MongoDB
- **Studio 3T**: Another popular MongoDB GUI with both free and paid versions

These tools can be useful for manual management of users and viewing collected transactions.