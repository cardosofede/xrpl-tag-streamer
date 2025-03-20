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

## Installation and Setup

### Option 1: Using the Makefile (Recommended)

The project includes a Makefile to simplify common operations:

1. Clone this repository:
```bash
git clone https://github.com/yourusername/xrpl-tag-collector.git
cd xrpl-tag-collector
```

2. Run the setup script to create a virtual environment and install dependencies:
```bash
make setup
```

3. Activate the virtual environment (the setup script will show you the command for your shell)

4. Install the package in development mode:
```bash
make install        # Basic installation
# OR
make dev-install    # Installation with development dependencies
```

### Option 2: Manual Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/xrpl-tag-collector.git
cd xrpl-tag-collector
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -e .        # Basic installation
# OR
pip install -e ".[dev]" # Installation with development dependencies
```

### Setting up MongoDB

Set up MongoDB using one of these methods:
- Install MongoDB locally
- Use a cloud service like MongoDB Atlas
- Run with Docker (recommended for development)

#### Running MongoDB with Docker

If you're using Docker, you can run both MongoDB and the application with:

```bash
make docker-build   # Build the Docker image
make docker-up      # Start the application and MongoDB containers
```

To stop the containers:
```bash
make docker-down
```

## Configuration

Create a `.env` file in the root directory (you can copy from `.env.example`):

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

### Running the Application

The Makefile provides convenient commands for running different modes:

```bash
make stream    # Run the transaction streamer to collect real-time transactions
make history   # Process historical transactions
make query     # Query stored transactions
make stats     # Show statistics about collected data
```

These commands are equivalent to:

```bash
python -m src.main stream
python -m src.main history
python -m src.main query
python -m src.main stats
```

### Development Tools

The Makefile also provides commands for development:

```bash
make format    # Format code with black and isort
make test      # Run tests
make clean     # Clean build artifacts and cache files
```

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