# XRPL Tag Streamer

A Python application that streams transactions from the XRP Ledger (XRPL) and filters for transactions with specific tags (e.g., "hummingbot").

## Features

- Real-time streaming of XRPL transactions
- Historical transaction fetching for missed blocks
- DuckDB storage for efficient querying
- Tag-based filtering of transactions

## Setup

### Prerequisites

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) for package management

### Installation

```bash
# Clone the repository
git clone https://github.com/cardosofede/xrpl-tag-streamer.git
cd xrpl-tag-streamer

# Create and activate a virtual environment with uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .
```

### Configuration

Copy the example environment file and adjust settings as needed:

```bash
cp .env.example .env
```

Edit the `.env` file to configure XRPL node connections and other settings.

## Usage

### Start the transaction streamer

```bash
python -m src.main stream
```

### Process historical transactions

```bash
python -m src.main history --start-ledger 12345 --end-ledger 67890
```

### Query stored data

```bash
python -m src.main query --tag "hummingbot" --limit 10
```

## Development

Install development dependencies:

```bash
uv pip install -e ".[dev]"
```

## License

See [LICENSE](LICENSE) file for details.