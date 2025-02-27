"""
Database module for DuckDB operations.
Handles connection, schema creation, and query operations.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb
import pandas as pd

from src.config import get_db_path

logger = logging.getLogger(__name__)

class XRPLDatabase:
    """DuckDB database manager for XRPL transactions."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """Initialize the database connection and create tables if needed."""
        self.db_path = db_path or get_db_path()
        self.con = duckdb.connect(str(self.db_path))
        self._create_schema()
        
    def _create_schema(self) -> None:
        """Create the necessary tables if they don't exist."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS ledgers (
                ledger_index BIGINT PRIMARY KEY,
                ledger_hash VARCHAR,
                ledger_time TIMESTAMP,
                tx_count INTEGER,
                close_time TIMESTAMP,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                tx_hash VARCHAR PRIMARY KEY,
                ledger_index BIGINT,
                tx_index INTEGER,
                account VARCHAR,
                destination VARCHAR,
                amount VARCHAR,
                fee VARCHAR,
                transaction_type VARCHAR,
                source_tag INTEGER,
                destination_tag INTEGER,
                tx_time TIMESTAMP,
                tag_matched BOOLEAN,
                memo_data VARCHAR,
                raw_tx JSON,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ledger_index) REFERENCES ledgers(ledger_index)
            );
        """)
        
        # Create indices for faster querying
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_tx_ledger_index ON transactions(ledger_index);")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_tx_account ON transactions(account);")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_tx_destination ON transactions(destination);")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_tx_tag_matched ON transactions(tag_matched);")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_tx_source_tag ON transactions(source_tag);")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_tx_destination_tag ON transactions(destination_tag);")
    
    def store_ledger(self, ledger_data: Dict[str, Any]) -> None:
        """Store a ledger in the database."""
        try:
            # Convert UNIX timestamp to datetime
            ledger_time = datetime.fromtimestamp(ledger_data.get("ledger_time", 0))
            close_time = datetime.fromtimestamp(ledger_data.get("close_time", 0))
            
            self.con.execute("""
                INSERT OR REPLACE INTO ledgers (
                    ledger_index, ledger_hash, ledger_time, tx_count, close_time
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                ledger_data.get("ledger_index"),
                ledger_data.get("ledger_hash"),
                ledger_time,
                ledger_data.get("txn_count", 0),
                close_time
            ))
            logger.debug(f"Stored ledger {ledger_data.get('ledger_index')}")
        except Exception as e:
            logger.error(f"Error storing ledger: {e}")
    
    def store_transaction(self, tx_data: Dict[str, Any], tag_matched: bool = False) -> None:
        """Store a transaction in the database."""
        try:
            # Extract basic transaction data
            tx_hash = tx_data.get("hash")
            ledger_index = tx_data.get("ledger_index")
            
            # Get tx_index from either the direct field or from metaData
            # XRPL may provide it differently depending on the API response format
            tx_index = 0
            if "metaData" in tx_data and isinstance(tx_data["metaData"], dict):
                tx_index = tx_data["metaData"].get("TransactionIndex", 0)
            elif "meta" in tx_data and isinstance(tx_data["meta"], dict):
                tx_index = tx_data["meta"].get("TransactionIndex", 0)
            elif "tx_index" in tx_data:
                tx_index = tx_data.get("tx_index", 0)
            
            account = tx_data.get("Account")
            destination = tx_data.get("Destination", "")
            amount = tx_data.get("Amount", "")
            fee = tx_data.get("Fee", "")
            tx_type = tx_data.get("TransactionType", "")
            source_tag = tx_data.get("SourceTag")
            destination_tag = tx_data.get("DestinationTag")
            
            # Extract memo data if present
            memo_data = ""
            if "Memos" in tx_data and tx_data["Memos"]:
                for memo in tx_data["Memos"]:
                    if "Memo" in memo and "MemoData" in memo["Memo"]:
                        memo_data += memo["Memo"]["MemoData"] + " "
            
            # Convert UNIX timestamp to datetime
            tx_time = datetime.fromtimestamp(tx_data.get("date", 0))
            
            # Store raw transaction as JSON
            raw_tx = json.dumps(tx_data)
            
            self.con.execute("""
                INSERT OR REPLACE INTO transactions (
                    tx_hash, ledger_index, tx_index, account, destination, 
                    amount, fee, transaction_type, source_tag, destination_tag,
                    tx_time, tag_matched, memo_data, raw_tx
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tx_hash, ledger_index, tx_index, account, destination,
                amount, fee, tx_type, source_tag, destination_tag,
                tx_time, tag_matched, memo_data, raw_tx
            ))
            logger.debug(f"Stored transaction {tx_hash}")
        except Exception as e:
            logger.error(f"Error storing transaction: {e}")
    
    def get_latest_ledger_index(self) -> int:
        """Get the index of the latest stored ledger."""
        result = self.con.execute("SELECT MAX(ledger_index) FROM ledgers").fetchone()
        return result[0] if result and result[0] is not None else 0
    
    def get_ledger_range(self, start_index: int, end_index: int) -> List[Dict]:
        """Get the ledgers within a specific range."""
        query = """
            SELECT ledger_index, ledger_hash, ledger_time, tx_count, close_time
            FROM ledgers
            WHERE ledger_index BETWEEN ? AND ?
            ORDER BY ledger_index
        """
        result = self.con.execute(query, (start_index, end_index)).fetchall()
        return [dict(zip(["ledger_index", "ledger_hash", "ledger_time", "tx_count", "close_time"], row))
                for row in result]
    
    def get_transactions_with_tag(self, tag: str, limit: int = 100) -> pd.DataFrame:
        """
        Get transactions that match a specific tag in source_tag, destination_tag, or memo.
        Returns as pandas DataFrame for easy analysis.
        """
        query = f"""
            SELECT *
            FROM transactions
            WHERE tag_matched = true
            ORDER BY tx_time DESC
            LIMIT {limit}
        """
        return self.con.execute(query).df()
    
    def is_transaction_exists(self, tx_hash: str) -> bool:
        """Check if a transaction already exists in the database."""
        result = self.con.execute("SELECT 1 FROM transactions WHERE tx_hash = ?", (tx_hash,)).fetchone()
        return result is not None
    
    def is_ledger_exists(self, ledger_index: int) -> bool:
        """Check if a ledger already exists in the database."""
        result = self.con.execute("SELECT 1 FROM ledgers WHERE ledger_index = ?", (ledger_index,)).fetchone()
        return result is not None
    
    def close(self) -> None:
        """Close the database connection."""
        self.con.close()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 