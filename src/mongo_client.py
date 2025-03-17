"""
MongoDB client for storing and retrieving XRPL transaction data.
"""

import logging
from typing import Dict, List, Any, Optional

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from src.config import MONGO_URI, MONGO_DB_NAME


logger = logging.getLogger(__name__)


class MongoDatabase:
    """MongoDB database client for XRPL transaction data."""

    def __init__(self, mongodb_uri: str = MONGO_URI, db_name: str = MONGO_DB_NAME):
        """
        Initialize the MongoDB client.
        
        Args:
            mongodb_uri: MongoDB connection URI
            db_name: Database name
        """
        self.client = MongoClient(mongodb_uri)
        self.db: Database = self.client[db_name]
        
        # Create collections
        self.transactions: Collection = self.db["transactions"]
        self.users: Collection = self.db["users"]
        self.open_orders: Collection = self.db["open_orders"]
        self.filled_orders: Collection = self.db["filled_orders"]
        self.deposits_withdrawals: Collection = self.db["deposits_withdrawals"]
        self.market_trades: Collection = self.db["market_trades"]
        self.canceled_orders: Collection = self.db["canceled_orders"]  # New collection for canceled orders
        
        # Create indexes
        self._create_indexes()

    def _create_indexes(self) -> None:
        """Create necessary indexes for collections."""
        # Transactions collection indexes
        self.transactions.create_index("hash", unique=True)
        self.transactions.create_index("ledger_index")
        self.transactions.create_index("Account")
        self.transactions.create_index("Destination")
        self.transactions.create_index("user_id")
        self.transactions.create_index("TransactionType")
        
        # Users collection indexes
        self.users.create_index("id", unique=True)
        
        # Open orders collection indexes
        self.open_orders.create_index("hash", unique=True)
        self.open_orders.create_index("account")
        self.open_orders.create_index("sequence")
        self.open_orders.create_index("created_ledger_index")
        self.open_orders.create_index([("status", pymongo.ASCENDING), ("account", pymongo.ASCENDING)])
        self.open_orders.create_index("user_id")
        
        # Filled orders collection indexes
        self.filled_orders.create_index("hash", unique=True)
        self.filled_orders.create_index("account")
        self.filled_orders.create_index("sequence")
        self.filled_orders.create_index("created_ledger_index")
        self.filled_orders.create_index("resolved_ledger_index")
        self.filled_orders.create_index("user_id")
        self.filled_orders.create_index("status")
        
        # Deposits/withdrawals collection indexes
        self.deposits_withdrawals.create_index("hash", unique=True)
        self.deposits_withdrawals.create_index("ledger_index")
        self.deposits_withdrawals.create_index("from_address")
        self.deposits_withdrawals.create_index("to_address")
        self.deposits_withdrawals.create_index("user_id")
        self.deposits_withdrawals.create_index("type")

        # Market trades collection indexes
        self.market_trades.create_index("hash", unique=True)
        self.market_trades.create_index("ledger_index")
        self.market_trades.create_index("taker_address")
        self.market_trades.create_index("maker_address")
        self.market_trades.create_index("user_id")
        self.market_trades.create_index("related_offer_sequence")
        self.market_trades.create_index("related_offer_hash")

        # Canceled orders collection indexes
        self.canceled_orders.create_index("hash", unique=True)
        self.canceled_orders.create_index("account")
        self.canceled_orders.create_index("sequence")
        self.canceled_orders.create_index("created_ledger_index")
        self.canceled_orders.create_index("canceled_ledger_index")
        self.canceled_orders.create_index("user_id")
        self.canceled_orders.create_index("cancel_tx_hash")

    def initialize_default_users(self, default_users: List[Dict[str, List[str]]]) -> None:
        """
        Initialize the users collection with default users.
        
        Args:
            default_users: Dictionary mapping user IDs to lists of wallet addresses
        """
        for user in default_users:
            id = user["id"]
            wallets = user["wallets"]
            self.users.update_one(
                {"id": id},
                {"$set": {"id": id, "wallets": wallets}},
                upsert=True
            )
            logger.info(f"Initialized user {id} with {len(wallets)} wallets")

    def get_users(self) -> List[Dict[str, Any]]:
        """
        Get all users from the database.
        
        Returns:
            List of user documents
        """
        return list(self.users.find({}))

    def store_transaction(self, tx: Dict[str, Any], user_id: str) -> str:
        """
        Store a raw transaction in the database.
        
        Args:
            tx: Transaction data
            user_id: User ID
            
        Returns:
            str: Transaction hash
        """
        # Add user_id to the transaction
        tx["user_id"] = user_id
        
        # Insert or update the transaction
        self.transactions.update_one(
            {"hash": tx["hash"]},
            {"$set": tx},
            upsert=True
        )
        
        return tx["hash"]

    def get_transactions(self, user_id: Optional[str] = None, wallet: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get transactions from the database.
        
        Args:
            user_id: Filter by user ID
            wallet: Filter by wallet address
            limit: Maximum number of transactions to return
            
        Returns:
            List of transaction documents
        """
        query = {}
        if user_id:
            query["user_id"] = user_id
        if wallet:
            query["$or"] = [{"Account": wallet}, {"Destination": wallet}]
            
        return list(self.transactions.find(query).sort("ledger_index", -1).limit(limit))

    def store_open_order(self, order: Dict[str, Any]) -> str:
        """
        Store an open order in the database.
        
        Args:
            order: Open order data
            
        Returns:
            str: Order hash
        """
        # Insert or update the order
        self.open_orders.update_one(
            {"hash": order["hash"]},
            {"$set": order},
            upsert=True
        )
        
        logger.info(f"Stored open order {order['hash']} for user {order['user_id']}")
        
        return order["hash"]

    def get_open_orders(self, account: Optional[str] = None, status: Optional[str] = None, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get open orders from the database.
        
        Args:
            account: Filter by account
            status: Filter by status
            user_id: Filter by user ID
            
        Returns:
            List of open order documents
        """
        query = {}
        if account:
            query["account"] = account
        if status:
            query["status"] = status
        if user_id:
            query["user_id"] = user_id
            
        return list(self.open_orders.find(query))

    def get_open_order_by_sequence(self, account: str, sequence: int) -> Optional[Dict[str, Any]]:
        """
        Get an open order by account and sequence.
        
        Args:
            account: Account address
            sequence: Offer sequence number
            
        Returns:
            Optional[Dict[str, Any]]: Open order document or None if not found
        """
        return self.open_orders.find_one({"account": account, "sequence": sequence})

    def update_open_order(self, order_hash: str, update_data: Dict[str, Any]) -> bool:
        """
        Update an open order in the database.
        
        Args:
            order_hash: Order hash
            update_data: Data to update
            
        Returns:
            bool: True if updated, False if not found
        """
        result = self.open_orders.update_one(
            {"hash": order_hash},
            {"$set": update_data}
        )
        
        return result.modified_count > 0

    def delete_open_order(self, order_hash: str) -> bool:
        """
        Delete an open order from the database.
        
        Args:
            order_hash: Order hash
            
        Returns:
            bool: True if deleted, False if not found
        """
        result = self.open_orders.delete_one({"hash": order_hash})
        
        return result.deleted_count > 0

    def store_filled_order(self, order: Dict[str, Any]) -> str:
        """
        Store a filled order in the database.
        
        Args:
            order: Filled order data
            
        Returns:
            str: Order hash
        """
        # Insert or update the order
        self.filled_orders.update_one(
            {"hash": order["hash"]},
            {"$set": order},
            upsert=True
        )
        
        logger.info(f"Stored filled order {order['hash']} for user {order['user_id']}")
        
        return order["hash"]

    def get_filled_orders(self, account: Optional[str] = None, status: Optional[str] = None, user_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get filled orders from the database.
        
        Args:
            account: Filter by account
            status: Filter by status
            user_id: Filter by user ID
            limit: Maximum number of orders to return
            
        Returns:
            List of filled order documents
        """
        query = {}
        if account:
            query["account"] = account
        if status:
            query["status"] = status
        if user_id:
            query["user_id"] = user_id
            
        return list(self.filled_orders.find(query).sort("resolved_ledger_index", -1).limit(limit))

    def store_deposit_withdrawal(self, deposit_withdrawal: Dict[str, Any]) -> str:
        """
        Store a deposit or withdrawal in the database.
        
        Args:
            deposit_withdrawal: Deposit or withdrawal data
            
        Returns:
            str: Transaction hash
        """
        # Insert or update the deposit/withdrawal
        self.deposits_withdrawals.update_one(
            {"hash": deposit_withdrawal["hash"]},
            {"$set": deposit_withdrawal},
            upsert=True
        )
        
        logger.info(f"Stored {deposit_withdrawal['type']} {deposit_withdrawal['hash']} for user {deposit_withdrawal['user_id']}")
        
        return deposit_withdrawal["hash"]

    def get_deposits_withdrawals(self, user_id: Optional[str] = None, tx_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get deposits and withdrawals from the database.
        
        Args:
            user_id: Filter by user ID
            tx_type: Filter by type ("deposit" or "withdrawal")
            limit: Maximum number of records to return
            
        Returns:
            List of deposit/withdrawal documents
        """
        query = {}
        if user_id:
            query["user_id"] = user_id
        if tx_type:
            query["type"] = tx_type
            
        return list(self.deposits_withdrawals.find(query).sort("ledger_index", -1).limit(limit))

    def get_min_open_order_ledger(self, account: str) -> Optional[int]:
        """
        Get the minimum ledger index of open orders for an account.
        
        Args:
            account: Account address
            
        Returns:
            Optional[int]: Minimum ledger index or None if no open orders
        """
        pipeline = [
            {"$match": {"account": account}},
            {"$group": {"_id": None, "min_ledger": {"$min": "$created_ledger_index"}}}
        ]
        
        result = list(self.open_orders.aggregate(pipeline))
        
        if result and "min_ledger" in result[0]:
            return result[0]["min_ledger"]
        
        return None

    def get_transaction_by_hash(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get a transaction by hash.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Optional[Dict[str, Any]]: Transaction document or None if not found
        """
        return self.transactions.find_one({"hash": tx_hash})

    def update_transaction(self, tx: Dict[str, Any]) -> bool:
        """
        Update a transaction in the database.
        
        Args:
            tx: Transaction data
            
        Returns:
            bool: True if updated, False if not found
        """
        result = self.transactions.update_one(
            {"hash": tx["hash"]},
            {"$set": tx}
        )
        
        return result.modified_count > 0

    def store_market_trade(self, trade: Dict[str, Any]) -> str:
        """
        Store a market trade that filled one of our orders.
        
        Args:
            trade: Market trade data
            
        Returns:
            str: Trade hash
        """
        # Insert or update the trade
        self.market_trades.update_one(
            {"hash": trade["hash"]},
            {"$set": trade},
            upsert=True
        )
        
        logger.info(f"Stored market trade {trade['hash']} for user {trade['user_id']}")
        
        return trade["hash"]

    def get_market_trades(self, user_id: Optional[str] = None, related_offer_hash: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get market trades from the database.
        
        Args:
            user_id: Filter by user ID
            related_offer_hash: Filter by related offer hash
            limit: Maximum number of trades to return
            
        Returns:
            List of market trade documents
        """
        query = {}
        if user_id:
            query["user_id"] = user_id
        if related_offer_hash:
            query["related_offer_hash"] = related_offer_hash
            
        return list(self.market_trades.find(query).sort("ledger_index", -1).limit(limit))

    def update_market_trade(self, trade_hash: str, update_data: Dict[str, Any]) -> bool:
        """
        Update a market trade in the database.
        
        Args:
            trade_hash: Trade hash
            update_data: Data to update
            
        Returns:
            bool: True if updated, False if not found
        """
        result = self.market_trades.update_one(
            {"hash": trade_hash},
            {"$set": update_data}
        )
        
        return result.modified_count > 0

    def store_canceled_order(self, order: Dict[str, Any]) -> str:
        """
        Store a canceled order in the database.
        
        Args:
            order: Canceled order data
            
        Returns:
            str: Order hash
        """
        # Insert or update the order
        self.canceled_orders.update_one(
            {"hash": order["hash"]},
            {"$set": order},
            upsert=True
        )
        
        logger.info(f"Stored canceled order {order['hash']} for user {order['user_id']}")
        
        return order["hash"]

    def get_canceled_orders(self, account: Optional[str] = None, user_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get canceled orders from the database.
        
        Args:
            account: Filter by account
            user_id: Filter by user ID
            limit: Maximum number of orders to return
            
        Returns:
            List of canceled order documents
        """
        query = {}
        if account:
            query["account"] = account
        if user_id:
            query["user_id"] = user_id
            
        return list(self.canceled_orders.find(query).sort("canceled_ledger_index", -1).limit(limit)) 