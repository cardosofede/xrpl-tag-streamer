"""
MongoDB database module for storing XRPL transactions.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION

logger = logging.getLogger(__name__)

class MongoDatabase:
    """MongoDB database for storing XRPL transactions."""
    
    def __init__(
        self,
        mongo_uri: str = MONGO_URI,
        db_name: str = MONGO_DB_NAME,
        collection_name: str = MONGO_COLLECTION
    ):
        """
        Initialize the MongoDB database.
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: MongoDB database name
            collection_name: MongoDB collection name
        """
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self.collection: Optional[Collection] = None
        self.users_collection: Optional[Collection] = None
        
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        
        # Connect to MongoDB
        self.connect()
    
    def connect(self) -> None:
        """Connect to MongoDB and ensure collections exist."""
        try:
            logger.info(f"Connecting to MongoDB: {self.mongo_uri}")
            self.client = MongoClient(self.mongo_uri)
            
            # Check connection by pinging the database
            self.client.admin.command('ping')
            logger.info("Connected to MongoDB successfully")
            
            # Get database (will be created if it doesn't exist)
            self.db = self.client[self.db_name]
            
            # Check and create collections if they don't exist
            self._ensure_collections_exist()
            
            # Get references to collections
            self.collection = self.db[self.collection_name]
            self.users_collection = self.db["users"]
            
            # Create indexes
            self._create_indexes()
            
            logger.info(f"MongoDB initialization complete: database '{self.db_name}' with collections '{self.collection_name}' and 'users'")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def _ensure_collections_exist(self) -> None:
        """Ensure required collections exist in the database."""
        existing_collections = self.db.list_collection_names()
        logger.info(f"Existing collections: {existing_collections}")
        
        # Check and create transactions collection
        if self.collection_name not in existing_collections:
            logger.info(f"Creating collection: {self.collection_name}")
            self.db.create_collection(self.collection_name)
        
        # Check and create users collection
        if "users" not in existing_collections:
            logger.info("Creating collection: users")
            self.db.create_collection("users")
    
    def _create_indexes(self) -> None:
        """Create indexes on collections."""
        logger.info("Creating indexes on collections")
        
        # Transactions collection indexes
        self.collection.create_index("hash", unique=True)
        self.collection.create_index("ledger_index")
        self.collection.create_index("Account")
        self.collection.create_index("Destination")
        self.collection.create_index("user_id")
        logger.info(f"Created indexes on '{self.collection_name}' collection")
        
        # Users collection indexes
        self.users_collection.create_index("id", unique=True)
        logger.info("Created indexes on 'users' collection")
    
    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def store_transaction(self, tx: Dict[str, Any], user_id: str) -> bool:
        """
        Store a transaction in MongoDB.
        
        Args:
            tx: The transaction data
            user_id: ID of the user who owns the wallet
            
        Returns:
            bool: True if successfully stored, False otherwise
        """
        if not self.collection:
            logger.error("MongoDB collection not available")
            return False
        
        # Add user_id to transaction
        tx["user_id"] = user_id
        
        try:
            # Use hash as unique identifier
            tx_hash = tx.get("hash")
            if not tx_hash:
                logger.warning("Transaction has no hash, skipping")
                return False
            
            # Update or insert transaction
            self.collection.update_one(
                {"hash": tx_hash},
                {"$set": tx},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store transaction: {e}")
            return False
    
    def get_transactions(self, user_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get transactions from MongoDB.
        
        Args:
            user_id: Filter by user ID (optional)
            limit: Maximum number of transactions to return
            
        Returns:
            List[Dict]: List of transactions
        """
        if not self.collection:
            logger.error("MongoDB collection not available")
            return []
        
        query = {}
        if user_id:
            query["user_id"] = user_id
        
        try:
            return list(self.collection.find(query).limit(limit))
        except Exception as e:
            logger.error(f"Failed to get transactions: {e}")
            return []
    
    # User management methods
    
    def get_users(self) -> List[Dict[str, Any]]:
        """
        Get all users from MongoDB.
        
        Returns:
            List[Dict]: List of users with their wallets
        """
        if not self.users_collection:
            logger.error("MongoDB users collection not available")
            return []
        
        try:
            return list(self.users_collection.find({}))
        except Exception as e:
            logger.error(f"Failed to get users: {e}")
            return []
    
    def add_user(self, user_id: str, wallets: List[str]) -> bool:
        """
        Add a new user to MongoDB or update an existing one.
        
        Args:
            user_id: Unique identifier for the user
            wallets: List of wallet addresses
            
        Returns:
            bool: True if successfully added/updated, False otherwise
        """
        if not self.users_collection:
            logger.error("MongoDB users collection not available")
            return False
        
        try:
            # Create user document
            user = {
                "id": user_id,
                "wallets": wallets
            }
            
            # Update or insert user
            self.users_collection.update_one(
                {"id": user_id},
                {"$set": user},
                upsert=True
            )
            logger.info(f"Added/updated user: {user_id} with {len(wallets)} wallets")
            return True
        except Exception as e:
            logger.error(f"Failed to add/update user {user_id}: {e}")
            return False
    
    def remove_user(self, user_id: str) -> bool:
        """
        Remove a user from MongoDB.
        
        Args:
            user_id: Unique identifier for the user
            
        Returns:
            bool: True if successfully removed, False otherwise
        """
        if not self.users_collection:
            logger.error("MongoDB users collection not available")
            return False
        
        try:
            result = self.users_collection.delete_one({"id": user_id})
            if result.deleted_count > 0:
                logger.info(f"Removed user: {user_id}")
                return True
            else:
                logger.warning(f"User {user_id} not found")
                return False
        except Exception as e:
            logger.error(f"Failed to remove user {user_id}: {e}")
            return False
    
    def add_wallet_to_user(self, user_id: str, wallet: str) -> bool:
        """
        Add a wallet to an existing user.
        
        Args:
            user_id: Unique identifier for the user
            wallet: Wallet address to add
            
        Returns:
            bool: True if successfully added, False otherwise
        """
        if not self.users_collection:
            logger.error("MongoDB users collection not available")
            return False
        
        try:
            # Add wallet to user's wallet list if it doesn't exist
            result = self.users_collection.update_one(
                {"id": user_id},
                {"$addToSet": {"wallets": wallet}}
            )
            
            if result.matched_count > 0:
                logger.info(f"Added wallet {wallet} to user {user_id}")
                return True
            else:
                logger.warning(f"User {user_id} not found")
                return False
        except Exception as e:
            logger.error(f"Failed to add wallet to user {user_id}: {e}")
            return False
    
    def remove_wallet_from_user(self, user_id: str, wallet: str) -> bool:
        """
        Remove a wallet from an existing user.
        
        Args:
            user_id: Unique identifier for the user
            wallet: Wallet address to remove
            
        Returns:
            bool: True if successfully removed, False otherwise
        """
        if not self.users_collection:
            logger.error("MongoDB users collection not available")
            return False
        
        try:
            # Remove wallet from user's wallet list
            result = self.users_collection.update_one(
                {"id": user_id},
                {"$pull": {"wallets": wallet}}
            )
            
            if result.matched_count > 0:
                logger.info(f"Removed wallet {wallet} from user {user_id}")
                return True
            else:
                logger.warning(f"User {user_id} not found")
                return False
        except Exception as e:
            logger.error(f"Failed to remove wallet from user {user_id}: {e}")
            return False
    
    def initialize_default_users(self, default_users: List[Dict[str, Any]]) -> None:
        """
        Initialize the users collection with default users if it's empty.
        
        Args:
            default_users: List of default user configurations
        """
        if not self.users_collection:
            logger.error("MongoDB users collection not available")
            return
        
        try:
            # Check if users collection is empty
            if self.users_collection.count_documents({}) == 0:
                logger.info("Initializing users collection with default users")
                
                # Insert default users
                for user in default_users:
                    self.add_user(user["id"], user["wallets"])
                
                logger.info(f"Added {len(default_users)} default users")
            else:
                logger.info("Users collection already contains data, skipping initialization")
        except Exception as e:
            logger.error(f"Failed to initialize default users: {e}")
            
    def verify_db_connection(self) -> bool:
        """
        Verify database connection is working and collections exist.
        
        Returns:
            bool: True if connection is verified, False otherwise
        """
        try:
            # Verify connection by pinging the server
            self.client.admin.command('ping')
            
            # Verify collections exist
            collections = self.db.list_collection_names()
            has_transactions = self.collection_name in collections
            has_users = "users" in collections
            
            if has_transactions and has_users:
                logger.info("MongoDB connection and collections verified")
                return True
            else:
                missing = []
                if not has_transactions:
                    missing.append(self.collection_name)
                if not has_users:
                    missing.append("users")
                logger.warning(f"Missing collections: {', '.join(missing)}")
                return False
        except Exception as e:
            logger.error(f"Failed to verify MongoDB connection: {e}")
            return False 