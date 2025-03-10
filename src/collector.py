"""
XRPL Transaction Collector.
Periodically fetches transactions for a list of wallets and filters by source tag.
"""

import asyncio
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models import Tx
from xrpl.models.requests import AccountTx

from src.config import (
    XRPL_RPC_URL,
    SOURCE_TAG,
    DEFAULT_USERS,
    COLLECTION_FREQUENCY,
    USER_CONFIG_REFRESH_INTERVAL, FROM_LEDGER
)
from src.db.database import MongoDatabase
from src.utils.transaction_utils import has_target_tag, enrich_transaction_metadata

logger = logging.getLogger(__name__)


class XRPLCollector:
    """
    XRPL Transaction Collector.
    Fetches transactions for a list of wallets and filters by source tag.
    """

    def __init__(
            self,
            rpc_url: str = XRPL_RPC_URL,
            source_tag: int = SOURCE_TAG,
            collection_frequency: int = COLLECTION_FREQUENCY,
            user_config_refresh_interval: int = USER_CONFIG_REFRESH_INTERVAL,
            db: Optional[MongoDatabase] = None,
    ):
        """
        Initialize the XRPL collector.
        
        Args:
            rpc_url: The XRPL JSON-RPC URL to connect to
            source_tag: The source tag to filter transactions for
            collection_frequency: How often to collect transactions (in seconds)
            user_config_refresh_interval: How often to refresh user configuration (in seconds)
            db: Optional database instance, will create one if not provided
        """
        self.rpc_url = rpc_url
        self.source_tag = source_tag
        self.collection_frequency = collection_frequency
        self.user_config_refresh_interval = user_config_refresh_interval

        # Initialize database and verify connection
        self.db = db or MongoDatabase()
        if not self.db.verify_db_connection():
            logger.error("Failed to verify MongoDB connection and collections")
            sys.exit(1)

        self.client = None
        self.running = False
        self.stats = {
            "total_transactions": 0,
            "matching_transactions": 0,
            "start_time": None,
            "last_config_refresh": None,
        }

        # Initialize the users collection with default users if it's empty
        self.db.initialize_default_users(DEFAULT_USERS)

        # Load users from database
        self.users = []
        self._refresh_user_config()

        logger.info("Collector initialization complete")

    async def start(self) -> None:
        """Start the transaction collector."""
        if self.running:
            logger.warning("Collector is already running")
            return

        self.running = True
        self.stats["start_time"] = datetime.now()

        logger.info(f"Starting XRPL collector, connecting to {self.rpc_url}")
        logger.info(f"Filtering for transactions with source tag: {self.source_tag}")
        logger.info(f"Collection frequency: {self.collection_frequency} seconds")
        logger.info(f"User configuration refresh interval: {self.user_config_refresh_interval} seconds")
        logger.info(
            f"Monitoring {len(self.users)} users with a total of {sum(len(user['wallets']) for user in self.users)} wallets")

        try:
            # Create client
            self.client = AsyncJsonRpcClient(self.rpc_url)

            # Run collection loop
            while self.running:
                start_time = datetime.now()
                logger.info(f"Starting collection cycle at {start_time}")

                # Check if we need to refresh user configuration
                self._check_refresh_user_config()

                # Process each user's wallets
                for user in self.users:
                    user_id = user["id"]
                    wallets = user["wallets"]

                    logger.info(f"Processing {len(wallets)} wallets for user {user_id}")

                    for wallet in wallets:
                        try:
                            last_tx = self.db.get_transactions(user_id=user_id, wallet=wallet, limit=1)
                            last_ledger = last_tx[0].get("ledger_index") if len(last_tx) > 0 else FROM_LEDGER
                            await self._process_wallet(wallet, user_id, from_ledger=last_ledger)
                        except Exception as e:
                            logger.error(f"Error processing wallet {wallet} for user {user_id}: {e}")

                # Calculate time to sleep
                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0, self.collection_frequency - elapsed)

                logger.info(f"Collection cycle completed in {elapsed:.2f} seconds")
                logger.info(f"Sleeping for {sleep_time:.2f} seconds")

                # Print statistics
                self._print_stats()

                # Sleep until next cycle
                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("Collector cancelled")
            self.running = False
        except Exception as e:
            logger.error(f"Error in collector: {e}")
            self.running = False
            raise
        finally:
            # Clean up
            if self.client:
                await self.client.close()

            # Close database connection
            if self.db:
                self.db.close()

    async def stop(self) -> None:
        """Stop the transaction collector."""
        logger.info("Stopping XRPL collector")
        self.running = False

        # Print statistics
        self._print_stats()

    def _refresh_user_config(self) -> None:
        """Refresh user configuration from MongoDB."""
        logger.info("Refreshing user configuration from MongoDB")

        try:
            # Get users from database
            users = self.db.get_users()

            # Update users list
            self.users = users

            # Update last refresh time
            self.stats["last_config_refresh"] = datetime.now()

            logger.info(f"User configuration refreshed: {len(self.users)} users loaded")
        except Exception as e:
            logger.error(f"Failed to refresh user configuration: {e}")

    def _check_refresh_user_config(self) -> None:
        """Check if user configuration needs to be refreshed."""
        if not self.stats["last_config_refresh"]:
            self._refresh_user_config()
            return

        elapsed = (datetime.now() - self.stats["last_config_refresh"]).total_seconds()
        if elapsed >= self.user_config_refresh_interval:
            self._refresh_user_config()

    async def _process_wallet(self, address: str, user_id: str, from_ledger: Optional[int] = -1) -> None:
        """
        Process transactions for a wallet.
        
        Args:
            address: The wallet address
            user_id: The user ID that owns this wallet
        """
        logger.info(f"Fetching transactions for wallet {address}")
        ledger_index_min = from_ledger
        all_transactions_queried = False
        retries = 0
        while not all_transactions_queried:
            try:
                # Request transactions for the account
                request = AccountTx(account=address,
                                    ledger_index_min=ledger_index_min,
                                    forward=True)
                response = await self.client.request(request)

                if not response.is_successful():
                    logger.error(f"Failed to fetch transactions for wallet {address}: {response.result}")
                    await asyncio.sleep(5.0)
                    if retries >= 3:
                        logger.error(f"Failed to fetch transactions for wallet {address} after 3 retries")
                        raise
                    retries += 1

                # Extract transactions
                transactions = response.result.get("transactions", [])

                if len(transactions) == 0:
                    logger.debug(f"No transactions found for wallet {address}")
                    all_transactions_queried = True
                    continue

                logger.info(f"Processing {len(transactions)} transactions for wallet {address}")

                # Process each transaction
                matching_count = 0
                for tx in transactions:
                    # Skip if not a complete transaction
                    if not isinstance(tx, dict) or not tx.get("hash"):
                        continue

                    # Check if this transaction has our source tag
                    tag_matched = has_target_tag(tx["tx_json"], str(self.source_tag))

                    # Only store matching transactions
                    if tag_matched:
                        # Enrich transaction with metadata analysis before storing
                        # First, make sure tx_json and meta are properly structured
                        if "tx_json" in tx and ("meta" in tx or "metaData" in tx):
                            # If the metadata is under metaData, standardize to meta key
                            if "metaData" in tx and "meta" not in tx:
                                tx["meta"] = tx["metaData"]
                            
                            # Merge tx_json and metadata for proper processing
                            tx_with_meta = tx["tx_json"].copy()
                            tx_with_meta["meta"] = tx["meta"]
                            
                            # Enrich the transaction with additional metadata analysis
                            enriched_tx = enrich_transaction_metadata(tx_with_meta)
                            
                            # Log if this is an offer and whether it was filled
                            if tx_with_meta.get("TransactionType") == "OfferCreate":
                                filled_status = "FILLED" if enriched_tx.get("offer_filled") else "NOT FILLED"
                                logger.info(f"OfferCreate transaction {tx.get('hash')} was {filled_status}")
                        else:
                            logger.warning(f"Transaction {tx.get('hash')} missing required tx_json or meta structure for enrichment")
                        
                        # Store transaction in database
                        self.db.store_transaction(tx, user_id)

                        # Update statistics
                        self.stats["matching_transactions"] += 1
                        matching_count += 1

                        logger.info(f"Found matching transaction with hash: {tx.get('hash')} for user {user_id}")

                    # Update total count
                    self.stats["total_transactions"] += 1
                    ledger_index_min = max(ledger_index_min, tx.get("ledger_index"))

                logger.info(f"Found {matching_count} matching transactions for wallet {address}")
            except Exception as e:
                logger.error(f"Error processing wallet {address}: {e}")
                raise

    def _print_stats(self) -> None:
        """Print collection statistics."""
        if not self.stats["start_time"]:
            return

        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)

        last_refresh = "Never"
        if self.stats["last_config_refresh"]:
            last_refresh = self.stats["last_config_refresh"].strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            f"Stats: Running for {int(hours)}h {int(minutes)}m {int(seconds)}s | "
            f"Total transactions: {self.stats['total_transactions']} | "
            f"Matching transactions: {self.stats['matching_transactions']} | "
            f"Users: {len(self.users)} | "
            f"Last config refresh: {last_refresh}"
        )


async def run_collector():
    """Run the XRPL collector as an async function."""
    collector = XRPLCollector()
    await collector.start()


def run_collector_sync():
    """Run the XRPL collector synchronously."""
    asyncio.run(run_collector())


if __name__ == "__main__":
    from src.utils.logger import setup_logging

    setup_logging()
    run_collector_sync()
