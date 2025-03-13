"""
XRPL Transaction Collector.
Periodically fetches transactions for a list of wallets and filters by source tag.
"""

import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models.requests import AccountTx, AccountOffers

from src.config import (
    XRPL_RPC_URL,
    SOURCE_TAG,
    DEFAULT_USERS,
    COLLECTION_FREQUENCY,
    USER_CONFIG_REFRESH_INTERVAL,
    FROM_LEDGER,
)
from src.mongo_client import MongoDatabase
from src.data_types import (
    XRPLAmount,
    OpenOrder,
    FilledOrder,
    DepositWithdrawal,
    TransactionType,
    OrderStatus,
    UserConfig
)
from src.utils.transaction_processor import (
    has_source_tag,
    analyze_transaction,
    is_deposit_or_withdrawal,
    is_offer_filled,
    is_market_trade,
    extract_trades_from_metadata,
    extract_amount
)

logger = logging.getLogger(__name__)


class XRPLCollector:
    """
    XRPL Transaction Collector.
    Fetches transactions from XRPL nodes and stores them in MongoDB.
    Following a clean processing pipeline approach.
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
        Initialize the collector.
        
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
        self.client = None
        self.running = False
        
        # Initialize database
        self.db = db or MongoDatabase()
        
        # Initialize users and wallet mappings
        self.users: List[UserConfig] = []
        self.user_wallets: Dict[str, List[str]] = {}  # user_id -> wallet addresses
        
        # Initialize statistics
        self.stats = {
            "total_transactions": 0,
            "matching_transactions": 0,
            "start_time": None,
            "last_config_refresh": None,
        }

    async def start(self) -> None:
        """Start the collector."""
        self.running = True
        logger.info(f"Starting collector with source tag: {self.source_tag}")
        
        logger.info(f"Collection frequency: {self.collection_frequency} seconds")
        logger.info(f"User config refresh interval: {self.user_config_refresh_interval} seconds")

        # Initialize users from database if available, or use DEFAULT_USERS
        self._refresh_user_config()
        
        # Start statistics
        self.stats = {
            "total_transactions": 0,
            "matching_transactions": 0,
            "start_time": datetime.now(),
            "last_config_refresh": datetime.now()
        }
        
        logger.info("Collector started")

        try:
            # Create client
            self.client = AsyncJsonRpcClient(self.rpc_url)

            # Run collection loop
            while self.running:
                start_time = datetime.now()
                logger.info(f"Starting collection cycle at {start_time}")

                # Check if we need to refresh user configuration
                self._check_refresh_user_config()

                # Process each user's wallets for new transactions
                for user in self.users:
                    user_id = user.id
                    wallets = user.wallets

                    logger.info(f"Processing {len(wallets)} wallets for user {user_id}")

                    for wallet in wallets:
                        try:
                            # Get the minimum ledger to start from
                            min_ledger = await self._get_min_ledger_index(user_id, wallet)
                            
                            # Process the wallet starting from the minimum ledger
                            await self._process_wallet(wallet, user_id, from_ledger=min_ledger)
                        except Exception as e:
                            logger.error(f"Error processing wallet {wallet} for user {user_id}: {e}")

                # Check for updates to open orders
                await self._check_open_orders()

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
    
    async def _get_min_ledger_index(self, user_id: str, wallet: str) -> int:
        """
        Get the minimum ledger index to start searching from for a wallet.
        This is either the last seen ledger or the minimum from config.
        
        Args:
            user_id: The user ID that owns this wallet
            wallet: The wallet address
            
        Returns:
            int: The minimum ledger index
        """
        # Get the last processed transaction for this wallet
        last_tx = self.db.get_transactions(user_id=user_id, wallet=wallet, limit=1)
        
        # Get the minimum ledger from the open orders for this wallet
        min_order_ledger = self.db.get_min_open_order_ledger(wallet)
        
        # Get the last processed ledger
        last_ledger = last_tx[0].get("ledger_index") if len(last_tx) > 0 else FROM_LEDGER
        
        # Return the minimum ledger index (the lowest of the two)
        if min_order_ledger and min_order_ledger < last_ledger:
            return min_order_ledger
        
        return last_ledger

    async def stop(self) -> None:
        """Stop the collector."""
        logger.info("Stopping XRPL collector")
        self.running = False

        # Print statistics
        self._print_stats()
        
        logger.info("Collector stopped")
    
    def _refresh_user_config(self) -> None:
        """Refresh user configuration from MongoDB."""
        logger.info("Refreshing user configuration from database")
        
        # Attempt to get users from database
        db_users = self.db.get_users()
        
        # If users exist in database, use them
        if db_users:
            self.users = [UserConfig(**user) for user in db_users]
            logger.info(f"Loaded {len(db_users)} users from database")
        else:
            # Otherwise initialize with default users
            logger.info("No users found in database, initializing with default users")
            self.db.initialize_default_users(DEFAULT_USERS)
            self.users = [UserConfig(**user) for user in self.db.get_users()]
        
        # Update user_wallets mapping
        self.user_wallets = {user.id: user.wallets for user in self.users}
        
        logger.info(f"Monitoring {len(self.users)} users with a total of {sum(len(user.wallets) for user in self.users)} wallets")
        
        # Update the last refresh time
        self.stats["last_config_refresh"] = datetime.now()
    
    def _check_refresh_user_config(self) -> None:
        """Check if we need to refresh user configuration."""
        if not self.stats["last_config_refresh"]:
            self._refresh_user_config()
            return
        
        elapsed = (datetime.now() - self.stats["last_config_refresh"]).total_seconds()
        if elapsed >= self.user_config_refresh_interval:
            logger.info(f"User config refresh interval elapsed ({elapsed:.2f} seconds), refreshing")
            self._refresh_user_config()

    async def _check_open_orders(self) -> None:
        """
        Check for updates on open orders by querying current account offers.
        This helps identify offers that were filled or canceled outside our direct observation.
        """
        # Get all open orders from database
        open_orders = self.db.get_open_orders()
        
        if not open_orders:
            logger.info("No open orders to check")
            return
        
        logger.info(f"Checking {len(open_orders)} open orders")
        
        # Group orders by account for efficient querying
        orders_by_account = {}
        for order in open_orders:
            account = order.get("account") or order.get("Account")
            if account:
                if account not in orders_by_account:
                    orders_by_account[account] = []
                orders_by_account[account].append(order)
        
        # Check each account's orders
        for account, account_orders in orders_by_account.items():
            try:
                # Get current offers for the account
                request = AccountOffers(account=account)
                response = await self.client.request(request)
                
                if not response.is_successful():
                    logger.error(f"Failed to get offers for account {account}: {response.result}")
                    continue
                
                current_offers = response.result.get("offers", [])
                
                # Create a map of sequence numbers to current offers for efficient lookups
                current_offers_map = {offer.get("seq"): offer for offer in current_offers}
                
                # Check each open order
                for order in account_orders:
                    # Standardize field names (some may use Account, some account)
                    sequence = order.get("sequence") or order.get("Sequence")
                    order_hash = order.get("hash")
                    
                    # If offer is no longer in the account's offers, it was either filled or canceled
                    if sequence not in current_offers_map:
                        # Mark as filled and move to filled orders
                        self._handle_filled_order(order)
                    else:
                        # The offer is still active, update the last checked time
                        self.db.update_open_order(
                            order_hash, 
                            {"last_checked_ledger": response.result.get("ledger_current_index")}
                        )
            except Exception as e:
                logger.error(f"Error checking orders for account {account}: {e}")
    
    def _handle_filled_order(self, order: Dict[str, Any]) -> None:
        """
        Handle an order that was detected as filled.
        
        Args:
            order: The order data
        """
        # Create a filled order record
        filled_order = {
            "hash": order.get("hash"),
            "account": order.get("account") or order.get("Account"),
            "sequence": order.get("sequence") or order.get("Sequence"),
            "created_ledger_index": order.get("created_ledger_index"),
            "resolved_ledger_index": order.get("last_checked_ledger"),
            "taker_gets": order.get("taker_gets") or order.get("TakerGets"),
            "taker_pays": order.get("taker_pays") or order.get("TakerPays"),
            "status": "filled",
            "user_id": order.get("user_id"),
            "transaction_type": order.get("transaction_type") or "OfferCreate",
            "created_date": order.get("created_date"),
            "resolution_date": datetime.now(),
            "resolution_method": "inferred"  # We're inferring this was filled
        }
        
        # Store filled order and remove from open orders
        self.db.store_filled_order(filled_order)
        self.db.delete_open_order(order.get("hash"))
        
        logger.info(f"Marked order {order.get('hash')} as filled (inferred)")

    async def _process_wallet(self, address: str, user_id: str, from_ledger: Optional[int] = -1) -> None:
        """
        Process transactions for a wallet following the clean pipeline approach.
        
        Args:
            address: The wallet address
            user_id: The user ID that owns this wallet
            from_ledger: The ledger index to start from
        """
        logger.info(f"Fetching transactions for wallet {address} from ledger {from_ledger}")
        ledger_index_min = from_ledger
        all_transactions_queried = False
        retries = 0
        
        while not all_transactions_queried and retries < 3:
            try:
                # Request transactions for the account
                request = AccountTx(
                    account=address,
                    ledger_index_min=ledger_index_min,
                    forward=True
                )
                response = await self.client.request(request)

                if not response.is_successful():
                    logger.error(f"Failed to fetch transactions for wallet {address}: {response.result}")
                    retries += 1
                    await asyncio.sleep(5.0)
                    continue

                # Extract transactions
                transactions = response.result.get("transactions", [])

                if len(transactions) == 0:
                    logger.debug(f"No transactions found for wallet {address}")
                    all_transactions_queried = True
                    continue

                logger.info(f"Processing {len(transactions)} transactions for wallet {address}")

                # Process each transaction following the pipeline approach
                for tx_info in transactions:
                    # Extract the main transaction from the response format
                    tx = tx_info.get("tx_json", tx_info.get("tx", {}))
                    
                    # Skip if not a complete transaction
                    if not tx or not tx.get("hash"):
                        continue
                    
                    # Ensure we have metadata
                    if "meta" not in tx and "metaData" in tx_info:
                        tx["meta"] = tx_info["metaData"]
                    
                    # Add ledger index if not present
                    if "ledger_index" not in tx:
                        tx["ledger_index"] = tx_info.get("ledger_index", 0)
                    
                    # Store the raw transaction and get the enriched version
                    await self._process_transaction(tx, user_id)
                    
                    # Update the minimum ledger index for next query
                    ledger_index_min = max(ledger_index_min, tx.get("ledger_index", 0))
                    
                    # Update statistics
                    self.stats["total_transactions"] += 1
                    if has_source_tag(tx, str(self.source_tag)):
                        self.stats["matching_transactions"] += 1

            except Exception as e:
                logger.error(f"Error processing wallet {address}: {e}")
                retries += 1
                await asyncio.sleep(5.0)
    
    async def _process_transaction(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process a transaction following the pipeline approach.
        
        Args:
            tx: The transaction data
            user_id: The user ID that owns the wallet
        """
        # First enrich the transaction with additional analysis, including balance changes
        enriched_tx = analyze_transaction(tx, self.user_wallets.get(user_id, []))
        
        # Store raw transaction in database if it has our tag
        has_tag = has_source_tag(tx, str(self.source_tag))
        if has_tag:
            self.db.store_transaction(enriched_tx, user_id)
            logger.info(f"Stored transaction {tx.get('hash')} for user {user_id}")
        
        # Process based on transaction type
        tx_type = tx.get("TransactionType")
        
        if tx_type == "Payment":
            # Check if this is a deposit/withdrawal or market trade
            deposit_withdrawal_type = is_deposit_or_withdrawal(tx, self.user_wallets.get(user_id, []))
            
            if deposit_withdrawal_type:
                # Process as deposit or withdrawal
                self._process_deposit_withdrawal(enriched_tx, user_id, deposit_withdrawal_type)
            elif is_market_trade(tx):
                # Process as market trade
                self._process_market_trade(enriched_tx, user_id)
        
        elif tx_type == "OfferCreate":
            # Check if the offer was filled immediately or not
            if is_offer_filled(tx):
                # Process as filled offer
                self._process_filled_offer(enriched_tx, user_id)
            else:
                # Process as open offer
                self._process_open_offer(enriched_tx, user_id)
        
        elif tx_type == "OfferCancel":
            # Process offer cancellation
            self._process_offer_cancel(enriched_tx, user_id)
    
    def _process_deposit_withdrawal(self, tx: Dict[str, Any], user_id: str, tx_type: str) -> None:
        """
        Process a deposit or withdrawal transaction.
        
        Args:
            tx: The transaction data
            user_id: The user ID
            tx_type: "deposit" or "withdrawal"
        """
        logger.info(f"Processing {tx_type} transaction {tx.get('hash')}")
        
        # Use balance changes to extract the exact amount if available
        amount = extract_amount(tx)
        balance_changes = tx.get("balance_changes", {})
        
        # If we have balance changes, use those for more accurate amount
        if balance_changes:
            account = tx.get("Account")
            destination = tx.get("Destination")
            target_account = destination if tx_type == "deposit" else account
            
            if target_account in balance_changes:
                for change in balance_changes[target_account]:
                    # For deposits, we want positive changes; for withdrawals, negative changes
                    change_value = float(change["value"])
                    if (tx_type == "deposit" and change_value > 0) or (tx_type == "withdrawal" and change_value < 0):
                        amount = XRPLAmount(
                            currency=change["currency"],
                            issuer=change.get("issuer"),
                            value=str(abs(change_value))
                        )
                        break
        
        # Create deposit/withdrawal record
        deposit_withdrawal = DepositWithdrawal(
            hash=tx.get("hash"),
            ledger_index=tx.get("ledger_index"),
            timestamp=datetime.fromtimestamp(tx.get("date", 0)),
            from_address=tx.get("Account"),
            to_address=tx.get("Destination"),
            amount=amount,
            type=tx_type,
            user_id=user_id
        )
        
        # Store in database
        self.db.store_deposit_withdrawal(deposit_withdrawal.dict())
    
    def _process_market_trade(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process a market trade (payment that consumed offers).
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing market trade transaction {tx.get('hash')}")
        
        # Extract trades from metadata, which now uses balance changes
        trades = extract_trades_from_metadata(tx)
        
        # Get balance changes to determine what was bought and sold
        balance_changes = tx.get("balance_changes", {})
        account = tx.get("Account")
        
        # Initialize sold and bought amounts
        sold_amount = None
        bought_amount = None
        
        # If we have balance changes, use those to determine what was traded
        if balance_changes and account in balance_changes:
            for change in balance_changes[account]:
                change_value = float(change["value"])
                if change_value < 0:  # Negative change means the account sold this asset
                    sold_amount = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=str(abs(change_value))
                    )
                elif change_value > 0:  # Positive change means the account bought this asset
                    bought_amount = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=change["value"]
                    )
        
        # If we couldn't determine from balance changes, use the transaction amount as a fallback
        if not sold_amount or not bought_amount:
            amount = extract_amount(tx)
            if tx_type == "Payment":
                # For payments, we assume the amount is what was sent (sold)
                sold_amount = amount
                bought_amount = amount  # This is a simplification; bought amount might be different
        
        # Create filled order record for the market trade
        filled_order = FilledOrder(
            hash=tx.get("hash"),
            account=tx.get("Account"),
            sequence=tx.get("Sequence", 0),
            created_ledger_index=tx.get("ledger_index"),
            resolved_ledger_index=tx.get("ledger_index"),
            taker_gets=sold_amount or extract_amount(tx),  # Amount being sold
            taker_pays=bought_amount or extract_amount(tx),  # Amount being bought
            status=OrderStatus.FILLED,
            user_id=user_id,
            transaction_type=TransactionType.PAYMENT,
            created_date=datetime.fromtimestamp(tx.get("date", 0)),
            resolution_date=datetime.fromtimestamp(tx.get("date", 0)),
            trades=trades
        )
        
        # Store in database
        self.db.store_filled_order(filled_order.dict())
    
    def _process_open_offer(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process an OfferCreate transaction that created an open offer.
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing open offer transaction {tx.get('hash')}")
        
        # Create open order record
        open_order = OpenOrder(
            hash=tx.get("hash"),
            account=tx.get("Account"),
            sequence=tx.get("Sequence"),
            created_ledger_index=tx.get("ledger_index"),
            last_checked_ledger=tx.get("ledger_index"),
            taker_gets=XRPLAmount.from_xrpl_amount(tx.get("TakerGets")),
            taker_pays=XRPLAmount.from_xrpl_amount(tx.get("TakerPays")),
            status=OrderStatus.OPEN,
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=datetime.fromtimestamp(tx.get("date", 0))
        )
        
        # Store in database
        self.db.store_open_order(open_order.dict())
    
    def _process_filled_offer(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process an OfferCreate transaction that was filled immediately.
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing filled offer transaction {tx.get('hash')}")
        
        # Extract trades from metadata, which now uses balance changes
        trades = extract_trades_from_metadata(tx)
        
        # Get balance changes to determine what was bought and sold
        balance_changes = tx.get("balance_changes", {})
        account = tx.get("Account")
        
        # Initialize filled amounts
        filled_gets = None
        filled_pays = None
        
        # If we have balance changes, use those to determine what was filled
        if balance_changes and account in balance_changes:
            for change in balance_changes[account]:
                change_value = float(change["value"])
                # In a filled offer, negative changes correspond to TakerGets (what was sold)
                if change_value < 0:
                    filled_gets = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=str(abs(change_value))
                    )
                # Positive changes correspond to TakerPays (what was bought)
                elif change_value > 0:
                    filled_pays = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=change["value"]
                    )
        
        # Create filled order record
        filled_order = FilledOrder(
            hash=tx.get("hash"),
            account=tx.get("Account"),
            sequence=tx.get("Sequence"),
            created_ledger_index=tx.get("ledger_index"),
            resolved_ledger_index=tx.get("ledger_index"),
            taker_gets=XRPLAmount.from_xrpl_amount(tx.get("TakerGets")),
            taker_pays=XRPLAmount.from_xrpl_amount(tx.get("TakerPays")),
            filled_gets=filled_gets,  # What was actually filled (sold)
            filled_pays=filled_pays,  # What was actually filled (bought)
            status=OrderStatus.FILLED,
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=datetime.fromtimestamp(tx.get("date", 0)),
            resolution_date=datetime.fromtimestamp(tx.get("date", 0)),
            trades=trades
        )
        
        # Store in database
        self.db.store_filled_order(filled_order.dict())
    
    def _process_offer_cancel(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process an OfferCancel transaction.
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing offer cancel transaction {tx.get('hash')}")
        
        # Find the open order that matches the sequence number
        account = tx.get("Account")
        offer_sequence = tx.get("OfferSequence")
        
        if not account or not offer_sequence:
            logger.warning(f"Cannot process OfferCancel without Account and OfferSequence: {tx.get('hash')}")
            return
        
        # Get the open order
        open_order = self.db.get_open_order_by_sequence(account, offer_sequence)
        
        if not open_order:
            logger.warning(f"Open order not found for account {account}, sequence {offer_sequence}")
            return
        
        # Create filled order record with canceled status
        filled_order = FilledOrder(
            hash=open_order.get("hash"),
            account=account,
            sequence=offer_sequence,
            created_ledger_index=open_order.get("created_ledger_index"),
            resolved_ledger_index=tx.get("ledger_index"),
            taker_gets=XRPLAmount.from_xrpl_amount(open_order.get("taker_gets")),
            taker_pays=XRPLAmount.from_xrpl_amount(open_order.get("taker_pays")),
            status=OrderStatus.CANCELED,
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=datetime.fromtimestamp(open_order.get("created_date", 0)),
            resolution_date=datetime.fromtimestamp(tx.get("date", 0)),
            cancel_tx_hash=tx.get("hash")
        )
        
        # Store in database and remove from open orders
        self.db.store_filled_order(filled_order.dict())
        self.db.delete_open_order(open_order.get("hash"))

    def _print_stats(self) -> None:
        """Print statistics."""
        if not self.stats["start_time"]:
            return
        
        runtime = (datetime.now() - self.stats["start_time"]).total_seconds()
        runtime_str = f"{runtime:.2f} seconds"
        if runtime > 60:
            runtime_str = f"{runtime / 60:.2f} minutes"
        if runtime > 3600:
            runtime_str = f"{runtime / 3600:.2f} hours"
        
        logger.info(f"Statistics for XRPL collector:")
        logger.info(f"  Runtime: {runtime_str}")
        logger.info(f"  Total transactions processed: {self.stats['total_transactions']}")
        logger.info(f"  Matching transactions: {self.stats['matching_transactions']}")


async def run_collector():
    """Run the collector as an async function."""
    collector = XRPLCollector()
    await collector.start()


def run_collector_sync():
    """Run the collector synchronously."""
    asyncio.run(run_collector())


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Run collector
    run_collector_sync()
