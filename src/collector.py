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
from xrpl.models.requests import AccountTx, AccountOffers, Tx
from xrpl.utils import ripple_time_to_datetime

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
    UserConfig, CanceledOrder, Trade
)
from src.utils.transaction_processor import (
    has_source_tag,
    analyze_transaction,
    is_market_trade,
    extract_amount,
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
        This is the maximum ledger index seen in transactions for this wallet.
        
        Args:
            user_id: The user ID that owns this wallet
            wallet: The wallet address
            
        Returns:
            int: The minimum ledger index to start searching from
        """
        # Get the last processed transaction for this wallet
        last_tx = self.db.get_transactions(user_id=user_id, wallet=wallet, limit=1)
        
        # Get the maximum ledger index from transactions
        max_ledger = last_tx[0].get("ledger_index") if len(last_tx) > 0 else FROM_LEDGER
        
        # Return the maximum ledger index as our starting point
        return max_ledger

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
            # Keep the original taker_gets and taker_pays values from the open order
            "taker_gets": order.get("taker_gets") or order.get("TakerGets"),
            "taker_pays": order.get("taker_pays") or order.get("TakerPays"),
            # For inferred fills, we assume it was fully filled (we don't have precise data)
            "filled_gets": order.get("taker_gets") or order.get("TakerGets"),
            "filled_pays": order.get("taker_pays") or order.get("TakerPays"),
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
                    forward=True,
                    limit=400,
                )
                response = await self.client.request(request)

                if not response.is_successful():
                    logger.error(f"Failed to fetch transactions for wallet {address}: {response.result}")
                    retries += 1
                    await asyncio.sleep(5.0)
                    continue

                # Extract transactions
                transactions = response.result.get("transactions", [])

                if len(transactions) <= 1:
                    logger.debug(f"No transactions found for wallet {address}")
                    all_transactions_queried = True

                logger.info(f"Processing {len(transactions)} transactions for wallet {address}")

                # Process each transaction following the pipeline approach
                for tx in transactions:
                    # Skip if not a complete transaction
                    if not tx.get("hash"):
                        continue
                    
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

        # Process based on transaction type
        tx_json = tx.get("tx_json", {})
        tx_type = tx_json.get("TransactionType")

        # Store raw transaction in database if it has our tag
        has_tag = has_source_tag(tx, str(self.source_tag))
        if has_tag or tx_type == "OfferCancel":
            self.db.store_transaction(enriched_tx, user_id)
            logger.info(f"Stored transaction {tx.get('hash')} for user {user_id}")

        if tx_type == "Payment":
            logger.info("Processing Payment transaction")
            # Check if this is a deposit/withdrawal or market trade
            if enriched_tx["transaction_nature"] in ["deposit", "withdrawal", "internal_transfer"]:
                # Process as deposit or withdrawal
                self._process_deposit_withdrawal(enriched_tx, user_id)
            elif is_market_trade(tx):
                # Check if this payment filled one of our offers
                if self._is_payment_filling_our_offer(tx, user_id):
                    # Process as market trade that filled our offer
                    await self._process_offer_filled_by_payment(tx, user_id)
                else:
                    # Process as regular market trade
                    self._process_market_trade(enriched_tx, user_id)

        elif tx_type == "OfferCreate":
            logger.info("Processing OfferCreate transaction")
            # Check if the offer was filled immediately or not
            if enriched_tx["offer_filled"]:
                # Process as filled offer
                self._process_filled_offer(enriched_tx, user_id)
            else:
                # Process as open offer
                self._process_open_offer(enriched_tx, user_id)

        elif tx_type == "OfferCancel":
            logger.info("Processing OfferCancel transaction")
            # Process offer cancellation
            self._process_offer_cancel(enriched_tx, user_id)
    
    def _process_deposit_withdrawal(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process a deposit or withdrawal transaction.
        
        Args:
            tx: The transaction data
            user_id: The user ID
            tx_type: "deposit" or "withdrawal"
        """
        tx_type = tx.get("transaction_nature")
        balance_changes = tx.get("balance_changes", [])
        tx_json = tx.get("tx_json", {})
        fee_xrp = tx.get("fee_xrp", 0.0)

        logger.info(f"Processing {tx_type} transaction {tx.get('hash')}")

        # If we have balance changes, use those for more accurate amount
        if balance_changes:
            account = tx_json.get("Account")
            destination = tx_json.get("Destination")
            target_account = destination if tx_type == "deposit" else account
            
            for balance_change in balance_changes:
                if balance_change["account"] == target_account:
                    for change in balance_change["balances"]:
                        # For deposits, we want positive changes; for withdrawals, negative changes
                        change_value = float(change["value"])
                        
                        # Skip XRP changes that match the transaction fee
                        if change["currency"] == "XRP" and tx_type == "withdrawal" and abs(change_value + fee_xrp) < 0.000001:
                            continue
                            
                        if (tx_type == "deposit" and change_value > 0) or (tx_type == "withdrawal" and change_value < 0):
                            if change["currency"] == "XRP" and tx_type == "withdrawal":
                                value = abs(change_value) - fee_xrp
                            else:
                                value = abs(change_value)
                            amount = XRPLAmount(
                                currency=change["currency"],
                                issuer=change.get("issuer"),
                                value=str(value)
                            )
                            break
        
        # Create deposit/withdrawal record
        deposit_withdrawal = DepositWithdrawal(
            hash=tx.get("hash"),
            ledger_index=tx.get("ledger_index"),
            timestamp=ripple_time_to_datetime(tx_json.get("date", 0)),
            from_address=tx_json.get("Account"),
            to_address=tx_json.get("Destination"),
            amount=amount,
            type=tx_type,
            user_id=user_id,
            fee_xrp=fee_xrp if tx_type != "deposit" else 0,
        )
        
        # Store in database
        self.db.store_deposit_withdrawal(deposit_withdrawal.model_dump())
    
    def _process_market_trade(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process a market trade (cross-currency payment).
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing market trade transaction {tx.get('hash')}")
        
        # Extract trades from metadata, which now uses balance changes
        trades = tx.get("trades", [])
        
        # Get balance changes to determine what was bought and sold
        balance_changes = tx.get("balance_changes", [])
        tx_json = tx.get("tx_json", {})
        account = tx_json.get("Account")
        fee_xrp = tx.get("fee_xrp", 0.0)
        tx_type = tx_json.get("TransactionType")
        
        # Initialize sold and bought amounts (for filled amounts)
        sold_amount = None
        bought_amount = None
        
        # If we have balance changes, use those to determine what was traded
        for balance_change in balance_changes:
            if balance_change["account"] == account:
                for change in balance_change["balances"]:
                    # Skip XRP changes that match the transaction fee
                    if change["currency"] == "XRP" and abs(float(change["value"]) + fee_xrp) < 0.000001:
                        continue
                        
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
        
        # For Payment transactions, the original amount is in the tx_json
        original_amount = extract_amount(tx)
        
        # Create trade record
        trade = Trade(
            hash=tx.get("hash"),
            ledger_index=tx.get("ledger_index"),
            timestamp=ripple_time_to_datetime(tx_json.get("date", 0)),
            taker_address=tx_json.get("Account"),
            maker_address=tx_json.get("Destination"),
            sold_amount=sold_amount,
            bought_amount=bought_amount,
            user_id=user_id,
            fee_xrp=fee_xrp
        )
        
        # Store trade
        self.db.store_trade(trade.model_dump())
        
        # Create filled order record for the market trade
        filled_order = FilledOrder(
            hash=tx.get("hash"),
            account=tx_json.get("Account"),
            sequence=tx_json.get("Sequence", 0),
            created_ledger_index=tx.get("ledger_index"),
            resolved_ledger_index=tx.get("ledger_index"),
            # For Payment transactions, set these based on the Amount field
            taker_gets=original_amount,  # Original amount specified in the payment
            taker_pays=original_amount,  # For payments, we don't know the original expected amount
            # Set filled amounts based on balance changes
            filled_gets=sold_amount,  # What was actually sold
            filled_pays=bought_amount,  # What was actually bought
            status=OrderStatus.FILLED,
            user_id=user_id,
            transaction_type=TransactionType.PAYMENT,
            created_date=ripple_time_to_datetime(tx_json.get("date", 0)),
            resolution_date=ripple_time_to_datetime(tx_json.get("date", 0)),
            trades=[trade],
            fee_xrp=fee_xrp  # Include fee information
        )
        
        # Store in database
        self.db.store_filled_order(filled_order.model_dump())
    
    def _process_open_offer(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process an OfferCreate transaction that created an open offer.
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing open offer transaction {tx.get('hash')}")
        
        tx_json = tx.get("tx_json", {})

        # Create open order record
        open_order = OpenOrder(
            hash=tx.get("hash"),
            account=tx_json.get("Account"),
            sequence=tx_json.get("Sequence"),
            created_ledger_index=tx.get("ledger_index"),
            last_checked_ledger=tx.get("ledger_index"),
            taker_gets=XRPLAmount.from_xrpl_amount(tx_json.get("TakerGets")),
            taker_pays=XRPLAmount.from_xrpl_amount(tx_json.get("TakerPays")),
            status=OrderStatus.OPEN,
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=ripple_time_to_datetime(tx_json.get("date", 0)),
            fee_xrp=tx.get("fee_xrp")  # Include fee information
        )
        
        # Store in database
        self.db.store_open_order(open_order.model_dump())
    
    def _process_filled_offer(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process an OfferCreate transaction that was filled immediately.
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing filled offer transaction {tx.get('hash')}")
        
        # Extract trades from metadata, which now uses balance changes
        trades = tx.get("trades", [])
        
        # Get balance changes to determine what was bought and sold
        balance_changes = tx.get("balance_changes", [])
        tx_json = tx.get("tx_json", {})
        account = tx_json.get("Account")
        fee_xrp = tx.get("fee_xrp", 0.0)
        
        # Initialize filled amounts
        filled_gets = None
        filled_pays = None
        
        # If we have balance changes, use those to determine what was filled
        for balance_change in balance_changes:
            if balance_change["account"] == account:
                for change in balance_change["balances"]:
                    # Skip XRP changes that match the transaction fee
                    if change["currency"] == "XRP" and abs(float(change["value"]) + fee_xrp) < 0.000001:
                        continue
                    
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
        
        # Create filled order record with original taker_gets and taker_pays from tx_json
        # and the actual filled_gets and filled_pays from balance changes
        filled_order = FilledOrder(
            hash=tx.get("hash"),
            account=tx_json.get("Account"),
            sequence=tx_json.get("Sequence"),
            created_ledger_index=tx.get("ledger_index"),
            resolved_ledger_index=tx.get("ledger_index"),
            # Use original offer values from tx_json
            taker_gets=XRPLAmount.from_xrpl_amount(tx_json.get("TakerGets")),
            taker_pays=XRPLAmount.from_xrpl_amount(tx_json.get("TakerPays")),
            # Use filled values from balance changes
            filled_gets=filled_gets,
            filled_pays=filled_pays,
            status=OrderStatus.FILLED,
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=ripple_time_to_datetime(tx_json.get("date", 0)),
            resolution_date=ripple_time_to_datetime(tx_json.get("date", 0)),
            trades=trades,
            fee_xrp=fee_xrp
        )
        
        # Store in database
        self.db.store_filled_order(filled_order.model_dump())
    
    def _process_offer_cancel(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process an OfferCancel transaction.
        
        Args:
            tx: The transaction data
            user_id: The user ID
        """
        logger.info(f"Processing offer cancel transaction {tx.get('hash')}")
        
        # Find the open order that matches the sequence number
        tx_json = tx.get("tx_json", {})
        account = tx_json.get("Account")
        offer_sequence = tx_json.get("OfferSequence")
        cancel_fee_xrp = tx.get("fee_xrp")
        
        if not account or not offer_sequence:
            logger.warning(f"Cannot process OfferCancel without Account and OfferSequence: {tx.get('hash')}")
            return
        
        # Get the open order
        open_order = self.db.get_open_order_by_sequence(account, offer_sequence)
        
        if not open_order:
            logger.warning(f"Open order not found for account {account}, sequence {offer_sequence}")
            return
        if OrderStatus.OPEN == open_order["status"]:
            # Process as canceled order
            self._process_canceled_order(open_order, tx, user_id, cancel_fee_xrp)
        else:
            # Process as filled order with partial fill
            self._process_partially_filled_order(open_order, tx, user_id, cancel_fee_xrp)

    def _process_partially_filled_order(self, open_order: Dict[str, Any], cancel_tx: Dict[str, Any], user_id: str, cancel_fee_xrp: float) -> None:
        """
        Process a partially filled order that was canceled.
        
        Args:
            open_order: The original open order
            cancel_tx: The cancel transaction
            user_id: The user ID
            cancel_fee_xrp: Fee for the cancel transaction
        """
        # Create filled order record
        filled_order = FilledOrder(
            hash=open_order.get("hash"),
            account=open_order.get("account"),
            sequence=open_order.get("sequence"),
            created_ledger_index=open_order.get("created_ledger_index"),
            resolved_ledger_index=cancel_tx.get("ledger_index"),
            taker_gets=XRPLAmount.from_xrpl_amount(open_order.get("taker_gets")),
            taker_pays=XRPLAmount.from_xrpl_amount(open_order.get("taker_pays")),
            filled_gets=XRPLAmount.from_xrpl_amount(open_order.get("filled_gets")),
            filled_pays=XRPLAmount.from_xrpl_amount(open_order.get("filled_pays")),
            status=OrderStatus.PARTIALLY_FILLED,
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=open_order.get("created_date"),
            resolution_date=ripple_time_to_datetime(cancel_tx.get("tx_json", {}).get("date", 0)),
            cancel_tx_hash=cancel_tx.get("hash"),
            fee_xrp=open_order.get("fee_xrp", 0.0) + cancel_fee_xrp  # Total fees
        )
        
        # Store in database and remove from open orders
        self.db.store_filled_order(filled_order.model_dump())
        self.db.delete_open_order(open_order.get("hash"))

    def _process_canceled_order(self, open_order: Dict[str, Any], cancel_tx: Dict[str, Any], user_id: str, cancel_fee_xrp: float) -> None:
        """
        Process a canceled order that was not filled.
        
        Args:
            open_order: The original open order
            cancel_tx: The cancel transaction
            user_id: The user ID
            cancel_fee_xrp: Fee for the cancel transaction
        """
        # Create canceled order record
        canceled_order = CanceledOrder(
            hash=open_order.get("hash"),
            account=open_order.get("account"),
            sequence=open_order.get("sequence"),
            created_ledger_index=open_order.get("created_ledger_index"),
            canceled_ledger_index=cancel_tx.get("ledger_index"),
            taker_gets=XRPLAmount.from_xrpl_amount(open_order.get("taker_gets")),
            taker_pays=XRPLAmount.from_xrpl_amount(open_order.get("taker_pays")),
            user_id=user_id,
            transaction_type=TransactionType.OFFER_CREATE,
            created_date=open_order.get("created_date"),
            canceled_date=ripple_time_to_datetime(cancel_tx.get("tx_json", {}).get("date", 0)),
            cancel_tx_hash=cancel_tx.get("hash"),
            create_fee_xrp=open_order.get("fee_xrp", 0.0),
            cancel_fee_xrp=cancel_fee_xrp,
            fee_xrp=open_order.get("fee_xrp", 0.0) + cancel_fee_xrp
        )
        
        # Store in database and remove from open orders
        self.db.store_canceled_order(canceled_order.model_dump())
        self.db.delete_open_order(open_order.get("hash"))

    def _is_payment_filling_our_offer(self, tx: Dict[str, Any], user_id: str) -> bool:
        """
        Check if a payment transaction is filling one of our offers.
        
        Args:
            tx: Transaction data
            user_id: User ID
            
        Returns:
            bool: True if payment is filling one of our offers
        """
        meta = tx.get("meta") or tx.get("metaData", {})
        if not meta or isinstance(meta, str):
            return False
            
        # Get affected nodes
        affected_nodes = meta.get("AffectedNodes", [])
        
        # Look for modified or deleted offer nodes
        for node in affected_nodes:
            if "DeletedNode" in node or "ModifiedNode" in node:
                node_data = node.get("DeletedNode") or node.get("ModifiedNode", {})
                if node_data.get("LedgerEntryType") == "Offer":
                    # Get the offer owner
                    offer_owner = node_data.get("FinalFields", {}).get("Account")
                    if offer_owner in self.user_wallets.get(user_id, []):
                        return True
        return False

    async def _process_offer_filled_by_payment(self, tx: Dict[str, Any], user_id: str) -> None:
        """
        Process a payment transaction that filled one of our offers.
        
        Args:
            tx: Transaction data
            user_id: User ID
        """
        logger.info(f"Processing payment that filled our offer: {tx.get('hash')}")
        
        meta = tx.get("meta") or tx.get("metaData", {})
        affected_nodes = meta.get("AffectedNodes", [])
        tx_json = tx.get("tx_json", {})
        fee_xrp = tx.get("fee_xrp", 0.0)
        
        # Find the offer that was filled
        filled_offer = None
        prev_tx_id = None
        prev_tx_status = None
        for node in affected_nodes:
            key = next(iter(node))
            if key in ["DeletedNode", "ModifiedNode"]:
                node_data = node.get(key)
                if node_data.get("LedgerEntryType") == "Offer":
                    final_fields = node_data.get("FinalFields", {})
                    if final_fields.get("Account") in self.user_wallets.get(user_id, []):
                        filled_offer = final_fields
                        prev_tx_id = node_data.get("PreviousTxnID")
                        prev_tx_status = "partially_filled" if key == "ModifiedNode" else "filled"
                        break
        
        if not filled_offer:
            logger.warning(f"Could not find filled offer in transaction {tx.get('hash')}")
            return
            
        # Create trade record
        trade = Trade(
            hash=tx.get("hash"),
            ledger_index=tx.get("ledger_index"),
            timestamp=ripple_time_to_datetime(tx_json.get("date", 0)),
            taker_address=tx_json.get("Account"),  # The address that initiated the trade
            maker_address=filled_offer.get("Account"),  # Our address
            sold_amount=XRPLAmount.from_xrpl_amount(filled_offer.get("TakerGets")),  # What we sold
            bought_amount=XRPLAmount.from_xrpl_amount(filled_offer.get("TakerPays")),  # What we bought
            related_offer_sequence=filled_offer.get("Sequence"),
            related_offer_hash=prev_tx_id,  # Hash of the offer creation
            user_id=user_id,
            fee_xrp=fee_xrp
        )
        
        # Store trade
        self.db.store_trade(trade.model_dump())
        
        if prev_tx_id:
            # Get the original open order
            open_order = self.db.get_open_order_by_sequence(filled_offer.get("Account"), filled_offer.get("Sequence"))
            # Get existing trades for this order
            existing_trades = self.db.get_trades(related_offer_hash=prev_tx_id)

            if not open_order:
                logger.warning(f"Could not find original open order for sequence {filled_offer.get('Sequence')}")
                return
                
            if prev_tx_status == "filled":
                # Create filled order record
                filled_order = FilledOrder(
                    hash=open_order.get("hash"),
                    account=open_order.get("account"),
                    sequence=open_order.get("sequence"),
                    created_ledger_index=open_order.get("created_ledger_index"),
                    resolved_ledger_index=tx.get("ledger_index"),
                    taker_gets=XRPLAmount.from_xrpl_amount(open_order.get("taker_gets")),
                    taker_pays=XRPLAmount.from_xrpl_amount(open_order.get("taker_pays")),
                    filled_gets=XRPLAmount.from_xrpl_amount(open_order.get("taker_gets")),  # Fully filled
                    filled_pays=XRPLAmount.from_xrpl_amount(open_order.get("taker_pays")),  # Fully filled
                    status=OrderStatus.FILLED,
                    user_id=user_id,
                    transaction_type=TransactionType.OFFER_CREATE,
                    created_date=open_order.get("created_date"),
                    resolution_date=ripple_time_to_datetime(tx_json.get("date", 0)),
                    trades=existing_trades,
                    fee_xrp=open_order.get("fee_xrp", 0.0)
                )
                
                # Store filled order and delete open order
                self.db.store_filled_order(filled_order.model_dump())
                self.db.delete_open_order(open_order.get("hash"))
                
            else:  # partially_filled
                # Calculate cumulative filled amounts from all trades
                original_gets = XRPLAmount.from_xrpl_amount(open_order.get("taker_gets"))
                remaining_gets = XRPLAmount.from_xrpl_amount(filled_offer.get("TakerGets"))
                original_pays = XRPLAmount.from_xrpl_amount(open_order.get("taker_pays"))
                remaining_pays = XRPLAmount.from_xrpl_amount(filled_offer.get("TakerPays"))
                
                # Calculate filled amounts as the difference between original and remaining
                total_filled_gets = XRPLAmount(
                    currency=original_gets.currency,
                    issuer=original_gets.issuer,
                    value=str(float(original_gets.value) - float(remaining_gets.value))
                )
                total_filled_pays = XRPLAmount(
                    currency=original_pays.currency,
                    issuer=original_pays.issuer,
                    value=str(float(original_pays.value) - float(remaining_pays.value))
                )
                
                # Update open order with new amounts and add trade
                self.db.update_open_order(
                    prev_tx_id,
                    {
                        "status": OrderStatus.PARTIALLY_FILLED,
                        "last_checked_ledger": tx.get("ledger_index"),
                        "filled_gets": total_filled_gets.model_dump(),
                        "filled_pays": total_filled_pays.model_dump(),
                        "trades": existing_trades
                    }
                )

    async def _get_transaction_status(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of a transaction.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Optional[Dict[str, Any]]: Updated transaction data or None if not found
        """
        try:
            request = Tx(transaction=tx_hash)
            response = await self.client.request(request)
            
            if not response.is_successful():
                logger.error(f"Failed to get transaction status for {tx_hash}: {response.result}")
                return None
                
            return response.result
        except Exception as e:
            logger.error(f"Error getting transaction status for {tx_hash}: {e}")
            return None

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
