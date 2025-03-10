"""
Historical XRPL transaction processor module.
Fetches and processes past ledgers and transactions.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

from xrpl.asyncio.clients import AsyncJsonRpcClient
from xrpl.models.requests import Ledger

from src.config import TARGET_TAG, XRPL_RPC_URL, HISTORY_BACKFILL_DAYS
from src.db.database import XRPLDatabase
from src.utils.transaction_utils import has_target_tag, format_transaction_for_display, enrich_transaction_metadata

logger = logging.getLogger(__name__)

class HistoricalProcessor:
    """
    Processor for historical XRPL ledgers and transactions.
    """
    
    def __init__(
        self,
        rpc_url: str = XRPL_RPC_URL,
        target_tag: str = TARGET_TAG,
        db: Optional[XRPLDatabase] = None,
    ):
        """
        Initialize the historical processor.
        
        Args:
            rpc_url: The XRPL JSON-RPC URL to connect to
            target_tag: The tag to filter transactions for
            db: Optional database instance, will create one if not provided
        """
        self.rpc_url = rpc_url
        self.target_tag = target_tag
        self.db = db or XRPLDatabase()
        self.client = None
        self.stats = {
            "total_ledgers": 0,
            "total_transactions": 0,
            "matching_transactions": 0,
            "start_time": None,
        }
    
    async def process_date_range(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        days: int = HISTORY_BACKFILL_DAYS,
    ) -> None:
        """
        Process ledgers within a date range.
        
        Args:
            start_date: The start date, defaults to `days` ago
            end_date: The end date, defaults to now
            days: Number of days to look back if start_date not specified
        """
        if days <= 0 and not start_date:
            logger.info("Historical processing disabled")
            return
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=days))
        
        logger.info(f"Processing historical data from {start_date} to {end_date}")
        
        # Convert dates to XRPL time (seconds since Jan 1, 2000 00:00:00 UTC)
        # XRPL epoch is 946684800 seconds after Unix epoch
        xrpl_epoch = 946684800
        start_time = int(start_date.timestamp() - xrpl_epoch)
        end_time = int(end_date.timestamp() - xrpl_epoch)
        
        # Get ledger indexes for the time range
        start_ledger, end_ledger = await self._get_ledger_range_for_time(start_time, end_time)
        
        if start_ledger is None or end_ledger is None:
            logger.error("Failed to determine ledger range for the specified time period")
            return
        
        await self.process_ledger_range(start_ledger, end_ledger)
    
    async def process_ledger_range(
        self,
        start_ledger: int,
        end_ledger: int,
        batch_size: int = 100,
    ) -> None:
        """
        Process a range of ledgers.
        
        Args:
            start_ledger: The starting ledger index
            end_ledger: The ending ledger index
            batch_size: Number of ledgers to process in each batch
        """
        self.stats["start_time"] = datetime.now()
        logger.info(f"Processing ledgers from {start_ledger} to {end_ledger}")
        
        # Connect to XRPL
        self.client = AsyncJsonRpcClient(self.rpc_url)
        
        # Process in batches to avoid memory issues
        for batch_start in range(start_ledger, end_ledger + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_ledger)
            logger.info(f"Processing ledger batch {batch_start} to {batch_end}")
            
            # Process each ledger in the batch
            for ledger_index in range(batch_start, batch_end + 1):
                # Skip if already processed
                if self.db.is_ledger_exists(ledger_index):
                    logger.debug(f"Skipping already processed ledger {ledger_index}")
                    continue
                
                await self._process_ledger(ledger_index)
                
                # Update statistics
                self.stats["total_ledgers"] += 1
                
                # Print periodic stats
                if self.stats["total_ledgers"] % 20 == 0:
                    self._print_stats()
        
        # Final stats
        self._print_stats()
        
        # Close client
        if self.client:
            await self.client.close()
    
    async def _get_ledger_range_for_time(
        self,
        start_time: int,
        end_time: int,
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Get the ledger indexes that correspond to a time range.
        
        Args:
            start_time: The start time in XRPL epoch seconds
            end_time: The end time in XRPL epoch seconds
            
        Returns:
            Tuple of (start_ledger_index, end_ledger_index)
        """
        try:
            # Connect to XRPL
            client = AsyncJsonRpcClient(self.rpc_url)
            
            # Get the current ledger to use as end if needed
            current_ledger_resp = await client.request(Ledger(ledger_index="validated"))
            
            if not current_ledger_resp.is_successful():
                logger.error(f"Failed to get current ledger: {current_ledger_resp.result}")
                return None, None
            
            current_ledger_index = current_ledger_resp.result["ledger"]["ledger_index"]
            current_ledger_time = current_ledger_resp.result["ledger"]["close_time"]
            
            # If end_time is in the future, use current ledger
            if end_time > current_ledger_time:
                end_ledger = current_ledger_index
            else:
                # Find the ledger closest to end_time
                end_ledger = await self._find_ledger_by_time(end_time, client)
            
            # Find the ledger closest to start_time
            start_ledger = await self._find_ledger_by_time(start_time, client)
            
            await client.close()
            
            # If we couldn't find a start ledger, default to 20,000 ledgers back (~1 day)
            if start_ledger is None:
                start_ledger = max(1, end_ledger - 20000)
            
            return start_ledger, end_ledger
            
        except Exception as e:
            logger.error(f"Error getting ledger range for time period: {e}")
            return None, None
    
    async def _find_ledger_by_time(
        self,
        target_time: int,
        client: AsyncJsonRpcClient,
    ) -> Optional[int]:
        """
        Find the ledger index closest to a specific time using binary search.
        
        Args:
            target_time: The target time in XRPL epoch seconds
            client: The XRPL client
            
        Returns:
            The closest ledger index, or None if not found
        """
        try:
            # Use binary search to find the ledger
            # Start with a wide range
            low = 32570  # First ledger with usable close_time
            
            # Get current validated ledger as upper bound
            high_resp = await client.request(Ledger(ledger_index="validated"))
            if not high_resp.is_successful():
                return None
            
            high = high_resp.result["ledger"]["ledger_index"]
            
            # Binary search with maximum 20 iterations
            for _ in range(20):
                if high < low:
                    break
                
                mid = (low + high) // 2
                
                # Get the ledger at mid point
                mid_resp = await client.request(Ledger(ledger_index=mid))
                if not mid_resp.is_successful():
                    # Try with a different mid point
                    mid = mid + 100
                    continue
                
                mid_time = mid_resp.result["ledger"]["close_time"]
                
                if mid_time < target_time:
                    low = mid + 1
                elif mid_time > target_time:
                    high = mid - 1
                else:
                    # Exact match
                    return mid
            
            # Return the closest ledger
            return low
            
        except Exception as e:
            logger.error(f"Error finding ledger by time: {e}")
            return None
    
    async def _process_ledger(self, ledger_index: int) -> None:
        """
        Process a single ledger.
        
        Args:
            ledger_index: The ledger index
        """
        try:
            # Request ledger with transactions
            request = Ledger(
                ledger_index=ledger_index,
                transactions=True,
                expand=True
            )
            
            response = await self.client.request(request)
            
            if not response.is_successful():
                logger.error(f"Failed to fetch ledger {ledger_index}: {response.result}")
                return
            
            # Extract ledger data
            ledger = response.result.get("ledger", {})
            ledger_data = {
                "ledger_index": ledger.get("ledger_index"),
                "ledger_hash": ledger.get("ledger_hash"),
                "ledger_time": ledger.get("close_time", 0),
                "close_time": ledger.get("close_time", 0),
                "txn_count": len(ledger.get("transactions", [])),
            }
            
            # Store ledger in database
            self.db.store_ledger(ledger_data)
            
            # Process transactions
            transactions = ledger.get("transactions", [])
            
            if not transactions:
                logger.debug(f"No transactions in ledger {ledger_index}")
                return
            
            logger.info(f"Processing {len(transactions)} transactions in ledger {ledger_index}")
            
            for tx in transactions:
                # Ensure it's a complete transaction object with metadata
                if not isinstance(tx, dict) or "meta" not in tx:
                    continue
                
                # Skip if already processed
                tx_hash = tx.get("hash")
                if tx_hash and self.db.is_transaction_exists(tx_hash):
                    continue
                
                # Add ledger index to transaction for reference
                tx["ledger_index"] = ledger_index
                
                # Standardize metadata field name if needed
                if "metaData" in tx and "meta" not in tx:
                    tx["meta"] = tx["metaData"]
                
                # Enrich transaction with metadata analysis
                enriched_tx = enrich_transaction_metadata(tx)
                tx.update(enriched_tx)
                
                # Log if this is an offer and whether it was filled
                if tx.get("TransactionType") == "OfferCreate":
                    filled_status = "FILLED" if enriched_tx.get("offer_filled") else "NOT FILLED"
                    logger.info(f"OfferCreate transaction {tx.get('hash')} was {filled_status}")
                
                # Check if this transaction has our target tag
                tag_matched = has_target_tag(tx, self.target_tag)
                
                # Store transaction in database
                self.db.store_transaction(tx, tag_matched)
                
                # Update statistics
                self.stats["total_transactions"] += 1
                if tag_matched:
                    self.stats["matching_transactions"] += 1
                    logger.info(f"Found matching transaction with hash: {tx.get('hash')}")
                    logger.debug(f"Transaction details: {format_transaction_for_display(tx)}")
            
            logger.info(f"Processed {len(transactions)} transactions in ledger {ledger_index}")
            
        except Exception as e:
            logger.error(f"Error processing ledger {ledger_index}: {e}")
    
    def _print_stats(self) -> None:
        """Print processing statistics."""
        if not self.stats["start_time"]:
            return
        
        runtime = datetime.now() - self.stats["start_time"]
        hours, remainder = divmod(runtime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logger.info("------- Historical Processing Statistics -------")
        logger.info(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        logger.info(f"Ledgers processed: {self.stats['total_ledgers']}")
        logger.info(f"Transactions processed: {self.stats['total_transactions']}")
        logger.info(f"Matching transactions: {self.stats['matching_transactions']}")
        logger.info("-----------------------------------------------")

async def process_history(days: int = HISTORY_BACKFILL_DAYS):
    """Process historical data for a specific number of days."""
    processor = HistoricalProcessor()
    try:
        await processor.process_date_range(days=days)
    except Exception as e:
        logger.exception(f"Error in historical processor: {e}")

async def process_ledger_range(start_ledger: int, end_ledger: int):
    """Process a specific ledger range."""
    processor = HistoricalProcessor()
    try:
        await processor.process_ledger_range(start_ledger, end_ledger)
    except Exception as e:
        logger.exception(f"Error in historical processor: {e}")

if __name__ == "__main__":
    from src.utils.logger import setup_logging
    logger = setup_logging()
    asyncio.run(process_history()) 