"""
XRPL Transaction Streamer module.
Connects to XRPL WebSocket API and processes transactions in real-time.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import xrpl
from xrpl.asyncio.clients import AsyncWebsocketClient
from xrpl.models import Subscribe, StreamParameter, Request
from xrpl.models.requests.request import RequestMethod

from src.config import TARGET_TAG, XRPL_WS_URL
from src.db.database import XRPLDatabase
from src.utils.transaction_utils import has_target_tag, format_transaction_for_display

logger = logging.getLogger(__name__)

class XRPLStreamer:
    """
    XRPL Transaction Streamer that connects to WebSocket API
    and processes transactions in real-time.
    """
    
    def __init__(
        self,
        websocket_url: str = XRPL_WS_URL,
        target_tag: str = TARGET_TAG,
        db: Optional[XRPLDatabase] = None,
    ):
        """
        Initialize the XRPL streamer.
        
        Args:
            websocket_url: The WebSocket URL to connect to
            target_tag: The tag to filter transactions for
            db: Optional database instance, will create one if not provided
        """
        self.websocket_url = websocket_url
        self.target_tag = target_tag
        self.db = db or XRPLDatabase()
        self.client = None
        self.running = False
        self.stats = {
            "total_ledgers": 0,
            "total_transactions": 0,
            "matching_transactions": 0,
            "start_time": None,
            "last_ledger": None,
        }
    
    async def start(self) -> None:
        """Start the transaction streamer."""
        if self.running:
            logger.warning("Streamer is already running")
            return
        
        self.running = True
        self.stats["start_time"] = datetime.now()
        logger.info(f"Starting XRPL streamer, connecting to {self.websocket_url}")
        logger.info(f"Filtering for transactions with tag: {self.target_tag}")
        
        try:
            async with AsyncWebsocketClient(self.websocket_url) as self.client:
                logger.info("Connected to XRPL WebSocket API")
                
                # Subscribe to ledger stream
                subscribe_request = Subscribe(streams=[StreamParameter.TRANSACTIONS])
                await self.client.send(subscribe_request)
                logger.info("Subscribed to ledger stream")
                
                # Process incoming messages
                async for message in self.client:
                    if not self.running:
                        break
                    
                    await self._process_message(message)
        except Exception as e:
            logger.error(f"Error in streamer: {e}")
            self.running = False
            raise
    
    async def stop(self) -> None:
        """Stop the transaction streamer."""
        logger.info("Stopping XRPL streamer")
        self.running = False
        
        # Close database connection
        if self.db:
            self.db.close()
        
        # Print statistics
        self._print_stats()
    
    async def _process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a WebSocket message from XRPL.
        
        Args:
            message: The WebSocket message
        """
        # Handle ledger_closed messages
        if isinstance(message, dict) and message.get("type") == "ledgerClosed":
            await self._process_ledger_closed(message)
    
    async def _process_ledger_closed(self, ledger_data: Dict[str, Any]) -> None:
        """
        Process a ledger_closed message.
        
        Args:
            ledger_data: The ledger data
        """
        ledger_index = ledger_data.get("ledger_index")
        ledger_hash = ledger_data.get("ledger_hash")
        
        logger.info(f"Processing ledger #{ledger_index} (hash: {ledger_hash})")
        
        # Store ledger in database
        self.db.store_ledger(ledger_data)
        
        # Fetch and process transactions in this ledger
        await self._process_ledger_transactions(ledger_index)
        
        # Update statistics
        self.stats["total_ledgers"] += 1
        self.stats["last_ledger"] = ledger_index
        
        # Print periodic stats
        if self.stats["total_ledgers"] % 10 == 0:
            self._print_stats()
    
    async def _process_ledger_transactions(self, ledger_index: int) -> None:
        """
        Fetch and process all transactions in a ledger.
        
        Args:
            ledger_index: The ledger index
        """
        try:
            # Request transactions in the ledger
            tx_request = {
                "command": "ledger",
                "id": ledger_index,
                "transactions": True,
                "expand": True
            }
            tx_request = Request(id=ledger_index, method=RequestMethod.TX)
            response = await self.client.request(tx_request)
            
            if not response.is_successful():
                logger.error(f"Failed to fetch transactions for ledger {ledger_index}: {response.result}")
                return
            
            # Extract and process transactions
            ledger = response.result.get("ledger", {})
            transactions = ledger.get("transactions", [])
            
            if not transactions:
                logger.debug(f"No transactions in ledger {ledger_index}")
                return
            
            logger.info(f"Processing {len(transactions)} transactions in ledger {ledger_index}")
            
            for tx in transactions:
                # Ensure it's a complete transaction object with metadata
                if not isinstance(tx, dict):
                    logger.debug(f"Skipping non-dict transaction: {type(tx)}")
                    continue
                
                # Some nodes return transactions with meta, others with metaData
                if "meta" not in tx and "metaData" not in tx:
                    logger.debug(f"Skipping transaction without metadata: {tx.get('hash', 'unknown')}")
                    continue
                
                # Add ledger index to transaction for reference
                tx["ledger_index"] = ledger_index
                
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
            logger.error(f"Error processing transactions for ledger {ledger_index}: {e}")
    
    def _print_stats(self) -> None:
        """Print streaming statistics."""
        if not self.stats["start_time"]:
            return
        
        runtime = datetime.now() - self.stats["start_time"]
        hours, remainder = divmod(runtime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        logger.info("------- XRPL Streamer Statistics -------")
        logger.info(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        logger.info(f"Ledgers processed: {self.stats['total_ledgers']}")
        logger.info(f"Transactions processed: {self.stats['total_transactions']}")
        logger.info(f"Matching transactions: {self.stats['matching_transactions']}")
        logger.info(f"Current ledger: {self.stats['last_ledger']}")
        logger.info("---------------------------------------")

async def run_streamer():
    """Run the XRPL streamer indefinitely."""
    streamer = XRPLStreamer()
    try:
        await streamer.start()
    except KeyboardInterrupt:
        logger.info("Streamer interrupted by user")
    except Exception as e:
        logger.exception(f"Error in streamer: {e}")
    finally:
        await streamer.stop()

def run_streamer_sync():
    """Synchronous wrapper to run the streamer."""
    try:
        asyncio.run(run_streamer())
    except KeyboardInterrupt:
        print("Streamer interrupted by user")
    except Exception as e:
        print(f"Error in streamer: {e}")

if __name__ == "__main__":
    from src.utils.logger import setup_logging
    logger = setup_logging()
    run_streamer_sync() 