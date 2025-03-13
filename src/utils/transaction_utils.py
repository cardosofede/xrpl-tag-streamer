"""
Utility functions for processing XRPL transactions.
"""

import binascii
import logging
from typing import Any, Dict, Optional

from xrpl.utils import (
    drops_to_xrp,
    get_balance_changes,
    get_order_book_changes,
)

from src.config import SOURCE_TAG

logger = logging.getLogger(__name__)

def has_target_tag(transaction: Dict[str, Any], target_tag: Optional[str] = None) -> bool:
    """
    Check if a transaction has the target tag, either in source/destination tag or memo.
    
    Args:
        transaction: The transaction data
        target_tag: The tag to look for, defaults to SOURCE_TAG from config
        
    Returns:
        bool: True if the transaction has the target tag, False otherwise
    """
    # Convert SOURCE_TAG to string for comparison
    tag = target_tag or str(SOURCE_TAG)
    
    # Check source tag
    if "SourceTag" in transaction and str(transaction["SourceTag"]) == tag:
        logger.debug(f"Found target tag in SourceTag: {tag}")
        return True
    
    # Check destination tag
    if "DestinationTag" in transaction and str(transaction["DestinationTag"]) == tag:
        logger.debug(f"Found target tag in DestinationTag: {tag}")
        return True
    
    # Check memo fields
    if "Memos" in transaction and transaction["Memos"]:
        for memo in transaction["Memos"]:
            if "Memo" in memo:
                memo_obj = memo["Memo"]
                
                # Check MemoData (hex encoded)
                if "MemoData" in memo_obj:
                    try:
                        # Try to decode as hex and then as UTF-8
                        memo_data = binascii.unhexlify(memo_obj["MemoData"]).decode("utf-8")
                        if tag in memo_data:
                            logger.debug(f"Found target tag in MemoData: {tag}")
                            return True
                    except Exception as e:
                        logger.debug(f"Failed to decode MemoData: {e}")
                
                # Check MemoType (hex encoded)
                if "MemoType" in memo_obj:
                    try:
                        memo_type = binascii.unhexlify(memo_obj["MemoType"]).decode("utf-8")
                        if tag in memo_type:
                            logger.debug(f"Found target tag in MemoType: {tag}")
                            return True
                    except Exception as e:
                        logger.debug(f"Failed to decode MemoType: {e}")
                
                # Check MemoFormat (hex encoded)
                if "MemoFormat" in memo_obj:
                    try:
                        memo_format = binascii.unhexlify(memo_obj["MemoFormat"]).decode("utf-8")
                        if tag in memo_format:
                            logger.debug(f"Found target tag in MemoFormat: {tag}")
                            return True
                    except Exception as e:
                        logger.debug(f"Failed to decode MemoFormat: {e}")
    
    return False

def format_transaction_for_display(tx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a transaction for display, converting technical fields to readable format.
    
    Args:
        tx: The transaction data
        
    Returns:
        Dict: A formatted transaction for display
    """
    result = {
        "hash": tx.get("hash", ""),
        "ledger_index": tx.get("ledger_index", 0),
        "transaction_type": tx.get("TransactionType", ""),
        "account": tx.get("Account", ""),
        "destination": tx.get("Destination", ""),
        "fee": int(tx.get("Fee", 0)) / 1_000_000,  # Convert to XRP
    }
    
    # Format amount
    amount = tx.get("Amount", "")
    if isinstance(amount, str):
        # Convert drops to XRP
        result["amount"] = float(amount) / 1_000_000
        result["currency"] = "XRP"
    elif isinstance(amount, dict):
        result["amount"] = float(amount.get("value", 0))
        result["currency"] = amount.get("currency", "")
    
    # Format tags
    if "SourceTag" in tx:
        result["source_tag"] = tx["SourceTag"]
    if "DestinationTag" in tx:
        result["destination_tag"] = tx["DestinationTag"]
    
    # Format memos
    if "Memos" in tx and tx["Memos"]:
        result["memos"] = []
        for memo in tx["Memos"]:
            if "Memo" in memo:
                memo_obj = memo["Memo"]
                memo_formatted = {}
                
                # Decode memo fields
                for field in ["MemoData", "MemoType", "MemoFormat"]:
                    if field in memo_obj:
                        try:
                            decoded = binascii.unhexlify(memo_obj[field]).decode("utf-8")
                            memo_formatted[field] = decoded
                        except Exception:
                            memo_formatted[field] = memo_obj[field]
                
                result["memos"].append(memo_formatted)
    
    return result

def is_offer_filled(tx: Dict[str, Any]) -> bool:
    """
    Determine if an OfferCreate transaction was filled (completely executed).
    
    Args:
        tx: Transaction data with metadata
    
    Returns:
        bool: True if the offer was completely filled, False otherwise
    """
    # Only process OfferCreate transactions
    if tx.get("TransactionType") != "OfferCreate":
        return False
    
    # Check if transaction was successful
    meta = tx.get("meta") or tx.get("metaData", {})
    if meta.get("TransactionResult") != "tesSUCCESS":
        return False
    
    # Look for deleted offer nodes in the affected nodes
    affected_nodes = meta.get("AffectedNodes", [])
    for node in affected_nodes:
        # Check for deleted offer that matches the account
        if "DeletedNode" in node:
            deleted_node = node["DeletedNode"]
            if deleted_node.get("LedgerEntryType") == "Offer":
                final_fields = deleted_node.get("FinalFields", {})
                if final_fields.get("Account") == tx.get("Account"):
                    # Offer was deleted, meaning it was fully filled
                    return True
    
    return False

def enrich_transaction_metadata(tx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich a transaction with additional metadata-derived information.
    
    Args:
        tx: Transaction data with metadata
        
    Returns:
        Dict: The transaction with additional fields
    """
    # Make a copy to avoid modifying the original
    enriched_tx = tx.copy()
    
    # Skip if no metadata
    meta = tx.get("meta") or tx.get("metaData")
    if not meta:
        return enriched_tx
    
    # Add transaction result
    enriched_tx["tx_result"] = meta.get("TransactionResult")
    
    # Process OfferCreate transactions
    if tx.get("TransactionType") == "OfferCreate":
        enriched_tx["offer_filled"] = is_offer_filled(tx)
        
        # Extract offer details
        if "TakerGets" in tx:
            taker_gets = tx["TakerGets"]
            if isinstance(taker_gets, str):
                # XRP amount in drops
                enriched_tx["base_currency"] = "XRP"
                enriched_tx["base_amount"] = drops_to_xrp(taker_gets)
            elif isinstance(taker_gets, dict):
                # IOU
                enriched_tx["base_currency"] = taker_gets.get("currency", "")
                enriched_tx["base_amount"] = float(taker_gets.get("value", 0))
                enriched_tx["base_issuer"] = taker_gets.get("issuer", "")
        
        if "TakerPays" in tx:
            taker_pays = tx["TakerPays"]
            if isinstance(taker_pays, str):
                # XRP amount in drops
                enriched_tx["quote_currency"] = "XRP"
                enriched_tx["quote_amount"] = drops_to_xrp(taker_pays)
            elif isinstance(taker_pays, dict):
                # IOU
                enriched_tx["quote_currency"] = taker_pays.get("currency", "")
                enriched_tx["quote_amount"] = float(taker_pays.get("value", 0))
                enriched_tx["quote_issuer"] = taker_pays.get("issuer", "")
        
        # For a filled offer, get the actual execution details
        if enriched_tx.get("offer_filled"):
            # Try to extract order book changes
            try:
                order_changes = get_order_book_changes(tx)
                if order_changes:
                    enriched_tx["order_book_changes"] = order_changes
            except Exception as e:
                logger.warning(f"Failed to get order book changes: {e}")
    
    # Get balance changes for all transaction types
    try:
        balance_changes = get_balance_changes(tx)
        if balance_changes:
            enriched_tx["balance_changes"] = balance_changes
    except Exception as e:
        logger.warning(f"Failed to get balance changes: {e}")
    
    return enriched_tx 