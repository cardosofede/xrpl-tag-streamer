"""
Utility functions for processing XRPL transactions.
"""

import base64
import binascii
import logging
from typing import Any, Dict, List, Optional, Union

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