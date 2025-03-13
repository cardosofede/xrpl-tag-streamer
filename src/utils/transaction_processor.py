"""
Transaction processing utilities for XRPL transactions.
These functions handle analyzing transaction data and extracting relevant information.
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union

from xrpl.utils import get_balance_changes
from xrpl.models.response import ResponseStatus

from src.data_types import (
    XRPLAmount, 
    Trade, 
    TransactionType,
    OrderStatus
)


def has_source_tag(tx: Dict[str, Any], target_tag: str) -> bool:
    """
    Check if a transaction has the specified source tag.
    
    Args:
        tx: Transaction data
        target_tag: Source tag to check for
        
    Returns:
        bool: True if transaction has the specified source tag
    """
    source_tag = tx.get("SourceTag") or tx.get("TagSource")
    if source_tag is not None:
        return str(source_tag) == target_tag
    return False


def is_deposit_or_withdrawal(tx: Dict[str, Any], user_wallets: List[str]) -> Optional[str]:
    """
    Check if a transaction is a deposit or withdrawal.
    
    Args:
        tx: Transaction data
        user_wallets: List of user wallet addresses
        
    Returns:
        str: 'deposit', 'withdrawal', or None if neither
    """
    if tx.get("TransactionType") != "Payment":
        return None
        
    account = tx.get("Account")
    destination = tx.get("Destination")
    
    if account in user_wallets and destination not in user_wallets:
        return "withdrawal"
    elif account not in user_wallets and destination in user_wallets:
        return "deposit"
    
    return None


def is_offer_filled(tx: Dict[str, Any]) -> bool:
    """
    Check if an OfferCreate transaction was immediately filled (partially or completely).
    
    Args:
        tx: Transaction data
        
    Returns:
        bool: True if offer was filled
    """
    if tx.get("TransactionType") != "OfferCreate":
        return False
        
    meta = tx.get("meta") or tx.get("metaData", {})
    if isinstance(meta, str):
        return False  # Can't process string metadata
        
    # Check the transaction result
    result = meta.get("TransactionResult")
    if result != "tesSUCCESS":
        return False
    
    # Get balance changes to determine if the offer was filled
    try:
        balance_changes = get_balance_changes(meta)
        
        # If there are balance changes for the account, the offer was at least partially filled
        account = tx.get("Account")
        for acct, changes in balance_changes.items():
            if acct == account and len(changes) > 0:
                return True
        
        # If there's no balance change, check if an offer was created
        affected_nodes = meta.get("AffectedNodes", [])
        for node in affected_nodes:
            if "CreatedNode" in node:
                created_node = node["CreatedNode"]
                if created_node.get("LedgerEntryType") == "Offer":
                    # An offer was created but not filled
                    return False
        
        # If we get here, the offer was either filled immediately or failed
        return True
    except Exception as e:
        # Fall back to the previous implementation if there's an issue with get_balance_changes
        # Check if the offer is in the affected nodes
        affected_nodes = meta.get("AffectedNodes", [])
        
        # Check if the transaction created an offer in the ledger
        offer_created = False
        for node in affected_nodes:
            if "CreatedNode" in node:
                created_node = node["CreatedNode"]
                if created_node.get("LedgerEntryType") == "Offer":
                    offer_created = True
                    break
        
        # If no offer was created, then it was filled immediately
        return not offer_created


def is_market_trade(tx: Dict[str, Any]) -> bool:
    """
    Check if a payment is actually a market trade (payment that consumed offers).
    
    Args:
        tx: Transaction data
        
    Returns:
        bool: True if payment is a market trade
    """
    if tx.get("TransactionType") != "Payment":
        return False
        
    meta = tx.get("meta") or tx.get("metaData", {})
    if isinstance(meta, str):
        return False  # Can't process string metadata
        
    # Check the transaction result
    result = meta.get("TransactionResult")
    if result != "tesSUCCESS":
        return False
    
    # Check if the payment has multiple currency balance changes
    # which would indicate a market trade (cross-currency payment)
    try:
        balance_changes = get_balance_changes(meta)
        
        # For the sender account, check if there are multiple currency changes
        account = tx.get("Account")
        if account in balance_changes:
            currencies = set()
            for change in balance_changes[account]:
                currencies.add(change["currency"])
            
            # If the sender has balance changes in multiple currencies, it's likely a market trade
            if len(currencies) > 1:
                return True
                
        # Also check if any offer nodes were affected
        affected_nodes = meta.get("AffectedNodes", [])
        for node in affected_nodes:
            if "DeletedNode" in node or "ModifiedNode" in node:
                node_data = node.get("DeletedNode") or node.get("ModifiedNode", {})
                if node_data.get("LedgerEntryType") == "Offer":
                    return True
                    
        return False
    except Exception:
        # Fall back to checking affected nodes directly
        affected_nodes = meta.get("AffectedNodes", [])
        for node in affected_nodes:
            if "DeletedNode" in node or "ModifiedNode" in node:
                node_data = node.get("DeletedNode") or node.get("ModifiedNode", {})
                if node_data.get("LedgerEntryType") == "Offer":
                    return True
        
        return False


def extract_trades_from_metadata(tx: Dict[str, Any]) -> List[Trade]:
    """
    Extract trade details from transaction metadata using balance changes.
    
    Args:
        tx: Transaction data
        
    Returns:
        List[Trade]: List of trade objects
    """
    trades = []
    meta = tx.get("meta") or tx.get("metaData", {})
    
    if not meta or isinstance(meta, str):
        return trades
    
    tx_hash = tx.get("hash")
    ledger_index = tx.get("ledger_index")
    timestamp = datetime.fromtimestamp(tx.get("date", 0))
    taker_address = tx.get("Account")
    
    try:
        # Get balance changes for all affected accounts
        balance_changes = get_balance_changes(meta)
        
        # First, identify the taker's changes (what they sold and bought)
        taker_sold = None
        taker_bought = None
        
        if taker_address in balance_changes:
            for change in balance_changes[taker_address]:
                # Negative changes are assets sold
                if float(change["value"]) < 0:
                    taker_sold = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=str(abs(float(change["value"])))
                    )
                # Positive changes are assets bought
                elif float(change["value"]) > 0:
                    taker_bought = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=change["value"]
                    )
        
        # Now look for matching changes in other accounts (the makers)
        for maker_address, changes in balance_changes.items():
            # Skip the taker's own account
            if maker_address == taker_address:
                continue
                
            # Look for the inverse changes in the maker account
            maker_sold = None
            maker_bought = None
            
            for change in changes:
                # Positive changes are assets the maker bought
                if float(change["value"]) > 0:
                    maker_bought = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=change["value"]
                    )
                # Negative changes are assets the maker sold
                elif float(change["value"]) < 0:
                    maker_sold = XRPLAmount(
                        currency=change["currency"],
                        issuer=change.get("issuer"),
                        value=str(abs(float(change["value"])))
                    )
            
            # If we have both what the maker sold and bought, create a trade
            if maker_sold and maker_bought:
                trades.append(Trade(
                    tx_hash=tx_hash,
                    ledger_index=ledger_index,
                    timestamp=timestamp,
                    taker_address=taker_address,
                    maker_address=maker_address,
                    sold_amount=maker_sold,  # What the maker sold
                    bought_amount=maker_bought  # What the maker bought
                ))
        
        # If no trades were found with the above method but we know a trade occurred,
        # try to extract from affected nodes as a fallback
        if not trades and (is_offer_filled(tx) or is_market_trade(tx)):
            return _extract_trades_from_affected_nodes(tx)
            
        return trades
    except Exception as e:
        # Fall back to the previous implementation
        return _extract_trades_from_affected_nodes(tx)


def _extract_trades_from_affected_nodes(tx: Dict[str, Any]) -> List[Trade]:
    """
    Fallback method to extract trades from affected nodes when balance changes fails.
    
    Args:
        tx: Transaction data
        
    Returns:
        List[Trade]: List of trade objects
    """
    trades = []
    meta = tx.get("meta") or tx.get("metaData", {})
    
    if not meta or isinstance(meta, str):
        return trades
        
    affected_nodes = meta.get("AffectedNodes", [])
    tx_hash = tx.get("hash")
    ledger_index = tx.get("ledger_index")
    timestamp = datetime.fromtimestamp(tx.get("date", 0))
    taker_address = tx.get("Account")
    
    for node in affected_nodes:
        if "DeletedNode" in node or "ModifiedNode" in node:
            node_data = node.get("DeletedNode") or node.get("ModifiedNode", {})
            
            if node_data.get("LedgerEntryType") == "Offer":
                # Extract offer details
                maker_address = None
                sold_amount = None
                bought_amount = None
                
                if "DeletedNode" in node:
                    # Offer was fully consumed
                    final_fields = node_data.get("FinalFields", {})
                    maker_address = final_fields.get("Account")
                    
                    # Extract what was sold and bought
                    if "TakerGets" in final_fields and "TakerPays" in final_fields:
                        sold_amount = XRPLAmount.from_xrpl_amount(final_fields["TakerGets"])
                        bought_amount = XRPLAmount.from_xrpl_amount(final_fields["TakerPays"])
                        
                elif "ModifiedNode" in node:
                    # Offer was partially consumed
                    final_fields = node_data.get("FinalFields", {})
                    previous_fields = node_data.get("PreviousFields", {})
                    maker_address = final_fields.get("Account")
                    
                    # Calculate what was sold and bought (the difference)
                    if "TakerGets" in previous_fields and "TakerGets" in final_fields:
                        sold_amount = calculate_amount_difference(
                            previous_fields["TakerGets"],
                            final_fields["TakerGets"]
                        )
                    
                    if "TakerPays" in previous_fields and "TakerPays" in final_fields:
                        bought_amount = calculate_amount_difference(
                            previous_fields["TakerPays"],
                            final_fields["TakerPays"]
                        )
                
                # Create trade record if we have all needed data
                if maker_address and sold_amount and bought_amount:
                    trades.append(Trade(
                        tx_hash=tx_hash,
                        ledger_index=ledger_index,
                        timestamp=timestamp,
                        taker_address=taker_address,
                        maker_address=maker_address,
                        sold_amount=sold_amount,
                        bought_amount=bought_amount
                    ))
    
    return trades


def calculate_amount_difference(
    previous: Union[str, Dict[str, Any]], 
    current: Union[str, Dict[str, Any]]
) -> XRPLAmount:
    """
    Calculate the difference between two XRPL amounts.
    
    Args:
        previous: Previous amount
        current: Current amount
        
    Returns:
        XRPLAmount: The difference as an XRPLAmount
    """
    if isinstance(previous, str) and isinstance(current, str):
        # XRP amount
        return XRPLAmount(
            currency="XRP",
            value=str(abs(int(current) - int(previous)) / 1000000)  # Convert drops to XRP
        )
    elif isinstance(previous, dict) and isinstance(current, dict):
        # Token amount
        return XRPLAmount(
            currency=current.get("currency", ""),
            issuer=current.get("issuer"),
            value=str(abs(float(current.get("value", 0)) - float(previous.get("value", 0))))
        )
    else:
        # Different types, unable to calculate difference
        return XRPLAmount(currency="UNKNOWN", value="0")


def extract_amount(tx: Dict[str, Any]) -> XRPLAmount:
    """
    Extract amount from a transaction.
    
    Args:
        tx: Transaction data
        
    Returns:
        XRPLAmount: The transaction amount
    """
    amount = tx.get("Amount")
    return XRPLAmount.from_xrpl_amount(amount) if amount else XRPLAmount(currency="UNKNOWN", value="0")


def extract_transaction_balance_changes(tx: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract all balance changes from a transaction using the XRPL utility function.
    
    Args:
        tx: Transaction data
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Dictionary of account address to list of balance changes
    """
    meta = tx.get("meta") or tx.get("metaData", {})
    
    if not meta or isinstance(meta, str):
        return {}
    
    try:
        return get_balance_changes(meta)
    except Exception as e:
        return {}


def analyze_transaction(tx: Dict[str, Any], user_wallets: List[str]) -> Dict[str, Any]:
    """
    Analyze a transaction and add additional metadata about its type and effects.
    
    Args:
        tx: Raw transaction data
        user_wallets: List of user wallet addresses
        
    Returns:
        Dict[str, Any]: Transaction with additional analysis metadata
    """
    tx_type = tx.get("TransactionType")
    enriched_tx = tx.copy()
    
    # Extract balance changes for all transaction types
    balance_changes = extract_transaction_balance_changes(tx)
    if balance_changes:
        enriched_tx["balance_changes"] = balance_changes
    
    if tx_type == "OfferCreate":
        enriched_tx["offer_filled"] = is_offer_filled(tx)
        if enriched_tx["offer_filled"]:
            enriched_tx["trades"] = extract_trades_from_metadata(tx)
    
    elif tx_type == "Payment":
        tx_nature = is_deposit_or_withdrawal(tx, user_wallets)
        if tx_nature:
            enriched_tx["transaction_nature"] = tx_nature
        elif is_market_trade(tx):
            enriched_tx["transaction_nature"] = "market_trade"
            enriched_tx["trades"] = extract_trades_from_metadata(tx)
    
    elif tx_type == "OfferCancel":
        # Find which offer is being canceled
        offer_sequence = tx.get("OfferSequence")
        if offer_sequence:
            enriched_tx["canceled_offer_sequence"] = offer_sequence
    
    return enriched_tx 