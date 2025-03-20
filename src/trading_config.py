"""
Configuration for supported tokens and trading pairs in the application.
Defines the whitelisted tokens and supported trading pairs for the XRPL Tag Streamer.
"""

from typing import Dict, List, Optional, Tuple, Literal
from pydantic import BaseModel


class Token(BaseModel):
    """Token structure for trading on XRPL."""
    currency: str  # Token currency identifier (hex code for non-XRP tokens)
    symbol: str    # Symbol for UI display


class TradingPair(BaseModel):
    """Trading pair structure for XRPL."""
    id: str        # Unique identifier for the pair (e.g., "XRP/RLUSD")
    base_token: Token  # The base token (e.g., XRP in XRP/RLUSD)
    quote_token: Token  # The quote token (e.g., RLUSD in XRP/RLUSD)


# Whitelisted tokens
TOKENS: Dict[str, Token] = {
    "XRP": Token(
        currency="XRP",  # XRP uses its symbol as currency
        symbol="XRP"
    ),
    "RLUSD": Token(
        currency="524C555344000000000000000000000000000000",  # Proper currency format
        symbol="RLUSD"
    ),
    "SOLO": Token(
        currency="534F4C4F00000000000000000000000000000000",
        symbol="SOLO"
    ),
    "CORE": Token(
        currency="434F524500000000000000000000000000000000",
        symbol="CORE"
    )
    # Add more tokens as needed
}


# Supported trading pairs
TRADING_PAIRS: List[TradingPair] = [
    TradingPair(
        id="XRP/RLUSD",
        base_token=TOKENS["XRP"],
        quote_token=TOKENS["RLUSD"]
    ),
    TradingPair(
        id="CORE/XRP",
        base_token=TOKENS["CORE"],
        quote_token=TOKENS["XRP"]
    ),
    TradingPair(
        id="SOLO/XRP",
        base_token=TOKENS["SOLO"],
        quote_token=TOKENS["XRP"]
    )
    # Add more trading pairs as needed
]


def is_whitelisted_token(currency: str, issuer: Optional[str] = None) -> bool:
    """
    Utility function to determine if a token is whitelisted.
    
    Args:
        currency: The currency code
        issuer: The issuer address (optional, only used for non-XRP tokens)
        
    Returns:
        bool: True if the token is whitelisted, False otherwise
    """
    # For XRP
    if currency == "XRP" and not issuer:
        return True
    
    # For other tokens - we only check the currency code
    # We don't validate the issuer here since different issuers might use the same currency code
    return any(token.currency == currency for token in TOKENS.values())


def is_supported_trading_pair(
    currency1: str, 
    issuer1: Optional[str], 
    currency2: str, 
    issuer2: Optional[str]
) -> bool:
    """
    Utility function to determine if a trading pair is supported.
    Checks both directions (e.g., XRP/RLUSD and RLUSD/XRP).
    
    Args:
        currency1: First currency code
        issuer1: First currency issuer (or None for XRP)
        currency2: Second currency code
        issuer2: Second currency issuer (or None for XRP)
        
    Returns:
        bool: True if the trading pair is supported, False otherwise
    """
    # Check if both tokens are whitelisted
    if not is_whitelisted_token(currency1, issuer1) or not is_whitelisted_token(currency2, issuer2):
        return False
    
    # Find tokens by currency
    token1 = next((t for t in TOKENS.values() if t.currency == currency1), None)
    token2 = next((t for t in TOKENS.values() if t.currency == currency2), None)
    
    if not token1 or not token2:
        print(f"Trading pair not supported: token(s) not found for currencies: {currency1}, {currency2}")
        return False
    
    # Check if this combination is a supported trading pair
    is_pair_supported = any(
        (pair.base_token.symbol == token1.symbol and pair.quote_token.symbol == token2.symbol) or
        (pair.base_token.symbol == token2.symbol and pair.quote_token.symbol == token1.symbol)
        for pair in TRADING_PAIRS
    )
    
    if not is_pair_supported:
        print(f"Trading pair not supported: {token1.symbol}/{token2.symbol} is not in whitelist")
    
    return is_pair_supported


def determine_market_side(
    pair_id: str,
    taker_gets_currency: str,
    taker_pays_currency: str
) -> Literal["BUY", "SELL", "UNKNOWN"]:
    """
    Determine market side (BUY or SELL) for a standard market notation.
    For example, in XRP/RLUSD:
    - If taker_pays is XRP and taker_gets is RLUSD, it's a SELL order (selling XRP for RLUSD)
    - If taker_pays is RLUSD and taker_gets is XRP, it's a BUY order (buying XRP with RLUSD)
    
    Args:
        pair_id: The trading pair ID (e.g., "XRP/RLUSD")
        taker_gets_currency: Currency the taker receives
        taker_pays_currency: Currency the taker pays
        
    Returns:
        Literal["BUY", "SELL", "UNKNOWN"]: The market side
    """
    pair = next((p for p in TRADING_PAIRS if p.id == pair_id), None)
    if not pair:
        return "UNKNOWN"
    
    # Find tokens by currency
    gets_token = next((t for t in TOKENS.values() if t.currency == taker_gets_currency), None)
    pays_token = next((t for t in TOKENS.values() if t.currency == taker_pays_currency), None)
    
    if not gets_token or not pays_token:
        return "UNKNOWN"
    
    base_symbol = pair.base_token.symbol
    quote_symbol = pair.quote_token.symbol
    
    if gets_token.symbol == quote_symbol and pays_token.symbol == base_symbol:
        return "BUY"  # Buying base with quote
    elif gets_token.symbol == base_symbol and pays_token.symbol == quote_symbol:
        return "SELL"  # Selling base for quote
    
    return "UNKNOWN"


def find_trading_pair(
    currency1: str, 
    issuer1: Optional[str], 
    currency2: str, 
    issuer2: Optional[str]
) -> Optional[TradingPair]:
    """
    Find the matching trading pair for two tokens.
    
    Args:
        currency1: First currency code
        issuer1: First currency issuer (or None for XRP)
        currency2: Second currency code
        issuer2: Second currency issuer (or None for XRP)
        
    Returns:
        Optional[TradingPair]: The matching trading pair or None if not found
    """
    # Find tokens by currency
    token1 = next((t for t in TOKENS.values() if t.currency == currency1), None)
    token2 = next((t for t in TOKENS.values() if t.currency == currency2), None)
    
    if not token1 or not token2:
        return None
    
    return next((
        pair for pair in TRADING_PAIRS if 
        (pair.base_token.symbol == token1.symbol and pair.quote_token.symbol == token2.symbol) or
        (pair.base_token.symbol == token2.symbol and pair.quote_token.symbol == token1.symbol)
    ), None) 