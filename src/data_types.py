"""
Data models for XRPL transaction processing.
These models define the structure of data used throughout the application.
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union, Any
from decimal import Decimal

from pydantic import BaseModel, Field, computed_field

from src.trading_config import find_trading_pair, TradingPair


class TransactionType(str, Enum):
    """Transaction types on the XRPL."""
    PAYMENT = "Payment"
    OFFER_CREATE = "OfferCreate"
    OFFER_CANCEL = "OfferCancel"


class OrderStatus(str, Enum):
    """Status of an order on the XRPL."""
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELED = "canceled"


class XRPLAmount(BaseModel):
    """
    Represents an amount in XRPL, which can be either:
    - XRP: a string representing drops of XRP
    - Token: a dictionary with currency, issuer, and value
    """
    currency: str
    issuer: Optional[str] = None
    value: str

    @classmethod
    def from_xrpl_amount(cls, amount: Union[str, Dict[str, Any]]) -> "XRPLAmount":
        """Create an XRPLAmount from an XRPL amount representation."""
        if isinstance(amount, str):
            # XRP amount in drops
            return cls(
                currency="XRP",
                value=str(int(amount) / 1000000)  # Convert drops to XRP
            )
        else:
            # Token amount
            return cls(
                currency=amount.get("currency", ""),
                issuer=amount.get("issuer"),
                value=amount.get("value", "0")
            )


class Trade(BaseModel):
    """Represents a trade that filled an order."""
    hash: str
    ledger_index: int
    timestamp: datetime
    taker_address: str  # The address that initiated the trade
    maker_address: str  # Our address (the one that had the offer)
    sold_amount: XRPLAmount  # What was sold by the maker (us)
    bought_amount: XRPLAmount  # What was bought by the maker (us)
    related_offer_sequence: Optional[int] = None  # The sequence number of the offer that was filled
    related_offer_hash: Optional[str] = None  # The hash of the offer that was filled
    user_id: Optional[str] = None # The user ID that owns the maker address
    fee_xrp: float = 0.0  # Transaction fee in XRP


class Transaction(BaseModel):
    """Base transaction model for XRPL transactions."""
    hash: str
    ledger_index: int
    transaction_type: TransactionType
    account: str
    destination: Optional[str] = None
    timestamp: datetime
    raw_tx: Dict[str, Any]
    user_id: str
    fee_xrp: float = 0.0  # Transaction fee in XRP


class MarketSide(str, Enum):
    """Market side (buy or sell) for an order."""
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class OpenOrder(BaseModel):
    """Represents an open order on the XRPL."""
    hash: str
    account: str
    sequence: int
    created_ledger_index: int
    last_checked_ledger: int
    taker_gets: XRPLAmount
    taker_pays: XRPLAmount
    status: OrderStatus = OrderStatus.OPEN
    user_id: str
    transaction_type: TransactionType = TransactionType.OFFER_CREATE
    created_date: datetime
    fee_xrp: float = 0.0  # Transaction fee in XRP
    
    @computed_field
    @property
    def trading_pair(self) -> Optional[TradingPair]:
        """Determine the trading pair for this order."""
        return find_trading_pair(
            self.taker_gets.currency, 
            self.taker_gets.issuer, 
            self.taker_pays.currency, 
            self.taker_pays.issuer
        )
    
    @computed_field
    @property
    def market_side(self) -> MarketSide:
        """Determine if this is a buy or sell order."""
        if not self.trading_pair:
            return MarketSide.UNKNOWN
            
        pair_id = self.trading_pair.id
        base_symbol = self.trading_pair.base_token.symbol
        quote_symbol = self.trading_pair.quote_token.symbol
        
        # Find tokens by currency
        gets_symbol = next((t.symbol for t in [self.trading_pair.base_token, self.trading_pair.quote_token] 
                          if t.currency == self.taker_gets.currency), None)
        pays_symbol = next((t.symbol for t in [self.trading_pair.base_token, self.trading_pair.quote_token] 
                          if t.currency == self.taker_pays.currency), None)
        
        if not gets_symbol or not pays_symbol:
            return MarketSide.UNKNOWN
        
        if gets_symbol == base_symbol and pays_symbol == quote_symbol:
            return MarketSide.SELL  # Selling base for quote
        elif gets_symbol == quote_symbol and pays_symbol == base_symbol:
            return MarketSide.BUY   # Buying base with quote
            
        return MarketSide.UNKNOWN
    
    @computed_field
    @property
    def original_amount(self) -> Optional[float]:
        """
        Calculate the original order amount in base currency.
        For buys: this is the taker_pays amount
        For sells: this is the taker_gets amount
        """
        if not self.trading_pair:
            return None
            
        try:
            base_symbol = self.trading_pair.base_token.symbol
            
            if self.market_side == MarketSide.BUY:
                # For buys, amount is in taker_pays (base currency)
                if self.taker_pays.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.taker_pays.value))
            else:  # SELL
                # For sells, amount is in taker_gets (base currency)
                if self.taker_gets.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.taker_gets.value))
            
            return None
        except (ValueError, TypeError):
            return None
    
    @computed_field
    @property
    def price(self) -> Optional[float]:
        """
        Calculate the price as quote/base.
        For buys: taker_gets/taker_pays (what you receive per what you pay)
        For sells: taker_pays/taker_gets (what you receive per what you give)
        """
        if not self.trading_pair or not self.original_amount or self.original_amount == 0:
            return None
            
        try:
            if self.market_side == MarketSide.BUY:
                # Buy: price is taker_gets (quote) / taker_pays (base)
                return float(Decimal(self.taker_gets.value) / Decimal(self.taker_pays.value))
            else:  # SELL
                # Sell: price is taker_pays (quote) / taker_gets (base)
                return float(Decimal(self.taker_pays.value) / Decimal(self.taker_gets.value))
        except (ValueError, TypeError, ZeroDivisionError):
            return None


class FilledOrder(BaseModel):
    """Represents a filled or canceled order on the XRPL."""
    hash: str
    account: str
    sequence: int
    created_ledger_index: int
    resolved_ledger_index: int
    taker_gets: XRPLAmount  # Original amount
    taker_pays: XRPLAmount  # Original amount
    filled_gets: Optional[XRPLAmount] = None  # Amount filled
    filled_pays: Optional[XRPLAmount] = None  # Amount filled
    status: OrderStatus
    user_id: str
    transaction_type: TransactionType
    created_date: datetime
    resolution_date: datetime
    cancel_tx_hash: Optional[str] = None
    trades: List[Trade] = Field(default_factory=list)
    fee_xrp: float = 0.0  # Transaction fee in XRP
    
    @computed_field
    @property
    def trading_pair(self) -> Optional[TradingPair]:
        """Determine the trading pair for this order."""
        return find_trading_pair(
            self.taker_gets.currency, 
            self.taker_gets.issuer, 
            self.taker_pays.currency, 
            self.taker_pays.issuer
        )
    
    @computed_field
    @property
    def market_side(self) -> MarketSide:
        """Determine if this is a buy or sell order."""
        if not self.trading_pair:
            return MarketSide.UNKNOWN
            
        pair_id = self.trading_pair.id
        base_symbol = self.trading_pair.base_token.symbol
        quote_symbol = self.trading_pair.quote_token.symbol
        
        # Find tokens by currency
        gets_symbol = next((t.symbol for t in [self.trading_pair.base_token, self.trading_pair.quote_token]
                          if t.currency == self.taker_gets.currency), None)
        pays_symbol = next((t.symbol for t in [self.trading_pair.base_token, self.trading_pair.quote_token]
                          if t.currency == self.taker_pays.currency), None)
        
        if not gets_symbol or not pays_symbol:
            return MarketSide.UNKNOWN
        
        if gets_symbol == base_symbol and pays_symbol == quote_symbol:
            return MarketSide.SELL  # Selling base for quote
        elif gets_symbol == quote_symbol and pays_symbol == base_symbol:
            return MarketSide.BUY   # Buying base with quote
            
        return MarketSide.UNKNOWN
    
    @computed_field
    @property
    def original_amount(self) -> Optional[float]:
        """
        Calculate the original order amount in base currency.
        For buys: this is the taker_pays amount
        For sells: this is the taker_gets amount
        """
        if not self.trading_pair:
            return None
        try:
            if self.market_side == MarketSide.BUY:
                # For buys, amount is in taker_pays (base currency)
                if self.taker_pays.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.taker_pays.value))
            else:  # SELL
                # For sells, amount is in taker_gets (base currency)
                if self.taker_gets.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.taker_gets.value))
            
            return None
        except (ValueError, TypeError):
            return None
    
    @computed_field
    @property
    def executed_amount(self) -> Optional[float]:
        """
        Calculate the executed amount in base currency.
        For buys: this is the filled_pays amount
        For sells: this is the filled_gets amount
        """
        if not self.trading_pair or not self.filled_gets or not self.filled_pays:
            return None
            
        try:
            if self.market_side == MarketSide.BUY:
                # For buys, executed amount is in filled_pays (base currency)
                if self.filled_pays.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.filled_pays.value))
            else:  # SELL
                # For sells, executed amount is in filled_gets (base currency)
                if self.filled_gets.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.filled_gets.value))
            
            return None
        except (ValueError, TypeError):
            return None
    
    @computed_field
    @property
    def price(self) -> Optional[float]:
        """
        Calculate the price as quote/base.
        For buys: taker_gets/taker_pays (what you receive per what you pay)
        For sells: taker_pays/taker_gets (what you receive per what you give)
        """
        if not self.trading_pair or not self.original_amount or self.original_amount == 0:
            return None
            
        try:
            if self.market_side == MarketSide.BUY:
                # Buy: price is taker_gets (quote) / taker_pays (base)
                return float(Decimal(self.taker_gets.value) / Decimal(self.taker_pays.value))
            else:  # SELL
                # Sell: price is taker_pays (quote) / taker_gets (base)
                return float(Decimal(self.taker_pays.value) / Decimal(self.taker_gets.value))
        except (ValueError, TypeError, ZeroDivisionError):
            return None
    
    @computed_field
    @property
    def executed_price(self) -> Optional[float]:
        """
        Calculate the executed price as quote/base from filled amounts.
        For buys: filled_gets/filled_pays
        For sells: filled_pays/filled_gets
        """
        if (not self.trading_pair or not self.filled_gets or not self.filled_pays or 
            not self.executed_amount or self.executed_amount == 0):
            return None
            
        try:
            if self.market_side == MarketSide.BUY:
                # Buy: executed price is filled_gets (quote) / filled_pays (base)
                return float(Decimal(self.filled_gets.value) / Decimal(self.filled_pays.value))
            else:  # SELL
                # Sell: executed price is filled_pays (quote) / filled_gets (base)
                return float(Decimal(self.filled_pays.value) / Decimal(self.filled_gets.value))
        except (ValueError, TypeError, ZeroDivisionError):
            return None


class CanceledOrder(BaseModel):
    """Represents an order that was canceled without being filled."""
    hash: str  # Hash of the offer creation transaction
    account: str
    sequence: int
    created_ledger_index: int
    canceled_ledger_index: int  # When the order was canceled
    taker_gets: XRPLAmount  # Original amount
    taker_pays: XRPLAmount  # Original amount
    status: OrderStatus = OrderStatus.CANCELED
    user_id: str
    transaction_type: TransactionType = TransactionType.OFFER_CREATE
    created_date: datetime
    canceled_date: datetime
    cancel_tx_hash: str  # Hash of the cancel transaction
    create_fee_xrp: float = 0.0  # Fee for creating the offer
    cancel_fee_xrp: float = 0.0  # Fee for canceling the offer
    fee_xrp: float = 0.0  # Total fees (create + cancel)
    
    @computed_field
    @property
    def trading_pair(self) -> Optional[TradingPair]:
        """Determine the trading pair for this order."""
        return find_trading_pair(
            self.taker_gets.currency, 
            self.taker_gets.issuer, 
            self.taker_pays.currency, 
            self.taker_pays.issuer
        )
    
    @computed_field
    @property
    def market_side(self) -> MarketSide:
        """Determine if this is a buy or sell order."""
        if not self.trading_pair:
            return MarketSide.UNKNOWN
            
        pair_id = self.trading_pair.id
        base_symbol = self.trading_pair.base_token.symbol
        quote_symbol = self.trading_pair.quote_token.symbol
        
        # Find tokens by currency
        gets_symbol = next((t.symbol for t in [self.trading_pair.base_token, self.trading_pair.quote_token]
                          if t.currency == self.taker_gets.currency), None)
        pays_symbol = next((t.symbol for t in [self.trading_pair.base_token, self.trading_pair.quote_token]
                          if t.currency == self.taker_pays.currency), None)
        
        if not gets_symbol or not pays_symbol:
            return MarketSide.UNKNOWN
        
        if gets_symbol == base_symbol and pays_symbol == quote_symbol:
            return MarketSide.SELL  # Selling base for quote
        elif gets_symbol == quote_symbol and pays_symbol == base_symbol:
            return MarketSide.BUY   # Buying base with quote
            
        return MarketSide.UNKNOWN
    
    @computed_field
    @property
    def original_amount(self) -> Optional[float]:
        """
        Calculate the original order amount in base currency.
        For buys: this is the taker_pays amount
        For sells: this is the taker_gets amount
        """
        if not self.trading_pair:
            return None
            
        try:
            base_symbol = self.trading_pair.base_token.symbol
            
            if self.market_side == MarketSide.BUY:
                # For buys, amount is in taker_pays (base currency)
                if self.taker_pays.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.taker_pays.value))
            else:  # SELL
                # For sells, amount is in taker_gets (base currency)
                if self.taker_gets.currency == self.trading_pair.base_token.currency:
                    return float(Decimal(self.taker_gets.value))
            
            return None
        except (ValueError, TypeError):
            return None
    
    @computed_field
    @property
    def price(self) -> Optional[float]:
        """
        Calculate the price as quote/base.
        For buys: taker_gets/taker_pays (what you receive per what you pay)
        For sells: taker_pays/taker_gets (what you receive per what you give)
        """
        if not self.trading_pair or not self.original_amount or self.original_amount == 0:
            return None
            
        try:
            if self.market_side == MarketSide.BUY:
                # Buy: price is taker_gets (quote) / taker_pays (base)
                return float(Decimal(self.taker_gets.value) / Decimal(self.taker_pays.value))
            else:  # SELL
                # Sell: price is taker_pays (quote) / taker_gets (base)
                return float(Decimal(self.taker_pays.value) / Decimal(self.taker_gets.value))
        except (ValueError, TypeError, ZeroDivisionError):
            return None


class DepositWithdrawal(BaseModel):
    """Represents a deposit or withdrawal transaction."""
    hash: str
    ledger_index: int
    timestamp: datetime
    from_address: str
    to_address: str
    amount: XRPLAmount
    type: str  # "deposit" or "withdrawal"
    user_id: str
    fee_xrp: Optional[float] = 0.0  # Transaction fee in XRP (relevant for withdrawals)


class UserConfig(BaseModel):
    """User configuration model."""
    id: str
    wallets: List[str]
    tags: List[int] = Field(default_factory=list)


class MarketTrade(BaseModel):
    """Represents a market trade that filled one of our orders."""
    hash: str
    ledger_index: int
    timestamp: datetime
    taker_address: str  # The address that initiated the trade
    maker_address: str  # Our address (the one that had the offer)
    sold_amount: XRPLAmount  # What was sold by the maker (us)
    bought_amount: XRPLAmount  # What was bought by the maker (us)
    related_offer_sequence: Optional[int] = None  # The sequence number of the offer that was filled
    related_offer_hash: Optional[str] = None  # The hash of the offer that was filled
    user_id: str  # The user ID that owns the maker address
    fee_xrp: float = 0.0  # Transaction fee in XRP 