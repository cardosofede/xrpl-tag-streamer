"""
Data models for XRPL transaction processing.
These models define the structure of data used throughout the application.
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union, Any

from pydantic import BaseModel, Field


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
    user_id: str  # The user ID that owns the maker address
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
    total_fee_xrp: float = 0.0  # Total fees (create + cancel) 