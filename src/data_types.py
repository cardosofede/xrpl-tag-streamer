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
    """Represents a trade that contributed to filling an order."""
    tx_hash: str
    ledger_index: int
    timestamp: datetime
    taker_address: str
    maker_address: str
    sold_amount: XRPLAmount
    bought_amount: XRPLAmount


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


class UserConfig(BaseModel):
    """User configuration model."""
    id: str
    wallets: List[str]
    tags: List[int] = Field(default_factory=list) 