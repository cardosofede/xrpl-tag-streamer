## Updated Transaction Processing Workflow

For each wallet associated with a user ID, the system should:

1. **Determine Starting Point**:
   - Get the last transaction related to the wallet stored in MongoDB 
   - Use that ledger index as starting point
   - If no transactions exist, use a default ledger index

2. **Query Transactions**:
   - Query all transactions for that wallet since the determined starting ledger index
   - Store each transaction in the `transactions` collection as is

3. **Transaction Routing & Processing**:
   ```python
   async def process_transaction(tx: Dict[str, Any], user_id: str):
       # Store raw transaction
       self.transactions_collection.insert_one(tx)
       
       # Process based on transaction type
       if tx["TransactionType"] == "Payment":
           if self._is_deposit_or_withdrawal(tx):
               self._process_deposit_withdrawal(tx, user_id)
           elif self._is_market_trade(tx):
               self._process_market_trade(tx, user_id)
       
       elif tx["TransactionType"] == "OfferCreate":
           if self._is_offer_filled(tx):
               self._process_filled_offer(tx, user_id)
           else:
               self._process_open_offer(tx, user_id)
       
       elif tx["TransactionType"] == "OfferCancel":
           self._process_offer_cancel(tx, user_id)
   ``` 

### 3. `open_orders` Collection
```python
{
    "hash": str,  # Transaction hash
    "Account": str,  # Order owner
    "Sequence": int,  # Offer sequence number
    "created_ledger_index": int,  # Creation ledger
    "last_checked_ledger": int,  # Last status check
    "TakerGets": Dict,  # What the taker gets
    "TakerPays": Dict,  # What the taker pays
    "status": str,  # open, filled, partially_filled, canceled
    "user_id": str,  # Associated user ID
    "TransactionType": str,  # Always "OfferCreate"
    "cancel_tx_hash": str,  # For canceled orders
    "cancel_ledger_index": int,  # Cancel ledger
    "resolution_date": datetime  # When order was resolved
}
```

### 4. `ledgers` Collection
```python
{
    "ledger_index": int,  # Unique ledger identifier
    "raw": Dict  # Complete ledger data
}
```

### 5. `orders_filled` Collection
```python
{
    "hash": str,  # Transaction hash that created the order
    "Account": str,  # Order owner 
    "Sequence": int,  # Offer sequence number
    "created_ledger_index": int,  # Creation ledger
    "resolved_ledger_index": int,  # Ledger where the order was filled/partially filled/canceled
    "TakerGets": Dict,  # What the taker gets - original amount
    "TakerPays": Dict,  # What the taker pays - original amount
    "filled_gets": Dict,  # Amount of TakerGets that was filled
    "filled_pays": Dict,  # Amount of TakerPays that was filled
    "status": str,  # filled, partially_filled, canceled
    "user_id": str,  # Associated user ID
    "TransactionType": str,  # "OfferCreate" or "Payment" for market orders
    "cancel_tx_hash": str,  # For canceled orders
    "resolution_date": datetime,  # When order was resolved
    "trades": List[Dict]  # List of trades that contributed to filling this order
}
```

### 6. `deposits_withdrawals` Collection
```python
{
    "hash": str,  # Transaction hash
    "ledger_index": int,  # Ledger index
    "timestamp": datetime,  # Transaction timestamp
    "from_address": str,  # Sender address
    "to_address": str,  # Receiver address
    "amount": str,  # Amount transferred
    "currency": str,  # Currency code
    "issuer": str,  # Currency issuer (for non-XRP)
    "type": str,  # "deposit" or "withdrawal"
    "user_id": str  # Associated user ID
}
```

## Processing Logic

### Initial Setup
1. Initialize MongoDB connection with proper indexes
2. Load user configurations from database
3. For each user's wallet, determine starting ledger index

### Transaction Processing Pipeline

1. **Transaction Collection**:
   ```python
   async def _process_wallet(self, address: str, user_id: str, from_ledger: int):
       # Fetch transactions from XRPL
       # Process each transaction
       # Update database
   ```

2. **Offer Management**:
   ```python
   def _handle_offer_create(self, tx: Dict[str, Any]):
       # Create open order record
       # Track offer status
       # Update database
   ```

3. **Order Status Updates**:
   ```python
   async def _check_open_orders(self):
       # Query current offers
       # Update order statuses
       # Handle partial fills
   ```

## Updated Transaction Processing Workflow

For each wallet associated with a user ID, the system should:

1. **Determine Starting Point**:
   - Get the last transaction related to the wallet stored in MongoDB 
   - Use that ledger index as starting point
   - If no transactions exist, use a default ledger index

2. **Query Transactions**:
   - Query all transactions for that wallet since the determined starting ledger index
   - Store each transaction in the `transactions` collection as is

3. **Transaction Routing & Processing**:
   ```python
   async def process_transaction(tx: Dict[str, Any], user_id: str):
       # Store raw transaction
       self.transactions_collection.insert_one(tx)
       
       # Process based on transaction type
       if tx["TransactionType"] == "Payment":
           if self._is_deposit_or_withdrawal(tx):
               self._process_deposit_withdrawal(tx, user_id)
           elif self._is_market_trade(tx):
               self._process_market_trade(tx, user_id)
       
       elif tx["TransactionType"] == "OfferCreate":
           if self._is_offer_filled(tx):
               self._process_filled_offer(tx, user_id)
           else:
               self._process_open_offer(tx, user_id)
       
       elif tx["TransactionType"] == "OfferCancel":
           self._process_offer_cancel(tx, user_id)
   ```

### Detailed Processing Functions

#### Deposits/Withdrawals Processing
```python
def _process_deposit_withdrawal(self, tx: Dict[str, Any], user_id: str):
    # Extract payment details
    deposit_withdrawal = {
        "hash": tx["hash"],
        "ledger_index": tx["ledger_index"],
        "timestamp": datetime.fromtimestamp(tx["date"]),
        "from_address": tx["Account"],
        "to_address": tx["Destination"],
        "amount": self._extract_amount(tx),
        "currency": self._extract_currency(tx),
        "issuer": self._extract_issuer(tx),
        "type": "deposit" if tx["Destination"] in self.user_wallets[user_id] else "withdrawal",
        "user_id": user_id
    }
    
    # Insert into deposits_withdrawals collection
    self.deposits_withdrawals_collection.insert_one(deposit_withdrawal)
```

#### OfferCreate Processing
```python
def _process_open_offer(self, tx: Dict[str, Any], user_id: str):
    # Create open order record
    open_order = {
        "hash": tx["hash"],
        "Account": tx["Account"],
        "Sequence": tx["Sequence"],
        "created_ledger_index": tx["ledger_index"],
        "last_checked_ledger": tx["ledger_index"],
        "TakerGets": tx["TakerGets"],
        "TakerPays": tx["TakerPays"],
        "status": "open",
        "user_id": user_id,
        "TransactionType": "OfferCreate"
    }
    
    # Insert into open_orders collection
    self.open_orders_collection.insert_one(open_order)

def _process_filled_offer(self, tx: Dict[str, Any], user_id: str):
    # Create filled order record
    filled_order = {
        "hash": tx["hash"],
        "Account": tx["Account"],
        "Sequence": tx["Sequence"],
        "created_ledger_index": tx["ledger_index"],
        "resolved_ledger_index": tx["ledger_index"],
        "TakerGets": tx["TakerGets"],
        "TakerPays": tx["TakerPays"],
        "filled_gets": self._calculate_filled_amount(tx, "TakerGets"),
        "filled_pays": self._calculate_filled_amount(tx, "TakerPays"),
        "status": "filled",
        "user_id": user_id,
        "TransactionType": "OfferCreate",
        "resolution_date": datetime.fromtimestamp(tx["date"]),
        "trades": self._extract_trades_from_metadata(tx)
    }
    
    # Insert into orders_filled collection
    self.orders_filled_collection.insert_one(filled_order)
```

#### OfferCancel Processing
```python
def _process_offer_cancel(self, tx: Dict[str, Any], user_id: str):
    # Find the open order that matches the sequence number
    open_order = self.open_orders_collection.find_one({
        "Account": tx["Account"],
        "Sequence": tx["OfferSequence"]
    })
    
    if open_order:
        # Check if it was partially filled
        if self._was_partially_filled(open_order):
            # Create canceled order record with partially filled status
            canceled_order = {
                **open_order,
                "status": "partially_filled",
                "cancel_tx_hash": tx["hash"],
                "cancel_ledger_index": tx["ledger_index"],
                "resolved_ledger_index": tx["ledger_index"],
                "resolution_date": datetime.fromtimestamp(tx["date"]),
                "filled_gets": self._calculate_filled_amount_from_open_order(open_order, "TakerGets"),
                "filled_pays": self._calculate_filled_amount_from_open_order(open_order, "TakerPays"),
                "trades": self._extract_trades_for_open_order(open_order)
            }
            
            # Insert into orders_filled collection
            self.orders_filled_collection.insert_one(canceled_order)
        
        # Remove from open_orders collection
        self.open_orders_collection.delete_one({"_id": open_order["_id"]})
```

#### Market Trades Processing
```python
def _process_market_trade(self, tx: Dict[str, Any], user_id: str):
    # Create market order record
    market_order = {
        "hash": tx["hash"],
        "Account": tx["Account"],
        "created_ledger_index": tx["ledger_index"],
        "resolved_ledger_index": tx["ledger_index"],
        "status": "filled",
        "user_id": user_id,
        "TransactionType": "Payment",
        "resolution_date": datetime.fromtimestamp(tx["date"]),
        "trades": self._extract_trades_from_payment(tx)
    }
    
    # Insert into orders_filled collection
    self.orders_filled_collection.insert_one(market_order)

def _process_offer_consumed_by_payment(self, tx: Dict[str, Any], offer_sequence: int):
    # Find the open order that matches the sequence number
    open_order = self.open_orders_collection.find_one({
        "Sequence": offer_sequence
    })
    
    if open_order:
        # Update the open order or move it to filled orders if completely filled
        if self._is_offer_completely_filled(open_order, tx):
            # Move to orders_filled collection
            filled_order = {
                **open_order,
                "status": "filled",
                "resolved_ledger_index": tx["ledger_index"],
                "resolution_date": datetime.fromtimestamp(tx["date"]),
                "filled_gets": open_order["TakerGets"],  # Fully filled
                "filled_pays": open_order["TakerPays"],  # Fully filled
                "trades": self._extract_trades_for_open_order(open_order) + [self._create_trade_from_payment(tx)]
            }
            
            # Insert into orders_filled collection and remove from open_orders
            self.orders_filled_collection.insert_one(filled_order)
            self.open_orders_collection.delete_one({"_id": open_order["_id"]})
        else:
            # Update the open order with new amounts
            updated_amounts = self._calculate_remaining_amounts(open_order, tx)
            
            # Update in open_orders collection
            self.open_orders_collection.update_one(
                {"_id": open_order["_id"]},
                {"$set": {
                    "TakerGets": updated_amounts["TakerGets"],
                    "TakerPays": updated_amounts["TakerPays"],
                    "last_checked_ledger": tx["ledger_index"]
                }}
            )
```

## Data Structure Design Considerations

### Orders and Trades Organization

When designing the data model for storing orders and their associated trades, there are two main approaches to consider:

#### Option 1: Combined Orders and Trades (Recommended)
The approach outlined above with a combined `orders_filled` collection that includes trades as a nested array has several advantages:

1. **Reduced Complexity**: All trade information is directly associated with its parent order
2. **Easier Querying**: No need for joins when retrieving an order and its trades
3. **Data Integrity**: Trades are always stored with their context (the parent order)
4. **Simpler Updates**: When additional trades occur for a partially filled order, the update is localized

#### Option 2: Separate Collections
Alternatively, we could use separate collections:

```python
# orders_filled Collection
{
    "hash": str,
    "Account": str,
    "Sequence": int,
    "created_ledger_index": int,
    "resolved_ledger_index": int,
    "TakerGets": Dict,
    "TakerPays": Dict,
    "filled_gets": Dict,
    "filled_pays": Dict,
    "status": str,
    "user_id": str,
    "TransactionType": str,
    "resolution_date": datetime
}

# trades Collection
{
    "order_hash": str,  # References the parent order
    "ledger_index": int,
    "timestamp": datetime,
    "taker_address": str,
    "maker_address": str,
    "gets_amount": Dict,
    "pays_amount": Dict,
    "user_id": str
}
```

This approach has these considerations:
1. **Flexibility**: Trades can be queried independently
2. **Normalization**: Follows database normalization principles
3. **Complexity**: Requires joins or multiple queries to get complete order data
4. **Performance**: May be slower for retrieving orders with their trades

For this specific use case where:
- Orders and their trades have a clear parent-child relationship
- Trades are primarily queried in the context of their parent order
- The number of trades per order is reasonable (unlikely to exceed MongoDB's document size limit)

The combined approach (Option 1) is recommended as it provides simpler querying and better data locality.

## Updated Indexing Strategy

```python
# Transactions collection indexes
collection.create_index("hash", unique=True)
collection.create_index("ledger_index")
collection.create_index("Account")
collection.create_index("Destination")
collection.create_index("user_id")

# Open orders collection indexes
open_orders_collection.create_index("Account")
open_orders_collection.create_index("Sequence", unique=True)
open_orders_collection.create_index("hash", unique=True)
open_orders_collection.create_index("created_ledger_index")
open_orders_collection.create_index([("status", pymongo.ASCENDING), ("Account", pymongo.ASCENDING)])

# Ledgers collection indexes
ledgers_collection.create_index("ledger_index", unique=True)

# Deposits/withdrawals collection indexes
deposits_withdrawals_collection.create_index("hash", unique=True)
deposits_withdrawals_collection.create_index("ledger_index")
deposits_withdrawals_collection.create_index("from_address")
deposits_withdrawals_collection.create_index("to_address")
deposits_withdrawals_collection.create_index("user_id")
deposits_withdrawals_collection.create_index("type")

# Orders filled collection indexes
orders_filled_collection.create_index("hash", unique=True)
orders_filled_collection.create_index("Account")
orders_filled_collection.create_index("Sequence")
orders_filled_collection.create_index("created_ledger_index")
orders_filled_collection.create_index("resolved_ledger_index")
orders_filled_collection.create_index("user_id")
orders_filled_collection.create_index("status")
orders_filled_collection.create_index([("status", pymongo.ASCENDING), ("Account", pymongo.ASCENDING)])
```

## Additional Query Examples

### Get User's Deposits and Withdrawals
```python
def get_deposits_withdrawals(self, user_id: str, tx_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    query = {"user_id": user_id}
    if tx_type:
        query["type"] = tx_type  # "deposit" or "withdrawal"
    return list(self.deposits_withdrawals_collection.find(query).sort("ledger_index", -1).limit(limit))
```

### Get User's Filled Orders
```python
def get_filled_orders(self, user_id: str, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    query = {"user_id": user_id}
    if status:
        query["status"] = status  # "filled", "partially_filled", or "canceled"
    return list(self.orders_filled_collection.find(query).sort("resolved_ledger_index", -1).limit(limit))
```

## Challenges and Solutions