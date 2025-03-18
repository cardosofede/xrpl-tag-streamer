# XRPL Maker Order

A maker order is created when an account submits an `OfferCreate` transaction to the XRP Ledger, placing an offer in the decentralized exchange (DEX) order book. Unlike taker trades, which immediately consume liquidity, maker orders provide liquidity to the market by remaining available until filled by someone else's transaction.

## Basic Concept

In a maker order:
- The account submits an `OfferCreate` transaction
- The offer is stored in the order book if it doesn't immediately match with an existing offer
- It specifies the exact amount the maker wants to trade and receive
- The offer remains active until someone takes it, it expires, or it's cancelled

## Example Maker Order

Here's a simplified view of a maker order transaction:

```json
{
  "Account": "rJtj42u8QPQWcPiwF3B8sNPb2GMo9gmNub",  // Account placing the order
  "Fee": "20",                                      // Transaction fee (in drops)
  "Flags": 65536,                                   // tfPassive flag
  "Sequence": 90653883,                             // Sequence number (important!)
  "TakerGets": {                                    // What the taker would get (what maker is selling)
    "currency": "434F524500000000000000000000000000000000",
    "issuer": "rcoreNywaoz2ZCQ8Lg2EbSLnGuRBmun6D",
    "value": "0.00822651359201"
  },
  "TakerPays": "427",                               // What the taker would pay (what maker wants to receive)
  "TransactionType": "OfferCreate",                 // Transaction type for maker orders
  "SourceTag": 19089388                             // Optional tag for tracking purposes
}
```

### Key Fields for Maker Orders:

1. **Account**: The account creating the offer
2. **TakerGets**: What the taker would receive (what the maker is selling)
3. **TakerPays**: What the taker would pay (what the maker wants to receive)
4. **Flags**: Optional flags that modify the behavior of the offer
   - tfPassive (65536): The offer won't consume offers at the same price
   - tfImmediateOrCancel (131072): Cancel any unfilled portion of the offer
   - tfFillOrKill (262144): The order must be fully filled or not executed at all
5. **Sequence**: A unique identifier for this transaction from the account

## Connection to Taker Trades

This specific maker order is directly connected to the taker trade we previously examined:

1. This maker order (sequence 90653883) was created by rJtj42u8QPQWcPiwF3B8sNPb2GMo9gmNub offering to:
   - Sell 0.00822651 CORE tokens
   - To receive 0.000427 XRP (427 drops)

2. Later, the taker trade we examined (from rhubarbMVC2nzASf3qSGQcUKtLnAzqcBjp) consumed this exact offer:
   - The offer is identified by the sequence number 90653883
   - This offer was one of three offers consumed in that taker trade
   - It was fully consumed (deleted) during the taker trade

This demonstrates the complete lifecycle:
1. The maker creates the offer (this transaction)
2. The offer sits in the order book waiting for a match
3. A taker executes a trade that matches and consumes the offer

## Effects on the Ledger

When this maker order was placed, the following happened:

1. **Creation of the Offer**:
   - A new offer entry was created in the ledger with LedgerIndex "5EC79F93A9134EE0D1F391CF2C0EC487F869CFA21E9C718C2D695805DE3D5022"
   - The offer was added to a new order book directory (F68B089391A293F59FF060828463AA6976EF0EFCEC09F745591270C36A2083D0)

2. **Account Changes**:
   - The account's XRP balance decreased by 0.00002 XRP (transaction fee)
   - The account's owner count was affected due to adding a new offer to their directory

3. **Directory Structure**:
   - A new order book directory was created for this offer
   - The account's owner directory was updated to include this offer

4. **Previous Offer Management**:
   - A previous offer (sequence 90653882) from the same account was deleted
   - This represents the account updating or replacing their previous position in the market

## Maker vs. Taker

| Maker | Taker |
|-------|-------|
| Creates liquidity by placing offers in the order book | Consumes liquidity by taking existing offers from the order book |
| Uses `OfferCreate` transaction type | Uses `Payment` transaction type (with path finding) |
| Specifies exact amount to exchange | Specifies desired outcome and maximum input |
| Passive - waits for someone else to take the offer | Active - immediately executes against existing offers |
| May pay lower fees (on some exchanges) | Typically pays higher fees (on some exchanges) |
| Order remains until filled, cancelled, or expired | Transaction either succeeds or fails immediately |

## Monitoring Maker Orders

When monitoring for maker orders:

1. Look for `OfferCreate` transactions
2. Track the creation and deletion of offers in the metadata
3. Monitor the sequence numbers to track specific offers throughout their lifecycle
4. Check for offer modifications when partial fills occur
5. Connect maker orders to subsequent taker trades by matching offer details (account, sequence number, and amounts)

## Practical Uses

Maker orders are commonly used for:

1. **Market Making**: Providing liquidity to the market by placing buy and sell orders
2. **Limit Orders**: Setting a specific price at which you're willing to buy or sell
3. **Algorithmic Trading**: Automatically adjusting offers based on market conditions
4. **Arbitrage**: Profiting from price differences between markets by placing strategic offers
