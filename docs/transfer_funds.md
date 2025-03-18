# XRPL Transaction Flow Knowledge Base

This document explains how to interpret transaction flows in the XRP Ledger (XRPL), focusing on payment transactions.

## Basic Transaction Concepts

Every XRPL transaction has:
- A sender (the `Account` field)
- A transaction type (the `TransactionType` field)
- A fee (in drops of XRP)
- A digital signature

## Payment Transactions

A Payment transaction transfers value from one account to another. This is the most common transaction type for transferring assets.

### Example Payment Transaction:
```json
{
  "Account": "rfhigvRU4iNFexR9NkmJXChEtpTgbBLTwx",  // Sender
  "Amount": "1",                                    // Amount (in drops if XRP)
  "Destination": "rJtj42u8QPQWcPiwF3B8sNPb2GMo9gmNub", // Recipient
  "Fee": "11",                                      // Transaction fee
  "Flags": 0,
  "LastLedgerSequence": 94743713,
  "Memos": [
    {
      "Memo": {
        "MemoData": "436C61696D2024434F524520746F6B656E2061697264726F702061742068747470733A2F2F636F7265756D2E617070",
        "MemoType": "41697264726F70"
      }
    }
  ],
  "Sequence": 94769074,
  "SigningPubKey": "EDDB8E412B3167339E69FF6EB7719845DFE7E7A4D610D2FC00019D0A2AA54AAC93",
  "TransactionType": "Payment",
  "TxnSignature": "DA696E01253AECF66E62A1BD71E06136B5973C671E4C8A3D7C4669AC3F0EE8126BFDD8715F0EB3C6110301E6D03A3D8E51B3A32B6BCCED2C5F23C00BE8B2F802",
  "hash": "B83146D89C28EA5C6B3766EA4CD7C41B778D18EA394EAD64D882F463CD2051CA",
  "DeliverMax": "1",
  "ctid": "C5A5AC3F00360000"
}
```

### Transaction Metadata
The metadata shows the actual changes to the ledger, including balance changes:

```json
"meta": {
  "AffectedNodes": [
    {
      "ModifiedNode": {
        "FinalFields": {
          "Account": "rJtj42u8QPQWcPiwF3B8sNPb2GMo9gmNub",
          "Balance": "70440026",
          "Flags": 0,
          "OwnerCount": 5,
          "Sequence": 90653894
        },
        "PreviousFields": {
          "Balance": "70440025"
        }
      }
    },
    {
      "ModifiedNode": {
        "FinalFields": {
          "Account": "rfhigvRU4iNFexR9NkmJXChEtpTgbBLTwx",
          "Balance": "72457802",
          "Flags": 0,
          "OwnerCount": 0,
          "Sequence": 94769075
        },
        "PreviousFields": {
          "Balance": "72457814",
          "Sequence": 94769074
        }
      }
    }
  ],
  "TransactionResult": "tesSUCCESS",
  "delivered_amount": "1"
}
```

### Simple Interpretation:

- **Withdrawal**: If the sender (`Account` field) is our account, this is a withdrawal
- **Deposit**: If the recipient (`Destination` field) is our account, this is a deposit

### Key Points:

1. The payment transaction shows:
   - Who sent the payment (`Account` field)
   - Who received the payment (`Destination` field)
   - How much was sent (`Amount` field)
   - Whether it succeeded (`TransactionResult` field - should be "tesSUCCESS")

2. Looking at balance changes:
   - The sender's balance decreased (from 72457814 to 72457802)
   - The receiver's balance increased (from 70440025 to 70440026)
   - Note that the sender's balance decreased by more than 1 because of the transaction fee

3. For monitoring purposes:
   - Monitor the `Account` and `Destination` fields to identify transactions relevant to your accounts
   - Check the `TransactionResult` field to ensure it's "tesSUCCESS"
   - Use the `AffectedNodes` section to see the actual balance changes

### Different Types of Payments:

1. **XRP Payments**: The `Amount` field is a string representing drops of XRP (as shown in the example)

2. **Token Payments**: The `Amount` field is an object with currency, issuer, and value fields:
   ```json
   "Amount": {
     "currency": "USD",
     "issuer": "rhub8VRN55s94qWKDv6jmDy1pUykJzF3wq",
     "value": "100"
   }
   ```

3. **Payments with Tags**: Using SourceTag or DestinationTag fields to identify specific accounts or purposes:
   ```json
   {
     "TransactionType": "Payment",
     "Account": "rSender",
     "Destination": "rExchange",
     "SourceTag": 12345,
     "DestinationTag": 67890,
     "Amount": "1000000"
   }
   ```
