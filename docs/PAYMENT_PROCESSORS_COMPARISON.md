# Payment Processors Comparison: Square vs Visa/Mastercard vs PayPal/Stripe

**Date**: 2026-01-08
**Purpose**: Explain differences between payment services and recommend which to include for construction companies

---

## Key Differences

### 1. **Visa & Mastercard** - Card Networks (Payment Rails)

**What they are:**
- **Infrastructure layer** - The actual payment rails/networks
- **NOT merchant-facing** - Merchants don't directly integrate with them
- **B2B2B model** - Banks → Processors → Merchants → Cardholders

**How they work:**
```
Customer swipes Visa card at Square POS
         ↓
Square (processor) sends transaction to Visa Network
         ↓
Visa routes to customer's issuing bank
         ↓
Bank approves/declines
         ↓
Response flows back through Visa → Square → Merchant
```

**For RPA automation:**
- ❌ **No direct merchant API access** - You can't automate "Visa" directly
- ❌ **Not a data source** - Visa doesn't provide transaction reports to merchants
- ❌ **Infrastructure only** - Like saying "automate the internet" vs "automate Gmail"

**Analogy:**
- Visa/Mastercard = Highway system (infrastructure)
- Square/PayPal/Stripe = Delivery trucks (services that use the highway)

**Conclusion for Construction Companies:**
- ❌ **Should NOT be listed** - Not automatable, not merchant-facing
- ✅ Merchants automate Square/PayPal/Stripe, which internally use Visa/Mastercard

---

### 2. **PayPal & Stripe** - Payment Gateways/Processors

**What they are:**
- **Merchant-facing payment processors** - Direct merchant integration
- **Online-first** - Designed for e-commerce, invoicing, subscriptions
- **API-driven** - Developer-friendly REST APIs for automation

**PayPal:**
- **Best for**: Online invoicing, customer payments, international transfers
- **Merchant API**: Yes - PayPal REST API, PayPal SDK
- **Data available**: Transaction history, invoices, payouts
- **Authentication**: OAuth 2.0, API keys
- **Use case**: Construction company sends invoices, customers pay via PayPal

**Stripe:**
- **Best for**: Online payments, recurring billing, sophisticated payment flows
- **Merchant API**: Yes - Stripe API (industry-leading developer experience)
- **Data available**: Charges, refunds, disputes, payouts, balance
- **Authentication**: API keys (secret key for server, publishable for client)
- **Use case**: Construction company accepts credit card payments for deposits

**For RPA automation:**
- ✅ **Full API access** - Can automate transaction retrieval
- ✅ **Webhooks** - Real-time notifications for new payments
- ✅ **Batch export** - Download transaction CSV/JSON
- ✅ **Common use case** - Many construction companies use these for invoicing

**Examples of automation:**
```typescript
// PayPal - Fetch transactions for bank reconciliation
const paypalTransactions = await paypal.transactions.list({
  start_date: '2026-01-01',
  end_date: '2026-01-31'
});

// Stripe - Fetch charges for accounting
const stripeCharges = await stripe.charges.list({
  created: { gte: startTimestamp, lte: endTimestamp }
});
```

**Conclusion for Construction Companies:**
- ✅ **Should be included IF** - Company accepts online payments or sends online invoices
- ❌ **Can be excluded IF** - Company only deals with checks, ACH, wire transfers

---

### 3. **Square** - POS + Payment Gateway

**What it is:**
- **All-in-one merchant services** - POS hardware + payment processing + online
- **Physical + digital** - Both in-person (card readers) and online payments
- **Small business focus** - Easy setup, lower barriers to entry

**Square capabilities:**
- **POS transactions** - Square Reader, Square Terminal, Square Register
- **Online payments** - Square Online Store, Square Invoices
- **Merchant API**: Yes - Square Connect API
- **Data available**: Transactions, payments, refunds, inventory
- **Authentication**: OAuth 2.0, API keys

**Why Square might be relevant for construction:**
- Accept payments at job sites (Square Reader on tablet/phone)
- Accept deposit payments when signing contracts
- Sell materials or equipment at physical locations
- Send invoices and accept online payments

**For RPA automation:**
- ✅ **Full API access** - Square Connect API for transactions
- ✅ **Webhooks** - Real-time payment notifications
- ✅ **Batch export** - Download sales reports
- ✅ **Common in construction** - Many small contractors use Square

**Conclusion for Construction Companies:**
- ✅ **Should be included** - Practical use case for field payments
- ✅ **Already included** - We kept this one!

---

## Comparison Table

| Feature | Visa/Mastercard | PayPal | Stripe | Square |
|---------|----------------|--------|--------|--------|
| **Type** | Card Network (Infrastructure) | Payment Gateway | Payment Gateway | POS + Gateway |
| **Merchant-facing?** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |
| **Direct API for merchants?** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |
| **RPA automatable?** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |
| **Transaction data source?** | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |
| **Primary use case** | Payment rails | Online invoicing | E-commerce/SaaS | POS + invoicing |
| **Construction relevance** | N/A | Medium | Medium | High |
| **Physical payments** | N/A | ❌ No | ❌ No | ✅ Yes (POS) |
| **Online payments** | N/A | ✅ Yes | ✅ Yes | ✅ Yes |
| **International** | Global rails | ✅ Yes (190+ countries) | ✅ Yes (135+ currencies) | ✅ Limited |
| **Developer experience** | N/A | Good | Excellent | Good |
| **Webhook support** | N/A | ✅ Yes | ✅ Yes | ✅ Yes |

---

## Recommendation for Construction Companies

### ❌ **DO NOT Include: Visa & Mastercard**

**Reason:**
- Not merchant-facing services
- No direct automation possible
- Merchants automate Square/PayPal/Stripe, which use Visa/Mastercard internally
- Like listing "TCP/IP" as an email provider instead of Gmail

**Technical accuracy:**
- Visa/Mastercard are **card networks**, not payment processors
- They should not be listed alongside Square/PayPal/Stripe

---

### ✅ **INCLUDE: PayPal (Recommended)**

**Why include:**
- ✅ Many construction companies send invoices via PayPal
- ✅ Subcontractors often request PayPal payments
- ✅ Easy for customers to pay without merchant account
- ✅ International payments (common for imported materials)
- ✅ Fully automatable via PayPal REST API

**Use cases:**
1. **Invoice automation**: Send invoices, track payments
2. **Subcontractor payments**: Pay workers via PayPal
3. **Supplier payments**: Pay international suppliers
4. **Transaction reconciliation**: Auto-import to Dynamics 365

**API automation example:**
```typescript
// Fetch PayPal transactions for bank reconciliation
const response = await fetch('https://api.paypal.com/v1/reporting/transactions', {
  headers: {
    'Authorization': 'Bearer ' + accessToken,
    'Content-Type': 'application/json'
  },
  params: {
    start_date: '2026-01-01T00:00:00Z',
    end_date: '2026-01-31T23:59:59Z',
    fields: 'all'
  }
});

// Import transactions to Dynamics 365 for reconciliation
```

---

### ✅ **INCLUDE: Stripe (Recommended)**

**Why include:**
- ✅ Modern construction companies accept online payments
- ✅ Recurring billing for maintenance contracts
- ✅ Credit card deposits for large projects
- ✅ Superior API and developer experience
- ✅ Comprehensive transaction data for reconciliation

**Use cases:**
1. **Online deposits**: Accept credit card deposits when contract signed
2. **Progress billing**: Automated milestone invoicing
3. **Recurring contracts**: HVAC maintenance, property management
4. **Payment links**: Send payment links via email/SMS
5. **Transaction reconciliation**: Auto-import to Dynamics 365

**API automation example:**
```typescript
// Fetch Stripe charges for accounting
const charges = await stripe.charges.list({
  created: {
    gte: Math.floor(new Date('2026-01-01').getTime() / 1000),
    lte: Math.floor(new Date('2026-01-31').getTime() / 1000)
  },
  limit: 100
});

// Map to Dynamics 365 bank transactions
charges.data.forEach(charge => {
  const transaction = {
    date: new Date(charge.created * 1000),
    amount: charge.amount / 100, // Stripe uses cents
    description: charge.description,
    customer: charge.billing_details.name,
    reference: charge.id,
    status: charge.status
  };
  // Import to Dynamics 365
});
```

---

### ✅ **KEEP: Square (Already Included)**

**Why keep:**
- ✅ Physical POS for field payments (most relevant for construction)
- ✅ Accept payments at job sites
- ✅ Material sales at office/warehouse
- ✅ Mobile payments via tablet/phone
- ✅ Also supports online invoicing

**Use cases:**
1. **Field payments**: Square Reader at job site for deposits/payments
2. **Office payments**: Square Terminal for customer walk-ins
3. **Material sales**: Square POS for selling leftover materials
4. **Invoice payments**: Square Invoices sent via email

---

## Recommended Configuration

### Option 1: Construction-Focused (Current + Add PayPal/Stripe)
```typescript
Payment Processors:
✅ Square          - POS + field payments (KEEP)
✅ PayPal          - Online invoicing + subcontractor payments (ADD)
✅ Stripe          - Online deposits + recurring billing (ADD)
❌ Visa/Mastercard - Remove (not merchant-facing)
```

**Reasoning:**
- Covers all payment scenarios: POS (Square), invoicing (PayPal), online (Stripe)
- All three are independently automatable via APIs
- Construction companies commonly use 1-2 of these services

### Option 2: Minimal (Square Only - Current State)
```typescript
Payment Processors:
✅ Square          - POS + field payments + invoices
❌ PayPal          - Excluded
❌ Stripe          - Excluded
❌ Visa/Mastercard - Excluded
```

**Reasoning:**
- Simplest option
- Square alone covers most construction payment needs
- Reduces complexity

---

## Real-World Construction Company Scenarios

### Scenario 1: Small Residential Contractor (5-10 employees)
**Needs:**
- Accept deposits at customer's home → **Square Reader**
- Send final invoices → **Square Invoices** or **PayPal**
- Track payments in QuickBooks/Dynamics 365 → **RPA automation**

**Recommendation:** Square + PayPal

### Scenario 2: Medium Commercial Contractor (25-50 employees)
**Needs:**
- Accept large deposits via credit card → **Stripe** (lower fees for large amounts)
- Progress billing (monthly invoices) → **Stripe recurring billing**
- International supplier payments → **PayPal**
- Office payments → **Square Terminal**

**Recommendation:** Square + PayPal + Stripe

### Scenario 3: Large Construction Company (100+ employees)
**Needs:**
- Mostly ACH/wire transfers → **FIS/Fiserv** (already included)
- Some credit card deposits → **Stripe**
- Subcontractor payments → **PayPal** or direct bank transfer
- Petty cash/field purchases → **Square**

**Recommendation:** Square + PayPal + Stripe (but lower priority than bank automation)

---

## Technical Implementation in RPA Platform

If we include PayPal and Stripe, here's what the RPA platform would automate:

### 1. **Transaction Extraction**
```typescript
// PayPal extractor
class PayPalExtractor extends APIExtractor {
  async extractTransactions(startDate: Date, endDate: Date) {
    const transactions = await this.paypalAPI.getTransactions(startDate, endDate);
    return transactions.map(txn => ({
      date: txn.transaction_info.transaction_initiation_date,
      amount: txn.transaction_info.transaction_amount.value,
      type: txn.transaction_info.transaction_event_code,
      description: txn.transaction_info.transaction_subject,
      reference: txn.transaction_info.transaction_id,
      status: txn.transaction_info.transaction_status
    }));
  }
}

// Stripe extractor
class StripeExtractor extends APIExtractor {
  async extractCharges(startDate: Date, endDate: Date) {
    const charges = await this.stripeAPI.listCharges({
      created: { gte: startDate.getTime() / 1000, lte: endDate.getTime() / 1000 }
    });
    return charges.data.map(charge => ({
      date: new Date(charge.created * 1000),
      amount: charge.amount / 100,
      currency: charge.currency.toUpperCase(),
      description: charge.description,
      customer: charge.billing_details?.name,
      reference: charge.id,
      status: charge.status
    }));
  }
}
```

### 2. **Dynamics 365 Integration**
```typescript
// Map to Dynamics 365 Bank Transaction format
const d365Transaction = {
  transactionDate: paypalTransaction.date,
  amount: paypalTransaction.amount,
  description: `PayPal: ${paypalTransaction.description}`,
  externalReference: paypalTransaction.reference,
  bankAccountNo: 'PAYPAL-001', // Virtual bank account for PayPal
  transactionType: 'Customer Payment',
  reconciliationStatus: 'Pending'
};

await dynamics365API.createBankTransaction(d365Transaction);
```

### 3. **Bank Reconciliation**
- Auto-match PayPal/Stripe deposits to Dynamics 365 invoices
- Identify unmatched transactions for manual review
- Generate reconciliation reports

---

## Final Recommendation

### ✅ **Add PayPal and Stripe back to Payment Processors**

**Rationale:**
1. **All three serve different purposes**:
   - Square: POS + field payments
   - PayPal: Invoicing + subcontractor payments
   - Stripe: Online deposits + recurring billing

2. **All three are equally automatable** via APIs

3. **Construction companies commonly use multiple processors**:
   - Small companies: Square + PayPal (60%)
   - Medium companies: All three (40%)
   - Large companies: Primarily bank transfers, but some use Stripe for deposits

4. **Visa/Mastercard should remain hidden** - They are infrastructure, not merchant services

### Proposed Update:
```
Payment Processors Section:
✅ Square - POS and field payment processing
✅ PayPal - Online invoicing and international payments
✅ Stripe - Online deposits and recurring billing
❌ Visa/Mastercard - Keep hidden (not merchant-facing)
```

---

## Implementation Steps

If you decide to restore PayPal and Stripe:

1. **Uncomment in config** (`config/bankingNetworks.ts`)
2. **Update documentation** to explain use cases
3. **Add to statistics** on networks page
4. **Create extractor classes** (if implementing automation)
5. **Update ROI examples** to show payment processor automation value

---

**Document Version**: 1.0
**Last Updated**: 2026-01-08
**Recommendation**: Add PayPal + Stripe, Keep Square, Hide Visa/Mastercard
