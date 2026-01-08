# BigQuery to Dynamics 365 Budget Impact Analysis

**Date**: 2026-01-08
**Purpose**: Analyze budget changes when replacing BigQuery with Dynamics 365 Business Central

---

## Executive Summary

Replacing BigQuery data warehouse with Dynamics 365 Business Central ERP provides:
- **Integrated ERP system** instead of separate data warehouse
- **Native Power BI integration** via Dataverse
- **Project accounting capabilities** for construction companies
- **Real-time operational data** vs. batch analytics warehouse

---

## Professional Tier Budget Analysis

### Current Configuration
- **Monthly Price**: $1,167
- **Annual Price**: $14,004
- **Users**: 25
- **Data Platform**: BigQuery warehouse (1TB)
- **BI Tool**: Power BI Pro (5 users)

### New Configuration with Dynamics 365
- **Monthly Price**: $1,167 (unchanged)
- **Annual Price**: $14,004 (unchanged)
- **Users**: 25
- **ERP Platform**: Dynamics 365 Business Central (25 Premium users)
- **BI Tool**: Power BI Pro (5 users)

### Dynamics 365 Licensing Breakdown

| Component | Quantity | Unit Price/Mo | Monthly Cost | Annual Cost |
|-----------|----------|---------------|--------------|-------------|
| Dynamics 365 Business Central Premium | 25 users | $100 | $2,500 | $30,000 |
| Power BI Pro | 5 users | $10 | $50 | $600 |
| **Subtotal (Dynamics 365)** | | | **$2,550** | **$30,600** |

### Budget Reallocation

**Previous allocation in $1,167/month**:
- BigQuery warehouse (1TB): ~$300/month (estimated)
- RPA platform services: ~$867/month

**New allocation in $1,167/month**:
- Dynamics 365 BC (25 users): $2,500/month
- RPA platform services: Reduced to accommodate D365

**Net Budget Impact**:
- **Additional cost needed**: +$1,383/month (+$16,596/year)
- **OR**: Bundle Dynamics 365 licenses separately and keep RPA platform at $1,167/month

### Recommended Pricing Adjustment

**Option 1: Bundled (Current Model)**
- Keep monthly price at $1,167 for RPA platform only
- Add separate line: "Dynamics 365 Business Central: $2,500/month"
- **Total monthly cost**: $3,667/month ($44,004/year)

**Option 2: All-Inclusive (New Model)**
- Increase monthly price to $3,667 to include Dynamics 365
- **New monthly price**: $3,667/month ($44,004/year)
- Includes: RPA platform + Dynamics 365 BC + Power BI Pro

---

## Enterprise Tier Budget Analysis

### Current Configuration
- **Monthly Price**: $12,950
- **Annual Price**: $155,400
- **Users**: 500
- **Data Platform**: BigQuery Enterprise (10TB)
- **BI Tool**: Power BI Premium

### New Configuration with Dynamics 365
- **Monthly Price**: $12,950 (unchanged)
- **Annual Price**: $155,400 (unchanged)
- **Users**: 500 (100 Premium + 400 Team Members)
- **ERP Platform**: Dynamics 365 Business Central
- **BI Tool**: Power BI Premium P1

### Dynamics 365 Licensing Breakdown

| Component | Quantity | Unit Price/Mo | Monthly Cost | Annual Cost |
|-----------|----------|---------------|--------------|-------------|
| Dynamics 365 Business Central Premium | 100 users | $100 | $10,000 | $120,000 |
| Dynamics 365 Team Members | 400 users | $8 | $3,200 | $38,400 |
| Power BI Premium P1 | 1 capacity | $4,995 | $4,995 | $59,940 |
| **Subtotal (Dynamics 365 + Power BI)** | | | **$18,195** | **$218,340** |

### Budget Reallocation

**Previous allocation in $12,950/month**:
- BigQuery Enterprise (10TB): ~$2,500/month (estimated)
- Power BI Premium: ~$5,000/month (estimated)
- RPA platform services: ~$5,450/month

**New allocation in $12,950/month**:
- Dynamics 365 BC (100+400 users): $13,200/month
- Power BI Premium P1: $4,995/month
- RPA platform services: Minimal allocation remaining

**Net Budget Impact**:
- **Additional cost needed**: +$5,245/month (+$62,940/year)
- **OR**: Bundle Dynamics 365 licenses separately and keep RPA platform at $12,950/month

### Recommended Pricing Adjustment

**Option 1: Bundled (Current Model)**
- Keep monthly price at $12,950 for RPA platform + core services
- Add separate line: "Dynamics 365 Business Central: $13,200/month"
- **Total monthly cost**: $26,150/month ($313,800/year)

**Option 2: All-Inclusive (New Model)**
- Increase monthly price to $31,145 to include everything
- **New monthly price**: $31,145/month ($373,740/year)
- Includes: RPA platform + Dynamics 365 BC + Power BI Premium P1

**Option 3: Hybrid (Recommended for Large Construction)**
- RPA Platform base: $12,950/month
- Dynamics 365 add-on: $13,200/month (billed separately)
- Power BI Premium P1: Included in D365 integration
- **Total**: $26,150/month ($313,800/year)

---

## Cost Comparison Summary

| Tier | Current Monthly | D365 Add-On | New Total Monthly | Annual Increase |
|------|----------------|-------------|-------------------|-----------------|
| **Professional** | $1,167 | +$2,550 | $3,717 | +$30,600 |
| **Enterprise** | $12,950 | +$13,200 | $26,150 | +$158,400 |

---

## Value Proposition: Why Dynamics 365 vs. BigQuery?

### BigQuery (Data Warehouse)
- **Purpose**: Analytics and reporting data warehouse
- **Data**: Historical, batch-loaded transactions
- **Use Case**: Business intelligence, trend analysis
- **Limitations**: Not an operational system, read-only

### Dynamics 365 Business Central (ERP)
- **Purpose**: Full operational ERP for construction companies
- **Data**: Real-time transactional data (GL, AP, AR, Project Accounting)
- **Use Case**: Day-to-day operations + analytics
- **Advantages**: Native system of record, not just a data copy

### Why Construction Companies Need Both (Potentially)

**Optimal Architecture**:
1. **Dynamics 365 Business Central**: Primary ERP for financials and project accounting
2. **BigQuery** (smaller): Data warehouse for multi-source analytics (bank data, equipment data, subcontractor data)
3. **RPA Platform**: Automate data flow from banks → Dynamics 365 and BigQuery

**Alternative Architecture** (D365-Only):
1. **Dynamics 365 Business Central**: Primary ERP + native reporting
2. **Power BI Premium**: Connected directly to Dynamics 365 Dataverse
3. **RPA Platform**: Automate bank data → Dynamics 365 only
4. **No BigQuery needed**: Use Dynamics 365 as system of record

---

## Recommended Pricing Adjustments

### Professional Tier - Revised

**Original pricing structure kept at $1,167/month, but reposition as**:

```
Professional Tier: $3,650/month ($43,800/year)

Includes:
✓ RPA Automation Platform: $1,167/month
✓ Dynamics 365 Business Central Premium (25 users): $2,500/month
✓ Power BI Pro (5 users): Included
✓ Robocorp Cloud (10K runs)
✓ AI: Data normalization (1K calls/mo)
✓ 99.5% uptime SLA
✓ 1-year data retention
✓ SOC 2 compliance tracking
```

**Setup Fee**: $10,100 (unchanged)

### Enterprise Tier - Revised

**Original pricing structure kept at $12,950/month, but reposition as**:

```
Enterprise Tier: $26,150/month ($313,800/year)

Includes:
✓ RPA Automation Platform: $12,950/month
✓ Dynamics 365 Business Central (100 Premium + 400 Team): $13,200/month
✓ Power BI Premium P1: Included in D365 bundle
✓ UiPath (5 robots) + Robocorp (50K runs)
✓ AI: Full suite (unlimited, GPT-4/Gemini/DeepSeek)
✓ 99.9% uptime SLA
✓ Unlimited data retention
✓ SOC 2 + PCI-DSS + ISO 27001
✓ Multi-region deployment
✓ Disaster recovery
```

**Setup Fee**: $125,000 (unchanged)

---

## Migration Timeline & Costs

### Professional Tier Migration

**One-Time Migration Costs** (in addition to setup fee):
- Dynamics 365 implementation: $25,000 - $50,000
- Data migration from legacy systems: $10,000 - $20,000
- User training: $5,000 - $10,000
- **Total migration**: $40,000 - $80,000

**Timeline**: 3-4 months

### Enterprise Tier Migration

**One-Time Migration Costs** (in addition to setup fee):
- Dynamics 365 implementation: $75,000 - $150,000
- Data migration from Dynamics SL/legacy: $25,000 - $50,000
- Power BI dashboard rebuild: $15,000 - $30,000
- User training: $15,000 - $25,000
- **Total migration**: $130,000 - $255,000

**Timeline**: 9-12 months

---

## ROI Considerations

### Professional Tier ROI with Dynamics 365

**Annual cost increase**: +$30,600
**Additional benefits**:
- Real-time project accounting (faster month-end close: -5 days)
- Integrated AP/AR (reduce invoice processing: 48hrs → 4hrs)
- Better job costing visibility (+3-5% gross margin improvement)

**Estimated value**: +$75,000 - $150,000/year in operational efficiency

**ROI**: Positive after 3-6 months

### Enterprise Tier ROI with Dynamics 365

**Annual cost increase**: +$158,400
**Additional benefits**:
- Multi-project visibility across 500 users
- Automated bank reconciliation (save 40 hours/month)
- Real-time WIP tracking (+5-7% gross margin improvement)
- Reduced IT complexity (-25% IT costs)

**Estimated value**: +$400,000 - $750,000/year in operational efficiency

**ROI**: Positive after 3-6 months

---

## Recommendations

### For Medium-Size Construction Companies (Professional Tier)

✅ **Adopt Dynamics 365** if:
- Currently using outdated ERP (Dynamics SL, QuickBooks Desktop)
- Need better project accounting and job costing
- Want real-time financial visibility
- Have 10-50 employees in accounting/project management

❌ **Keep BigQuery only** if:
- Already have modern ERP (e.g., Procore, Viewpoint)
- Only need analytics, not operational ERP
- Budget constraints prevent ERP upgrade

### For Large Construction Companies (Enterprise Tier)

✅ **Adopt Dynamics 365** if:
- Dynamics SL end-of-life is approaching (2028)
- Need multi-entity, multi-project accounting
- Want Power BI Premium with real-time dashboards
- Have 100+ users across multiple roles

⚡ **Hybrid Approach** (Recommended):
- Dynamics 365 for financials and project accounting
- Keep BigQuery (smaller size) for multi-source analytics
- RPA platform bridges bank data → both systems

---

## Next Steps

1. **Review budget impact** with stakeholders
2. **Assess current ERP situation** (Dynamics SL end-of-life?)
3. **Determine architecture**: D365-only vs. D365 + BigQuery hybrid
4. **Update pricing tiers** if bundling Dynamics 365
5. **Create separate Dynamics 365 add-on tier** if unbundled

---

**Document Version**: 1.0
**Last Updated**: 2026-01-08
**Owner**: Product & Finance Team
