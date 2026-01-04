# Microsoft Power BI Setup Guide

Complete guide for connecting Power BI to the RPA Automation Platform's BigQuery data warehouse.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Connection Setup](#connection-setup)
3. [Table Selection](#table-selection)
4. [Direct Query Configuration](#direct-query-configuration)
5. [Data Model & Relationships](#data-model--relationships)
6. [Sample Dashboards](#sample-dashboards)
7. [Performance Optimization](#performance-optimization)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Access
- **Power BI Desktop** or **Power BI Service** with Pro license
- **BigQuery Read-Only Credentials** (service account JSON key)
- Access to `rpa_warehouse` dataset in Google Cloud Platform

### Service Account Setup
Contact your GCP administrator to obtain:
- Service account email: `powerbi-readonly@YOUR_PROJECT.iam.gserviceaccount.com`
- JSON key file downloaded from GCP Console
- Verify the account has these roles:
  - `roles/bigquery.dataViewer` (read data)
  - `roles/bigquery.jobUser` (run queries)

---

## Connection Setup

### Step 1: Install Power BI Desktop
Download from: https://powerbi.microsoft.com/desktop

### Step 2: Connect to BigQuery

1. Open Power BI Desktop
2. Click **Get Data** > **More**
3. Search for "**Google BigQuery**" (Native connector)
4. Click **Connect**

### Step 3: Authentication

Select **Service Account** authentication:

- **Billing Project ID**: `your-gcp-project-id`
- **Service Account Email**: `powerbi-readonly@YOUR_PROJECT.iam.gserviceaccount.com`
- **Service Account Key**: Click **Browse** and select your JSON key file

Click **Sign in** to authenticate.

---

## Table Selection

### Step 1: Navigate to Dataset

In the Navigator window:
1. Expand: `YOUR_PROJECT` > `rpa_warehouse`
2. You'll see all available tables and views

### Step 2: Select Tables

Select the following tables for your data model:

**Fact Tables** (transactional data):
- ☑ `fact_banking_transactions` - All banking transactions
- ☑ `fact_audit_logs` - Compliance and security audit trail
- ☑ `fact_job_executions` - Job execution history
- ☑ `fact_etl_pipeline_jobs` - ETL pipeline tracking

**Dimension Tables** (reference data):
- ☑ `dim_jobs` - RPA job configurations
- ☑ `dim_banking_networks` - Banking network sources
- ☑ `dim_users` - User dimension
- ☑ `dim_date` - Date dimension for time intelligence

**Materialized Views** (pre-aggregated for performance):
- ☑ `mv_daily_transaction_summary` - Daily transaction rollups
- ☑ `mv_job_performance_metrics` - Job KPIs

### Step 3: Load Tables

Click **Transform Data** to open Power Query Editor (recommended) or **Load** to load directly.

---

## Direct Query Configuration

**IMPORTANT**: For real-time data, configure **DirectQuery mode** instead of Import mode.

### In Power Query Editor:

1. Right-click on each table
2. Select **Storage Mode** > **DirectQuery**
3. Repeat for all fact tables
4. Dimension tables can use **Import** mode for better performance

### Recommended Configuration:

| Table Type | Storage Mode | Reason |
|------------|--------------|--------|
| Fact Tables | DirectQuery | Real-time data, large volume |
| Dimension Tables | Import | Small, changes infrequently |
| Materialized Views | DirectQuery | Pre-aggregated, updated hourly |

Click **Close & Apply** to save changes.

---

## Data Model & Relationships

### Auto-Detected Relationships

Power BI should auto-detect these relationships:

```
fact_banking_transactions[transaction_date] → dim_date[date_key]
fact_banking_transactions[job_id] → dim_jobs[job_id]
fact_job_executions[job_id] → dim_jobs[job_id]
fact_job_executions[banking_network_id] → dim_banking_networks[network_id]
fact_job_executions[execution_date] → dim_date[date_key]
fact_audit_logs[log_date] → dim_date[date_key]
fact_audit_logs[user_id] → dim_users[user_id]
fact_etl_pipeline_jobs[execution_date] → dim_date[date_key]
```

### Manual Relationship Creation

If relationships aren't auto-detected:

1. Go to **Model View** (left sidebar)
2. Drag from foreign key column to primary key column
3. Set **Cardinality**: Many-to-One (*)
4. Set **Cross-filter direction**: Single
5. Check **Make this relationship active**

---

## Sample Dashboards

### Dashboard 1: Transaction Overview

**Page Setup:**
- Canvas: 1920x1080
- Theme: Corporate

**Visuals:**

1. **KPI Cards** (Top Row):
   - Total Transaction Amount
     ```DAX
     Total Amount = SUM(fact_banking_transactions[amount])
     ```
   - Transaction Count
     ```DAX
     Transaction Count = COUNTROWS(fact_banking_transactions)
     ```
   - Average Transaction
     ```DAX
     Avg Transaction = AVERAGE(fact_banking_transactions[amount])
     ```
   - Success Rate
     ```DAX
     Success Rate % =
     DIVIDE(
       COUNTROWS(FILTER(fact_banking_transactions, fact_banking_transactions[status] = "completed")),
       COUNTROWS(fact_banking_transactions)
     ) * 100
     ```

2. **Line Chart**: Daily Transaction Volume
   - X-axis: `dim_date[date_key]`
   - Y-axis: `Transaction Count`
   - Filter: Last 30 days

3. **Donut Chart**: Transactions by Type
   - Legend: `fact_banking_transactions[transaction_type]`
   - Values: `Transaction Count`

4. **Table**: Top 10 Accounts by Volume
   - Columns: `account_number`, `Transaction Count`, `Total Amount`
   - Sort by: `Total Amount` descending
   - Top N filter: 10

5. **Map**: Transactions by Source Bank
   - Location: `fact_banking_transactions[source_bank_id]`
   - Size: `Total Amount`

### Dashboard 2: Job Performance

**Visuals:**

1. **Gauge**: Success Rate
   ```DAX
   Job Success Rate =
   DIVIDE(
     COUNTROWS(FILTER(fact_job_executions, fact_job_executions[status] = "success")),
     COUNTROWS(fact_job_executions)
   ) * 100
   ```
   - Target: 95%

2. **Column Chart**: Executions by Banking Network
   - X-axis: `dim_banking_networks[network_name]`
   - Y-axis: Count of executions
   - Legend: `fact_job_executions[status]`

3. **Line + Column Chart**: Success vs Failed Over Time
   - X-axis: `dim_date[date_key]`
   - Column: Success count
   - Line: Failed count

4. **Table**: Top 10 Slowest Jobs
   ```DAX
   Avg Duration (sec) = AVERAGE(fact_job_executions[duration_ms]) / 1000
   ```
   - Columns: Job Name, Avg Duration, Total Executions
   - Sort by: Avg Duration descending

### Dashboard 3: Compliance & Audit

**Visuals:**

1. **Card**: Total Audit Events (Last 30 Days)
   ```DAX
   Audit Events =
   CALCULATE(
     COUNTROWS(fact_audit_logs),
     dim_date[date_key] >= TODAY() - 30
   )
   ```

2. **Stacked Bar Chart**: Actions by Resource Type
   - Y-axis: `fact_audit_logs[resource]`
   - X-axis: Count
   - Legend: `fact_audit_logs[action]`

3. **Heatmap** (Matrix): Audit Activity by Hour and Day
   - Rows: `HOUR(fact_audit_logs[timestamp])`
   - Columns: `WEEKDAY(fact_audit_logs[timestamp])`
   - Values: Count of events

4. **Table**: Recent Failed Operations
   - Filter: `result = 'failure'`, Last 7 days
   - Columns: Timestamp, User, Action, Resource, Error Message

---

## Performance Optimization

### 1. Use Materialized Views

For dashboards with aggregations, use materialized views instead of fact tables:

**Good** (fast):
```DAX
Daily Transactions = SUM(mv_daily_transaction_summary[transaction_count])
```

**Avoid** (slow):
```DAX
Daily Transactions =
CALCULATE(
  COUNTROWS(fact_banking_transactions),
  ALLEXCEPT(fact_banking_transactions, fact_banking_transactions[transaction_date])
)
```

### 2. Apply Date Filters

**ALWAYS** filter fact tables by date to enable partition pruning:

```DAX
Last 30 Days Filter =
FILTER(
  dim_date,
  dim_date[date_key] >= TODAY() - 30 &&
  dim_date[date_key] <= TODAY()
)
```

Add this filter to all fact table visuals.

### 3. Limit Visual Interactivity

Reduce cross-filtering between visuals:
1. Select visual
2. Format pane > **Edit interactions**
3. Set non-critical visuals to **None**

### 4. Query Reduction

Enable query reduction:
1. File > Options > Query Reduction
2. Check ☑ **Reduce number of queries**
3. Add **Apply** button to slicers

### 5. Incremental Refresh (Power BI Service only)

For large fact tables, configure incremental refresh:
1. Right-click table > **Incremental refresh**
2. Archive data: 2 years
3. Refresh data: Last 7 days
4. Detect changes: `transaction_date`

---

## Sample DAX Measures

Create these measures in a dedicated "Measures" table:

### Transaction Metrics

```DAX
// Total Transaction Amount
Total Transaction Amount = SUM(fact_banking_transactions[amount])

// Transaction Count
Transaction Count = COUNTROWS(fact_banking_transactions)

// Average Transaction Size
Avg Transaction Size = AVERAGE(fact_banking_transactions[amount])

// Transaction Growth (vs previous period)
Transaction Growth % =
VAR CurrentPeriod = [Transaction Count]
VAR PreviousPeriod =
  CALCULATE(
    [Transaction Count],
    DATEADD(dim_date[date_key], -1, MONTH)
  )
RETURN
  DIVIDE(CurrentPeriod - PreviousPeriod, PreviousPeriod) * 100
```

### Job Performance Metrics

```DAX
// Job Success Rate
Job Success Rate % =
DIVIDE(
  COUNTROWS(FILTER(fact_job_executions, fact_job_executions[status] = "success")),
  COUNTROWS(fact_job_executions)
) * 100

// Average Execution Time
Avg Execution Time (min) =
AVERAGE(fact_job_executions[duration_ms]) / 60000

// Failed Jobs Count
Failed Jobs =
COUNTROWS(FILTER(fact_job_executions, fact_job_executions[status] = "failed"))
```

### Audit Metrics

```DAX
// Total Audit Events
Audit Events = COUNTROWS(fact_audit_logs)

// Failed Operations
Failed Operations =
COUNTROWS(FILTER(fact_audit_logs, fact_audit_logs[result] = "failure"))

// Unique Users
Unique Users = DISTINCTCOUNT(fact_audit_logs[user_id])
```

---

## Troubleshooting

### Connection Timeout

**Error**: "Query timeout expired"

**Solution**:
1. File > Options > Data Load
2. Increase **Query timeout**: 600 seconds (default)
3. Or add partition filters to reduce query size

### Slow Queries

**Error**: Visuals take > 10 seconds to load

**Solutions**:
1. ✅ Use materialized views instead of fact tables
2. ✅ Add date range filters (enable partition pruning)
3. ✅ Use Import mode for dimension tables
4. ✅ Reduce number of visuals per page
5. ✅ Check query execution plan in BigQuery console

### Authentication Errors

**Error**: "Unable to connect - permission denied"

**Solutions**:
1. Verify service account has `bigquery.dataViewer` role
2. Verify service account has `bigquery.jobUser` role
3. Check JSON key file is valid and not expired
4. Confirm project ID matches your GCP project

### Missing Tables

**Error**: "Table not found" or empty Navigator

**Solutions**:
1. Run schema setup: `tsx server/database/bigquery-setup.ts`
2. Verify dataset name matches `BQ_DATASET` in .env
3. Check service account has access to `rpa_warehouse` dataset

### Partition Filter Error

**Error**: "Partition filter is required"

**Solution**: Add date filter to visual:
1. Drag `dim_date[date_key]` to Filters pane
2. Select "is after" > Last 30 days
3. Apply filter

---

## Best Practices

### Security

1. **Never** share service account JSON keys via email
2. Rotate service account keys quarterly
3. Use separate service accounts for dev/staging/prod
4. Implement row-level security (RLS) for multi-tenant scenarios:
   ```DAX
   [user_id] = USERPRINCIPALNAME()
   ```

### Cost Management

1. **Always** use date filters (partition pruning saves 90%+ cost)
2. Limit DirectQuery report refresh to business hours
3. Monitor BigQuery costs in GCP Console > Billing
4. Set budget alerts at $50/month

### Data Governance

1. Document all custom measures in a central "Measures" table
2. Use consistent naming: `[Metric Name (Unit)]`
3. Add descriptions to all measures (right-click > Properties)
4. Version control .pbix files in Git

---

## Support Resources

- **BigQuery Documentation**: https://cloud.google.com/bigquery/docs
- **Power BI Documentation**: https://docs.microsoft.com/power-bi
- **DAX Guide**: https://dax.guide
- **Project Documentation**: `/docs/POWER_BI_IMPLEMENTATION.md`

---

**Setup Complete!** Your Power BI dashboards are now connected to real-time banking data from BigQuery.

For issues or questions, contact your platform administrator.
