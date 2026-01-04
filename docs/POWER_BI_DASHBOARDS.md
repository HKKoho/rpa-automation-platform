# Power BI Dashboard Templates

Pre-built dashboard templates for RPA Banking Network Utility operations.

---

## Dashboard 1: Executive Overview

**Audience:** C-Suite, Senior Management
**Refresh:** Every 15 minutes (DirectQuery)

### Layout (1920x1080)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    RPA Banking Operations - Executive Dashboard                     │
├─────────────┬─────────────┬─────────────┬─────────────┬────────────┤
│  Total TX   │  Success    │   Avg Time  │   Active    │   Failed   │
│   $2.4B     │   98.5%     │    4.2s     │    12 jobs  │      3     │
│   +12% ↑    │   +0.5% ↑   │   -0.8s ↓   │             │   -2 ↓     │
└─────────────┴─────────────┴─────────────┴─────────────┴────────────┘
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │         Transaction Volume Trend (Last 30 Days)              │  │
│  │  [Line Chart: Daily transaction count and amount]            │  │
│  │                                                               │  │
│  │   Chart shows upward trend with annotations for key events   │  │
│  │                                                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────┐  ┌──────────────────────────────────┐│
│  │   TX by Banking Network   │  │   Job Performance Heat Map      ││
│  │   [Donut Chart]           │  │   [Matrix: Job x Hour]          ││
│  │                            │  │                                  ││
│  │   - ACH: 45%              │  │   Shows peak hours and slowest   ││
│  │   - SWIFT: 30%            │  │   performing jobs                ││
│  │   - FedWire: 15%          │  │                                  ││
│  │   - Other: 10%            │  │                                  ││
│  └──────────────────────────┘  └──────────────────────────────────┘│
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │         Top 10 Banking Partners by Volume                    │  │
│  │  [Table: Bank, TX Count, Amount, Success Rate, Avg Time]    │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### DAX Measures

```dax
// ==================== KPI Cards ====================

// Total Transaction Amount
Total TX Amount =
CALCULATE(
    SUM(fact_banking_transactions[amount]),
    dim_date[date_key] >= TODAY() - 30
)

// Total Transaction Amount (Previous Period)
Total TX Amount PY =
CALCULATE(
    [Total TX Amount],
    DATEADD(dim_date[date_key], -30, DAY)
)

// Transaction Amount Growth
TX Amount Growth % =
DIVIDE(
    [Total TX Amount] - [Total TX Amount PY],
    [Total TX Amount PY],
    0
) * 100

// Success Rate
Success Rate % =
VAR CompletedCount =
    CALCULATE(
        COUNTROWS(fact_banking_transactions),
        fact_banking_transactions[status] = "completed"
    )
VAR TotalCount = COUNTROWS(fact_banking_transactions)
RETURN
    DIVIDE(CompletedCount, TotalCount, 0) * 100

// Average Processing Time
Avg Processing Time (sec) =
CALCULATE(
    AVERAGE(fact_job_executions[duration_ms]) / 1000,
    dim_date[date_key] >= TODAY() - 30
)

// Active Jobs Count
Active Jobs =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] IN {"pending", "running"}
)

// Failed Jobs Count
Failed Jobs =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] = "failed",
    dim_date[date_key] >= TODAY() - 30
)

// ==================== Charts ====================

// Daily Transaction Count
Daily TX Count =
CALCULATE(
    COUNTROWS(fact_banking_transactions),
    ALL(dim_date),
    USERELATIONSHIP(fact_banking_transactions[transaction_date], dim_date[date_key])
)

// Daily Transaction Amount
Daily TX Amount =
CALCULATE(
    SUM(fact_banking_transactions[amount]),
    ALL(dim_date),
    USERELATIONSHIP(fact_banking_transactions[transaction_date], dim_date[date_key])
)
```

### Visuals Configuration

**KPI Cards:**
- Format: `#,##0.0,,,"B"` (billions) for amounts
- Format: `#,##0.0%` for percentages
- Format: `#,##0.0` for seconds
- Conditional formatting: Green for positive growth, red for negative

**Line Chart (Transaction Volume):**
- X-axis: `dim_date[date_key]`
- Y-axis (Column): `Daily TX Count`
- Y-axis (Line): `Daily TX Amount` (secondary axis)
- Data labels: Show on hover
- Trend line: Enabled
- Forecasting: 7 days ahead

**Donut Chart (TX by Network):**
- Legend: `dim_banking_networks[network_name]`
- Values: `COUNTROWS(fact_banking_transactions)`
- Colors: Custom palette (corporate colors)

---

## Dashboard 2: Operational Monitoring

**Audience:** Operations Team, RPA Engineers
**Refresh:** Every 5 minutes (DirectQuery)

### Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│               Real-Time Operational Monitoring                       │
├─────────────┬─────────────┬─────────────┬─────────────┬────────────┤
│  Jobs       │  Running    │   Queued    │   Completed │   Failed   │
│  Running    │     8       │      24     │    1,245    │     12     │
│  Now        │             │             │   (Today)   │  (Today)   │
└─────────────┴─────────────┴─────────────┴─────────────┴────────────┘
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │         Job Execution Timeline (Real-Time)                    │  │
│  │  [Gantt Chart: Job Start/End Times, Status]                  │  │
│  │                                                               │  │
│  │   Visual representation of job execution over time           │  │
│  │                                                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────┐  ┌────────────────────────────────────┐ │
│  │  Job Success Rate      │  │   Error Distribution              │ │
│  │  [Gauge]               │  │   [Clustered Bar Chart]           │ │
│  │                         │  │                                    │ │
│  │    98.5%               │  │   - Network Timeout: 45%          │ │
│  │                         │  │   - Auth Failed: 30%              │ │
│  │  Target: 95%           │  │   - Data Format: 15%              │ │
│  │  Max: 100%             │  │   - Other: 10%                    │ │
│  └───────────────────────┘  └────────────────────────────────────┘ │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │         Active Jobs Detail (Refreshes every 30 sec)          │  │
│  │  [Table: Job Name, Status, Progress, Started, Duration]     │  │
│  │  Conditional formatting: Red for jobs > 10 min              │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### DAX Measures

```dax
// ==================== Real-Time Metrics ====================

// Jobs Running Now
Jobs Running Now =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] = "running",
    fact_job_executions[started_at] >= NOW() - 1
)

// Jobs Queued
Jobs Queued =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] = "pending"
)

// Jobs Completed Today
Jobs Completed Today =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] = "success",
    fact_job_executions[execution_date] = TODAY()
)

// Jobs Failed Today
Jobs Failed Today =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] = "failed",
    fact_job_executions[execution_date] = TODAY()
)

// Job Success Rate (Last 24h)
Job Success Rate 24h =
VAR SuccessCount =
    CALCULATE(
        COUNTROWS(fact_job_executions),
        fact_job_executions[status] = "success",
        fact_job_executions[started_at] >= NOW() - 1
    )
VAR TotalCount =
    CALCULATE(
        COUNTROWS(fact_job_executions),
        fact_job_executions[started_at] >= NOW() - 1
    )
RETURN
    DIVIDE(SuccessCount, TotalCount, 0) * 100

// Average Job Duration (Running Jobs)
Avg Duration Running Jobs (min) =
CALCULATE(
    AVERAGEX(
        fact_job_executions,
        DATEDIFF(fact_job_executions[started_at], NOW(), MINUTE)
    ),
    fact_job_executions[status] = "running"
)

// Jobs Exceeding SLA (> 10 minutes)
Jobs Exceeding SLA =
CALCULATE(
    COUNTROWS(fact_job_executions),
    fact_job_executions[status] = "running",
    DATEDIFF(fact_job_executions[started_at], NOW(), MINUTE) > 10
)
```

---

## Dashboard 3: Compliance & Audit

**Audience:** Compliance Officers, Auditors
**Refresh:** Every hour

### Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Compliance & Audit Dashboard                       │
├─────────────┬─────────────┬─────────────┬─────────────┬────────────┤
│  Audit      │  Failed     │  Data       │  Credential │   Anomaly  │
│  Events     │  Operations │  Breaches   │  Rotations  │   Alerts   │
│   12,456    │      24     │       0     │      8      │      3     │
│  (30 days)  │  (7 days)   │  (30 days)  │   (Today)   │  (Today)   │
└─────────────┴─────────────┴─────────────┴─────────────┴────────────┘
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │      Audit Events Over Time (Daily Breakdown)                │  │
│  │  [Stacked Area Chart: Events by Action Type]                 │  │
│  │                                                               │  │
│  │   Shows volume and types of audit events                     │  │
│  │                                                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────────┐  ┌──────────────────────────────────┐│
│  │   Access by User           │  │   Failed Operations Detail      ││
│  │   [Treemap]                │  │   [Table with drill-through]    ││
│  │                             │  │                                  ││
│  │   Size: Event count        │  │   Timestamp, User, Action,      ││
│  │   Color: User role         │  │   Resource, Error Message       ││
│  │                             │  │                                  ││
│  └───────────────────────────┘  └──────────────────────────────────┘│
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │         Compliance Score Card                                │  │
│  │  [KPI Cards with Sparklines]                                 │  │
│  │                                                               │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │  │
│  │  │ PCI-DSS  │  │   GDPR   │  │  SOC2    │  │ ISO27001 │    │  │
│  │  │  98.5%   │  │  100%    │  │  97.2%   │  │  99.1%   │    │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### DAX Measures

```dax
// ==================== Compliance Metrics ====================

// Total Audit Events (30 Days)
Audit Events 30d =
CALCULATE(
    COUNTROWS(fact_audit_logs),
    dim_date[date_key] >= TODAY() - 30
)

// Failed Operations (7 Days)
Failed Operations 7d =
CALCULATE(
    COUNTROWS(fact_audit_logs),
    fact_audit_logs[result] = "failure",
    dim_date[date_key] >= TODAY() - 7
)

// Data Breach Count
Data Breaches =
CALCULATE(
    COUNTROWS(fact_audit_logs),
    fact_audit_logs[action] = "data.breach.detected",
    dim_date[date_key] >= TODAY() - 30
)

// Credential Rotations Today
Credential Rotations Today =
CALCULATE(
    COUNTROWS(fact_audit_logs),
    fact_audit_logs[action] = "credential.rotated",
    fact_audit_logs[log_date] = TODAY()
)

// Anomaly Alerts Today
Anomaly Alerts Today =
CALCULATE(
    COUNTROWS(fact_audit_logs),
    fact_audit_logs[action] IN {"anomaly.detected", "suspicious.activity"},
    fact_audit_logs[log_date] = TODAY()
)

// PCI-DSS Compliance Score
PCI-DSS Compliance % =
VAR TotalRequirements = 12
VAR MetRequirements =
    // Implement based on specific PCI-DSS requirements
    // Example: Check for encryption, access controls, logging, etc.
    CALCULATE(
        COUNTROWS(fact_audit_logs),
        fact_audit_logs[compliance_mode] = "PCI-DSS",
        fact_audit_logs[result] = "success"
    )
RETURN
    DIVIDE(MetRequirements, TotalRequirements, 0) * 100

// Unique Users Accessing System
Unique Users =
DISTINCTCOUNT(fact_audit_logs[user_id])

// Access Pattern Anomaly Score (0-100)
Access Anomaly Score =
VAR AvgAccess =
    CALCULATE(
        AVERAGEX(
            SUMMARIZE(
                fact_audit_logs,
                dim_date[date_key],
                "DailyAccess", COUNTROWS(fact_audit_logs)
            ),
            [DailyAccess]
        )
    )
VAR TodayAccess =
    CALCULATE(
        COUNTROWS(fact_audit_logs),
        fact_audit_logs[log_date] = TODAY()
    )
VAR Deviation = ABS(TodayAccess - AvgAccess) / AvgAccess
RETURN
    MIN(Deviation * 100, 100)
```

---

## Dashboard 4: Banking Network Performance

**Audience:** Network Operations, Banking Partners
**Refresh:** Every 30 minutes

### Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│             Banking Network Performance Analysis                     │
├─────────────┬─────────────┬─────────────┬─────────────┬────────────┤
│  Networks   │  Avg Latency│  Throughput │  Uptime     │   Errors   │
│  Active: 24 │    142ms    │  2,450 TX/h │   99.8%     │     0.2%   │
└─────────────┴─────────────┴─────────────┴─────────────┴────────────┘
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │      Network Performance Comparison (Latency & Success)       │  │
│  │  [Scatter Chart: X=Latency, Y=Success Rate, Size=Volume]     │  │
│  │                                                               │  │
│  │   Identifies best and worst performing networks              │  │
│  │                                                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────────────────┐  ┌─────────────────────────┐│
│  │  TX Volume by Network Type         │  │   Peak Hours Analysis   ││
│  │  [Stacked Column Chart]            │  │   [Heat Map]            ││
│  │                                     │  │                         ││
│  │  Clearinghouse: 60%                │  │   Hour x Day of Week    ││
│  │  Payment Processor: 30%            │  │   Color: TX volume      ││
│  │  Shared Infrastructure: 10%        │  │                         ││
│  └───────────────────────────────────┘  └─────────────────────────┘│
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │      Top 10 Networks by Volume & Performance                 │  │
│  │  [Table: Network, TX Count, Avg Latency, Success %, Uptime] │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### DAX Measures

```dax
// ==================== Network Performance ====================

// Active Networks Count
Active Networks =
CALCULATE(
    DISTINCTCOUNT(fact_banking_transactions[clearinghouse_id]),
    dim_date[date_key] >= TODAY() - 1
)

// Average Network Latency
Avg Network Latency (ms) =
AVERAGE(fact_job_executions[duration_ms])

// Throughput (TX per hour)
Throughput TX/h =
VAR Hours =
    DATEDIFF(
        MIN(fact_banking_transactions[transaction_timestamp]),
        MAX(fact_banking_transactions[transaction_timestamp]),
        HOUR
    )
RETURN
    DIVIDE(COUNTROWS(fact_banking_transactions), Hours, 0)

// Network Uptime %
Network Uptime % =
VAR SuccessfulConnections =
    CALCULATE(
        COUNTROWS(fact_job_executions),
        fact_job_executions[error_count] = 0
    )
VAR TotalAttempts = COUNTROWS(fact_job_executions)
RETURN
    DIVIDE(SuccessfulConnections, TotalAttempts, 0) * 100

// Network Error Rate
Network Error Rate % =
100 - [Network Uptime %]

// TX Volume by Network Type
TX by Network Type =
SWITCH(
    TRUE(),
    SELECTEDVALUE(dim_banking_networks[network_type]) = "clearinghouse",
        CALCULATE(COUNTROWS(fact_banking_transactions),
                  dim_banking_networks[network_type] = "clearinghouse"),
    SELECTEDVALUE(dim_banking_networks[network_type]) = "payment-processor",
        CALCULATE(COUNTROWS(fact_banking_transactions),
                  dim_banking_networks[network_type] = "payment-processor"),
    CALCULATE(COUNTROWS(fact_banking_transactions))
)
```

---

## Implementation Steps

### 1. Connect Power BI to BigQuery
Follow `POWER_BI_SETUP.md` for connection details.

### 2. Import Tables
Import all fact and dimension tables using DirectQuery mode.

### 3. Create Measures Table
Create a dedicated "Measures" table to store all DAX measures.

### 4. Build Dashboards
Create each dashboard on a separate page in Power BI Desktop.

### 5. Apply Filters
Add page-level filters:
- Date range: Last 30 days (adjustable)
- Banking network (multi-select)
- Job status (multi-select)

### 6. Configure Refresh
- DirectQuery: Real-time (no refresh needed)
- Import tables: Refresh every 1 hour

### 7. Publish to Power BI Service
Publish workspace and configure row-level security (RLS) if needed.

---

## Row-Level Security (RLS)

For multi-tenant scenarios, implement RLS:

```dax
// RLS Rule: Users can only see their own organization's data
[organization_id] = USERPRINCIPALNAME()
```

Apply this filter to fact tables.

---

## Performance Tips

1. **Use DirectQuery** for fact tables (large, frequently updated)
2. **Use Import** for dimension tables (small, infrequently updated)
3. **Always filter by date** to enable partition pruning
4. **Use materialized views** for complex aggregations
5. **Limit visuals per page** to 10-15 for optimal performance
6. **Disable cross-filtering** for non-interactive visuals

---

## Troubleshooting

**Issue:** Dashboards are slow
**Solution:** Add date filters, use materialized views, reduce visual count

**Issue:** Data not refreshing
**Solution:** Check BigQuery connection, verify DirectQuery is enabled

**Issue:** Metrics don't match database
**Solution:** Verify DAX measure logic, check for hidden filters

---

Ready to create world-class analytics! 📊
