# Phase 3: Real-Time Streaming & Advanced Analytics

Implementation guide for Phase 3 features of the Power BI BigQuery integration.

---

## Overview

Phase 3 builds on the foundation established in Phases 1-2:
- **Phase 1-2**: BigQuery schema, analytics API, job tracking ✅ **COMPLETED**
- **Phase 3**: Real-time streaming, materialized views, scheduled refreshes, advanced analytics

---

## Phase 3 Features

### 1. Real-Time Data Streaming

**Objective:** Stream transaction data to BigQuery in real-time (<5 second latency)

#### Architecture

```
Banking Network → RPA Extraction → ETL Pipeline → BigQuery Streaming Insert
                                                            ↓
                                                    Power BI DirectQuery
                                                            ↓
                                                    Real-Time Dashboards
```

#### Implementation

**a) Update ETL Pipeline for Streaming**

```typescript
// src/core/pipeline/ETLPipeline.ts

import { BigQueryClient } from '@/core/warehouse/BigQueryClient';

export class ETLPipeline {
  private bigQueryClient: BigQueryClient;
  private enableStreaming: boolean;

  constructor() {
    this.bigQueryClient = new BigQueryClient();
    this.enableStreaming = process.env.ENABLE_BQ_STREAMING === 'true';
  }

  async process(extractedData: ExtractedData): Promise<ETLResult> {
    // Validate
    const validatedData = await this.validate(extractedData);

    // Transform
    const transformedData = await this.transform(validatedData);

    // Load to BigQuery with streaming
    if (this.enableStreaming) {
      await this.streamToBigQuery(transformedData);
    } else {
      await this.batchLoad(transformedData);
    }

    return {
      status: 'completed',
      recordsProcessed: transformedData.length,
    };
  }

  /**
   * Stream individual transactions to BigQuery in real-time
   */
  private async streamToBigQuery(data: any[]): Promise<void> {
    const transactions = data.map((tx) => ({
      transaction_id: tx.transactionId,
      account_number: tx.accountNumber,
      amount: parseFloat(tx.amount),
      currency: tx.currency || 'USD',
      transaction_type: tx.transactionType,
      status: tx.status,
      transaction_timestamp: tx.timestamp.toISOString(),
      transaction_date: tx.timestamp.toISOString().split('T')[0],
      created_at: new Date().toISOString(),
      job_id: tx.jobId,
      source_bank_id: tx.sourceBank,
      destination_bank_id: tx.destinationBank,
      clearinghouse_id: tx.clearinghouseId,
      metadata: JSON.stringify(tx.metadata || {}),
    }));

    // Stream insert (< 5 second latency)
    await this.bigQueryClient.streamInsert('fact_banking_transactions', transactions);

    console.log(`[ETLPipeline] Streamed ${transactions.length} transactions to BigQuery`);
  }
}
```

**b) Environment Configuration**

```env
# Enable real-time streaming to BigQuery
ENABLE_BQ_STREAMING=true

# Streaming configuration
BQ_STREAMING_BATCH_SIZE=100
BQ_STREAMING_INTERVAL_MS=1000
```

### 2. Materialized Views for Performance

**Objective:** Pre-aggregate data for faster Power BI queries

#### Daily Transaction Summary View

```sql
-- server/database/materialized-views/mv_daily_transaction_summary.sql

CREATE MATERIALIZED VIEW `rpa_warehouse.mv_daily_transaction_summary`
PARTITION BY transaction_date
CLUSTER BY banking_network_id
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 60
)
AS
SELECT
  transaction_date,
  COALESCE(ft.clearinghouse_id, 'UNKNOWN') AS banking_network_id,
  dbn.network_name,
  dbn.network_type,
  COUNT(*) AS transaction_count,
  SUM(ft.amount) AS total_amount,
  AVG(ft.amount) AS avg_amount,
  COUNTIF(ft.status = 'completed') AS completed_count,
  COUNTIF(ft.status = 'failed') AS failed_count,
  SAFE_DIVIDE(COUNTIF(ft.status = 'completed'), COUNT(*)) * 100 AS success_rate
FROM `rpa_warehouse.fact_banking_transactions` ft
LEFT JOIN `rpa_warehouse.dim_banking_networks` dbn
  ON ft.clearinghouse_id = dbn.network_id
GROUP BY transaction_date, clearinghouse_id, dbn.network_name, dbn.network_type;
```

#### Job Performance Metrics View

```sql
-- server/database/materialized-views/mv_job_performance_metrics.sql

CREATE MATERIALIZED VIEW `rpa_warehouse.mv_job_performance_metrics`
PARTITION BY execution_date
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 30
)
AS
SELECT
  execution_date,
  job_id,
  dj.job_name,
  dj.extraction_method,
  banking_network_id,
  COUNT(*) AS execution_count,
  COUNTIF(status = 'success') AS success_count,
  COUNTIF(status = 'failed') AS failure_count,
  AVG(duration_ms) AS avg_duration_ms,
  MIN(duration_ms) AS min_duration_ms,
  MAX(duration_ms) AS max_duration_ms,
  STDDEV(duration_ms) AS stddev_duration_ms,
  SUM(records_extracted) AS total_records_extracted,
  AVG(data_size_bytes) AS avg_data_size_bytes
FROM `rpa_warehouse.fact_job_executions` fje
LEFT JOIN `rpa_warehouse.dim_jobs` dj
  ON fje.job_id = dj.job_id AND dj.is_current = TRUE
GROUP BY execution_date, job_id, dj.job_name, dj.extraction_method, banking_network_id;
```

#### Hourly Network Performance View

```sql
-- server/database/materialized-views/mv_hourly_network_performance.sql

CREATE MATERIALIZED VIEW `rpa_warehouse.mv_hourly_network_performance`
OPTIONS (
  enable_refresh = TRUE,
  refresh_interval_minutes = 15
)
AS
SELECT
  TIMESTAMP_TRUNC(started_at, HOUR) AS hour_timestamp,
  banking_network_id,
  dbn.network_name,
  dbn.network_type,
  COUNT(*) AS job_count,
  COUNTIF(status = 'success') AS success_count,
  AVG(duration_ms) AS avg_latency_ms,
  SAFE_DIVIDE(COUNTIF(status = 'success'), COUNT(*)) * 100 AS uptime_pct
FROM `rpa_warehouse.fact_job_executions` fje
LEFT JOIN `rpa_warehouse.dim_banking_networks` dbn
  ON fje.banking_network_id = dbn.network_id
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY hour_timestamp, banking_network_id, dbn.network_name, dbn.network_type;
```

#### Deployment Script

```typescript
// server/database/create-materialized-views.ts

import { BigQuery } from '@google-cloud/bigquery';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';

dotenv.config();

async function createMaterializedViews() {
  const bigquery = new BigQuery({
    projectId: process.env.GCP_PROJECT_ID,
    keyFilename: process.env.GCP_KEY_FILE,
  });

  const viewsDir = path.join(__dirname, 'materialized-views');
  const viewFiles = fs.readdirSync(viewsDir).filter((f) => f.endsWith('.sql'));

  console.log(`Creating ${viewFiles.length} materialized views...`);

  for (const file of viewFiles) {
    const viewName = file.replace('.sql', '');
    const sql = fs.readFileSync(path.join(viewsDir, file), 'utf8');

    console.log(`Creating view: ${viewName}...`);

    try {
      await bigquery.query(sql);
      console.log(`✓ Created: ${viewName}`);
    } catch (error) {
      console.error(`✗ Failed: ${viewName}`, error);
    }
  }

  console.log('Materialized views creation completed!');
}

// Run if executed directly
if (require.main === module) {
  createMaterializedViews()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { createMaterializedViews };
```

### 3. Scheduled Data Refreshes

**Objective:** Automate dimension table and materialized view refreshes

#### Scheduler Worker

```typescript
// server/workers/warehouseRefreshWorker.ts

import { BigQueryClient } from '@/core/warehouse/BigQueryClient';
import { DimensionSync } from '@/core/warehouse/DimensionSync';
import cron from 'node-cron';

/**
 * Warehouse Refresh Worker
 *
 * Schedules automatic refreshes of:
 * - Dimension tables (jobs, banking networks)
 * - Materialized views
 * - Data quality checks
 */
class WarehouseRefreshWorker {
  private bigQueryClient: BigQueryClient;
  private dimensionSync: DimensionSync;

  constructor() {
    this.bigQueryClient = new BigQueryClient();
    this.dimensionSync = new DimensionSync();
  }

  /**
   * Start scheduled refresh jobs
   */
  start() {
    // Refresh dimension tables every hour
    cron.schedule('0 * * * *', async () => {
      console.log('[WarehouseRefresh] Syncing dimension tables...');
      await this.refreshDimensions();
    });

    // Manually refresh materialized views every 30 minutes
    // (Auto-refresh is configured in BigQuery, this is backup)
    cron.schedule('*/30 * * * *', async () => {
      console.log('[WarehouseRefresh] Refreshing materialized views...');
      await this.refreshMaterializedViews();
    });

    // Data quality checks daily at 2 AM
    cron.schedule('0 2 * * *', async () => {
      console.log('[WarehouseRefresh] Running data quality checks...');
      await this.runDataQualityChecks();
    });

    console.log('[WarehouseRefresh] Scheduled refresh jobs started');
  }

  /**
   * Refresh dimension tables
   */
  private async refreshDimensions(): Promise<void> {
    try {
      await this.dimensionSync.syncBankingNetworks();
      console.log('[WarehouseRefresh] Dimension tables synced');
    } catch (error) {
      console.error('[WarehouseRefresh] Failed to sync dimensions:', error);
    }
  }

  /**
   * Manually refresh materialized views
   */
  private async refreshMaterializedViews(): Promise<void> {
    const views = [
      'mv_daily_transaction_summary',
      'mv_job_performance_metrics',
      'mv_hourly_network_performance',
    ];

    for (const view of views) {
      try {
        await this.bigQueryClient.query(
          `CALL BQ.REFRESH_MATERIALIZED_VIEW('rpa_warehouse.${view}')`
        );
        console.log(`[WarehouseRefresh] Refreshed: ${view}`);
      } catch (error) {
        console.error(`[WarehouseRefresh] Failed to refresh ${view}:`, error);
      }
    }
  }

  /**
   * Run data quality checks
   */
  private async runDataQualityChecks(): Promise<void> {
    // Check for data anomalies
    const checks = [
      {
        name: 'Transaction Volume Anomaly',
        query: `
          SELECT
            transaction_date,
            transaction_count,
            AVG(transaction_count) OVER (ORDER BY transaction_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_7d
          FROM rpa_warehouse.mv_daily_transaction_summary
          WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          HAVING ABS(transaction_count - avg_7d) / avg_7d > 0.5
        `,
      },
      {
        name: 'Job Failure Rate Spike',
        query: `
          SELECT
            execution_date,
            job_id,
            SAFE_DIVIDE(failure_count, execution_count) AS failure_rate
          FROM rpa_warehouse.mv_job_performance_metrics
          WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            AND SAFE_DIVIDE(failure_count, execution_count) > 0.1
        `,
      },
    ];

    for (const check of checks) {
      try {
        const results = await this.bigQueryClient.query(check.query);
        if (results.length > 0) {
          console.warn(`[DataQuality] ${check.name}: ${results.length} anomalies detected`);
          // TODO: Send alerts (email, Slack, etc.)
        } else {
          console.log(`[DataQuality] ${check.name}: PASSED`);
        }
      } catch (error) {
        console.error(`[DataQuality] Failed to run ${check.name}:`, error);
      }
    }
  }
}

// Start worker if executed directly
if (require.main === module) {
  const worker = new WarehouseRefreshWorker();
  worker.start();
}

export { WarehouseRefreshWorker };
```

**package.json script:**

```json
{
  "scripts": {
    "warehouse:refresh": "tsx server/workers/warehouseRefreshWorker.ts"
  }
}
```

### 4. Advanced Analytics Features

#### Predictive Analytics (Transaction Volume Forecasting)

```sql
-- BigQuery ML model for transaction volume prediction

CREATE OR REPLACE MODEL `rpa_warehouse.tx_volume_forecast_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='transaction_date',
  time_series_data_col='transaction_count',
  auto_arima=TRUE,
  data_frequency='DAILY'
) AS
SELECT
  transaction_date,
  SUM(transaction_count) AS transaction_count
FROM `rpa_warehouse.mv_daily_transaction_summary`
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
GROUP BY transaction_date
ORDER BY transaction_date;

-- Forecast next 30 days
SELECT
  forecast_timestamp,
  forecast_value AS predicted_transaction_count,
  standard_error,
  confidence_level
FROM ML.FORECAST(MODEL `rpa_warehouse.tx_volume_forecast_model`,
                 STRUCT(30 AS horizon, 0.95 AS confidence_level));
```

#### Anomaly Detection (Job Performance)

```sql
-- Detect job performance anomalies using BigQuery ML

CREATE OR REPLACE MODEL `rpa_warehouse.job_anomaly_detection_model`
OPTIONS(
  model_type='AUTOENCODER',
  activation_fn='RELU',
  dropout=0.2
) AS
SELECT
  job_id,
  avg_duration_ms,
  stddev_duration_ms,
  success_count,
  failure_count,
  total_records_extracted
FROM `rpa_warehouse.mv_job_performance_metrics`
WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

-- Detect anomalies in recent job executions
SELECT
  job_id,
  execution_date,
  is_anomaly,
  anomaly_probability
FROM ML.DETECT_ANOMALIES(MODEL `rpa_warehouse.job_anomaly_detection_model`,
                          STRUCT(0.02 AS contamination),
                          TABLE `rpa_warehouse.mv_job_performance_metrics`);
```

---

## Phase 3 Deployment Checklist

### Pre-Deployment

- [ ] Enable BigQuery Streaming API in GCP Console
- [ ] Configure materialized view quotas
- [ ] Set up BigQuery ML permissions
- [ ] Review and adjust refresh intervals

### Deployment Steps

1. **Create Materialized Views:**
   ```bash
   tsx server/database/create-materialized-views.ts
   ```

2. **Enable Streaming in ETL Pipeline:**
   ```bash
   # Update .env
   ENABLE_BQ_STREAMING=true
   ```

3. **Start Warehouse Refresh Worker:**
   ```bash
   npm run warehouse:refresh
   ```

4. **Update Power BI Reports:**
   - Replace fact table queries with materialized views
   - Test performance improvements
   - Publish to Power BI Service

5. **Create ML Models:**
   - Run forecasting model SQL
   - Run anomaly detection model SQL
   - Schedule weekly re-training

### Post-Deployment

- [ ] Monitor BigQuery streaming insert quotas
- [ ] Verify materialized view refresh schedules
- [ ] Check data quality alerts
- [ ] Review Power BI dashboard performance
- [ ] Set up cost alerts in GCP Billing

---

## Cost Optimization

### Streaming Inserts

- **Free tier:** First 2 TB/month
- **Cost:** $0.05/GB after free tier
- **Estimate:** ~$150/month for 3 TB/month

### Materialized Views

- **Storage:** Same as regular tables (~$0.02/GB/month)
- **Refresh:** Charged as regular queries
- **Estimate:** ~$50/month for 3 views

### BigQuery ML

- **Training:** Charged by bytes processed
- **Prediction:** $0.25/1000 predictions
- **Estimate:** ~$25/month for daily forecasts

**Total Phase 3 Cost:** ~$225/month

---

## Monitoring & Alerts

### Metrics to Monitor

1. **Streaming insert latency** (target: < 5 seconds)
2. **Materialized view freshness** (target: < 1 hour)
3. **Query performance** (target: < 10 seconds)
4. **Storage growth** (target: < 10% monthly)
5. **BigQuery costs** (budget: $300/month)

### Alert Configuration

```yaml
# alerts.yaml

alerts:
  - name: High Streaming Latency
    condition: streaming_latency > 10s
    action: Send Slack notification

  - name: Materialized View Stale
    condition: view_age > 2h
    action: Trigger manual refresh

  - name: BigQuery Cost Spike
    condition: daily_cost > $20
    action: Send email to finance team

  - name: Data Quality Failure
    condition: anomaly_count > 10
    action: Create incident in PagerDuty
```

---

## Next Steps After Phase 3

- **Phase 4:** Machine Learning for fraud detection
- **Phase 5:** Natural language query interface (Looker + BigQuery)
- **Phase 6:** Embedded analytics in customer portals

---

Ready to deploy Phase 3! 🚀
