/**
 * BigQuery Data Warehouse Schema Setup
 *
 * One-time initialization script to create:
 * - Dataset
 * - Fact tables (with partitioning and clustering)
 * - Dimension tables
 * - Materialized views
 * - Indexes and optimizations
 *
 * Run this script once to set up the Power BI reporting infrastructure.
 *
 * Usage:
 *   tsx server/database/bigquery-setup.ts
 */

import { BigQuery } from '@google-cloud/bigquery';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

const PROJECT_ID = process.env.GCP_PROJECT_ID;
const DATASET = process.env.BQ_DATASET || 'rpa_warehouse';
const LOCATION = process.env.BQ_LOCATION || 'US';

/**
 * Initialize BigQuery dataset and schema
 */
async function setupBigQueryWarehouse() {
  console.log('='.repeat(60));
  console.log('BigQuery Data Warehouse Setup');
  console.log('='.repeat(60));
  console.log(`Project: ${PROJECT_ID}`);
  console.log(`Dataset: ${DATASET}`);
  console.log(`Location: ${LOCATION}`);
  console.log('='.repeat(60));

  const bigquery = new BigQuery({
    projectId: PROJECT_ID,
    keyFilename: process.env.GCP_KEY_FILE,
  });

  try {
    // Step 1: Create dataset
    console.log('\n[1/4] Creating dataset...');
    await createDataset(bigquery);

    // Step 2: Create fact tables
    console.log('\n[2/4] Creating fact tables...');
    await createFactTables(bigquery);

    // Step 3: Create dimension tables
    console.log('\n[3/4] Creating dimension tables...');
    await createDimensionTables(bigquery);

    // Step 4: Create materialized views
    console.log('\n[4/4] Creating materialized views...');
    await createMaterializedViews(bigquery);

    console.log('\n' + '='.repeat(60));
    console.log('✓ BigQuery warehouse setup completed successfully!');
    console.log('='.repeat(60));
    console.log('\nNext steps:');
    console.log('1. Run dimension sync: tsx server/database/migrate-to-warehouse.ts');
    console.log('2. Configure Power BI connection');
    console.log('3. Start streaming data from RPA jobs');
  } catch (error) {
    console.error('\n✗ Setup failed:', error);
    process.exit(1);
  }
}

/**
 * Create BigQuery dataset
 */
async function createDataset(bigquery: BigQuery): Promise<void> {
  try {
    const [dataset] = await bigquery.createDataset(DATASET, {
      location: LOCATION,
      description: 'RPA Automation Platform Data Warehouse for Power BI Reporting',
    });
    console.log(`  ✓ Dataset created: ${dataset.id}`);
  } catch (error: any) {
    if (error.code === 409) {
      console.log(`  ⊘ Dataset already exists: ${DATASET}`);
    } else {
      throw error;
    }
  }
}

/**
 * Create fact tables with partitioning and clustering
 */
async function createFactTables(bigquery: BigQuery): Promise<void> {
  const factTables = [
    {
      name: 'fact_banking_transactions',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.fact_banking_transactions\` (
          -- Primary Key
          transaction_id STRING NOT NULL,

          -- Foreign Keys
          job_id STRING,
          source_bank_id STRING,
          destination_bank_id STRING,
          clearinghouse_id STRING,

          -- Transaction Details
          account_number STRING NOT NULL,
          amount NUMERIC(18, 2) NOT NULL,
          currency STRING NOT NULL,
          transaction_type STRING NOT NULL,
          status STRING NOT NULL,

          -- Temporal Dimensions
          transaction_timestamp TIMESTAMP NOT NULL,
          transaction_date DATE NOT NULL,
          created_at TIMESTAMP NOT NULL,

          -- Metadata
          clearinghouse_reference STRING,
          metadata JSON,

          -- Audit Fields
          extraction_job_id STRING,
          etl_job_id STRING,
          validation_status STRING,
          data_quality_score FLOAT64
        )
        PARTITION BY transaction_date
        CLUSTER BY transaction_type, status, currency
        OPTIONS(
          description="Banking transactions extracted from network sources",
          require_partition_filter=true,
          partition_expiration_days=2555
        )
      `,
    },
    {
      name: 'fact_audit_logs',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.fact_audit_logs\` (
          -- Primary Key
          audit_id STRING NOT NULL,

          -- Foreign Keys
          user_id STRING NOT NULL,

          -- Audit Details
          action STRING NOT NULL,
          resource STRING NOT NULL,
          resource_id STRING NOT NULL,
          result STRING NOT NULL,

          -- Temporal Dimensions
          timestamp TIMESTAMP NOT NULL,
          log_date DATE NOT NULL,

          -- Request Context
          ip_address STRING,
          user_agent STRING,

          -- Change Tracking
          changes JSON,
          error_message STRING,

          -- Metadata
          session_id STRING,
          compliance_mode STRING
        )
        PARTITION BY log_date
        CLUSTER BY action, resource, result
        OPTIONS(
          description="Immutable audit logs for compliance tracking",
          require_partition_filter=true,
          partition_expiration_days=90
        )
      `,
    },
    {
      name: 'fact_job_executions',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.fact_job_executions\` (
          -- Primary Key
          execution_id STRING NOT NULL,

          -- Foreign Keys
          job_id STRING NOT NULL,
          banking_network_id STRING,

          -- Execution Details
          status STRING NOT NULL,
          extraction_method STRING NOT NULL,

          -- Temporal Dimensions
          started_at TIMESTAMP NOT NULL,
          completed_at TIMESTAMP,
          execution_date DATE NOT NULL,
          duration_ms INT64,

          -- Metrics
          records_extracted INT64,
          records_processed INT64,
          error_count INT64,
          data_size_bytes INT64,

          -- Error Details
          error_message STRING,
          error_code STRING,
          retry_attempt INT64,

          -- Performance
          extraction_duration_ms INT64,
          validation_duration_ms INT64,
          transformation_duration_ms INT64,
          load_duration_ms INT64
        )
        PARTITION BY execution_date
        CLUSTER BY status, banking_network_id, extraction_method
        OPTIONS(
          description="Job execution history and performance metrics"
        )
      `,
    },
    {
      name: 'fact_etl_pipeline_jobs',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.fact_etl_pipeline_jobs\` (
          -- Primary Key
          etl_job_id STRING NOT NULL,

          -- Foreign Keys
          extraction_job_id STRING NOT NULL,

          -- Pipeline Details
          stage STRING NOT NULL,
          status STRING NOT NULL,

          -- Temporal Dimensions
          started_at TIMESTAMP,
          completed_at TIMESTAMP,
          execution_date DATE NOT NULL,
          duration_ms INT64,

          -- Metrics
          records_processed INT64,
          error_count INT64,
          warning_count INT64,

          -- Data Quality
          validation_errors JSON,
          transformation_rules_applied JSON
        )
        PARTITION BY execution_date
        CLUSTER BY status, stage
        OPTIONS(
          description="ETL pipeline execution tracking"
        )
      `,
    },
  ];

  for (const table of factTables) {
    try {
      await bigquery.query({ query: table.schema });
      console.log(`  ✓ Created fact table: ${table.name}`);
    } catch (error: any) {
      console.error(`  ✗ Failed to create ${table.name}:`, error.message);
      throw error;
    }
  }
}

/**
 * Create dimension tables
 */
async function createDimensionTables(bigquery: BigQuery): Promise<void> {
  const dimensionTables = [
    {
      name: 'dim_jobs',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.dim_jobs\` (
          job_id STRING NOT NULL,
          job_name STRING NOT NULL,

          -- Schedule Configuration
          cron_expression STRING,
          timezone STRING,
          schedule_enabled BOOLEAN,

          -- Source Configuration
          banking_network_id STRING,
          extraction_method STRING,

          -- Retry Configuration
          max_retry_attempts INT64,
          backoff_strategy STRING,

          -- Temporal
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL,
          deleted_at TIMESTAMP,

          -- Metadata
          tags ARRAY<STRING>,
          description STRING,

          -- SCD Type 2 Fields
          is_current BOOLEAN NOT NULL DEFAULT TRUE,
          effective_from TIMESTAMP NOT NULL,
          effective_to TIMESTAMP
        )
        CLUSTER BY job_id, is_current
        OPTIONS(
          description="Slowly Changing Dimension for RPA jobs"
        )
      `,
    },
    {
      name: 'dim_banking_networks',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.dim_banking_networks\` (
          network_id STRING NOT NULL,
          network_name STRING NOT NULL,
          network_type STRING NOT NULL,
          category STRING NOT NULL,

          -- Capabilities
          supports_real_time BOOLEAN,
          supports_batch BOOLEAN,
          supports_webhooks BOOLEAN,

          -- Technical Details
          protocols ARRAY<STRING>,
          auth_methods ARRAY<STRING>,
          api_endpoint STRING,

          -- Metadata
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        )
        CLUSTER BY network_id
        OPTIONS(
          description="Banking network source definitions"
        )
      `,
    },
    {
      name: 'dim_users',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.dim_users\` (
          user_id STRING NOT NULL,
          username STRING,
          email STRING,
          role STRING,
          department STRING,

          -- Temporal
          created_at TIMESTAMP NOT NULL,
          last_login_at TIMESTAMP,

          -- Status
          is_active BOOLEAN NOT NULL DEFAULT TRUE
        )
        CLUSTER BY user_id
        OPTIONS(
          description="User dimension for audit tracking"
        )
      `,
    },
    {
      name: 'dim_date',
      schema: `
        CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.dim_date\` (
          date_key DATE NOT NULL,
          year INT64,
          quarter INT64,
          month INT64,
          month_name STRING,
          week INT64,
          day_of_month INT64,
          day_of_week INT64,
          day_name STRING,
          is_weekend BOOLEAN,
          is_holiday BOOLEAN,
          fiscal_year INT64,
          fiscal_quarter INT64
        )
        CLUSTER BY date_key
        OPTIONS(
          description="Date dimension for time intelligence"
        )
      `,
    },
  ];

  for (const table of dimensionTables) {
    try {
      await bigquery.query({ query: table.schema });
      console.log(`  ✓ Created dimension table: ${table.name}`);
    } catch (error: any) {
      console.error(`  ✗ Failed to create ${table.name}:`, error.message);
      throw error;
    }
  }
}

/**
 * Create materialized views for Power BI performance
 */
async function createMaterializedViews(bigquery: BigQuery): Promise<void> {
  const views = [
    {
      name: 'mv_daily_transaction_summary',
      schema: `
        CREATE MATERIALIZED VIEW IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.mv_daily_transaction_summary\`
        OPTIONS(
          enable_refresh=true,
          refresh_interval_minutes=60
        )
        AS
        SELECT
          transaction_date,
          transaction_type,
          currency,
          status,
          COUNT(*) AS transaction_count,
          SUM(amount) AS total_amount,
          AVG(amount) AS average_amount,
          MIN(amount) AS min_amount,
          MAX(amount) AS max_amount,
          COUNTIF(validation_status = 'valid') AS valid_transactions,
          COUNTIF(validation_status = 'invalid') AS invalid_transactions
        FROM \`${PROJECT_ID}.${DATASET}.fact_banking_transactions\`
        GROUP BY transaction_date, transaction_type, currency, status
      `,
    },
    {
      name: 'mv_job_performance_metrics',
      schema: `
        CREATE MATERIALIZED VIEW IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.mv_job_performance_metrics\`
        OPTIONS(
          enable_refresh=true,
          refresh_interval_minutes=60
        )
        AS
        SELECT
          j.job_id,
          j.job_name,
          j.banking_network_id,
          DATE(e.started_at) AS execution_date,
          COUNT(*) AS total_executions,
          COUNTIF(e.status = 'success') AS successful_executions,
          COUNTIF(e.status = 'failed') AS failed_executions,
          AVG(e.duration_ms) AS avg_duration_ms,
          SUM(e.records_extracted) AS total_records_extracted,
          AVG(e.records_extracted) AS avg_records_per_execution
        FROM \`${PROJECT_ID}.${DATASET}.fact_job_executions\` e
        INNER JOIN \`${PROJECT_ID}.${DATASET}.dim_jobs\` j
          ON e.job_id = j.job_id AND j.is_current = TRUE
        GROUP BY j.job_id, j.job_name, j.banking_network_id, execution_date
      `,
    },
  ];

  for (const view of views) {
    try {
      await bigquery.query({ query: view.schema });
      console.log(`  ✓ Created materialized view: ${view.name}`);
    } catch (error: any) {
      console.error(`  ✗ Failed to create ${view.name}:`, error.message);
      throw error;
    }
  }
}

// Run setup if executed directly
if (require.main === module) {
  setupBigQueryWarehouse()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { setupBigQueryWarehouse };
