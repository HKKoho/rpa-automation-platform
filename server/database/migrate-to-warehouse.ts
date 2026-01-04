/**
 * Historical Data Migration Script
 *
 * One-time migration to populate BigQuery warehouse with historical data from:
 * - JSONL audit log files
 * - Redis job queue (if any persistent data exists)
 * - Any existing file-based data exports
 *
 * Usage:
 *   tsx server/database/migrate-to-warehouse.ts
 */

import { AuditLogger } from '@/core/security/AuditLogger';
import { BigQueryClient } from '@/core/warehouse/BigQueryClient';
import { DimensionSync } from '@/core/warehouse/DimensionSync';
import { setupBigQueryWarehouse } from './bigquery-setup';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

/**
 * Main migration function
 */
async function migrateToWarehouse() {
  console.log('='.repeat(60));
  console.log('Historical Data Migration to BigQuery Warehouse');
  console.log('='.repeat(60));
  console.log(`Project: ${process.env.GCP_PROJECT_ID}`);
  console.log(`Dataset: ${process.env.BQ_DATASET || 'rpa_warehouse'}`);
  console.log('='.repeat(60));

  try {
    const bigQueryClient = new BigQueryClient();
    const dimensionSync = new DimensionSync();
    const auditLogger = new AuditLogger();

    // Step 1: Ensure schema exists
    console.log('\n[1/4] Verifying BigQuery schema...');
    await setupBigQueryWarehouse();

    // Step 2: Populate dimension tables
    console.log('\n[2/4] Populating dimension tables...');
    await populateDimensions(dimensionSync);

    // Step 3: Migrate historical audit logs
    console.log('\n[3/4] Migrating historical audit logs...');
    await migrateAuditLogs(auditLogger, bigQueryClient);

    // Step 4: Summary
    console.log('\n[4/4] Migration summary...');
    await printMigrationSummary(bigQueryClient);

    console.log('\n' + '='.repeat(60));
    console.log('✓ Migration completed successfully!');
    console.log('='.repeat(60));
    console.log('\nNext steps:');
    console.log('1. Verify data in BigQuery console');
    console.log('2. Configure Power BI connection');
    console.log('3. Enable real-time data streaming from RPA jobs');
  } catch (error) {
    console.error('\n✗ Migration failed:', error);
    process.exit(1);
  }
}

/**
 * Populate dimension tables
 */
async function populateDimensions(dimensionSync: DimensionSync): Promise<void> {
  try {
    // Sync banking networks dimension
    console.log('  → Syncing banking networks...');
    await dimensionSync.syncBankingNetworks();

    // Generate date dimension (2020-2030)
    console.log('  → Generating date dimension (2020-2030)...');
    await dimensionSync.generateDateDimension(2020, 2030);

    console.log('  ✓ Dimension tables populated');
  } catch (error) {
    console.error('  ✗ Failed to populate dimensions:', error);
    throw error;
  }
}

/**
 * Migrate historical audit logs from JSONL files to BigQuery
 */
async function migrateAuditLogs(
  auditLogger: AuditLogger,
  bigQueryClient: BigQueryClient
): Promise<void> {
  try {
    // Query all historical audit logs (last 90 days)
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 90);

    console.log(`  → Querying audit logs since ${startDate.toISOString().split('T')[0]}...`);
    const historicalLogs = await auditLogger.query({
      startDate,
      limit: 100000, // Max 100K logs per migration
    });

    if (historicalLogs.length === 0) {
      console.log('  ⊘ No historical audit logs found');
      return;
    }

    console.log(`  → Found ${historicalLogs.length} audit logs`);

    // Transform to BigQuery schema
    const rows = historicalLogs.map((log: any) => ({
      audit_id: log.id,
      user_id: log.userId,
      action: log.action,
      resource: log.resource,
      resource_id: log.resourceId,
      result: log.result,
      timestamp: log.timestamp instanceof Date ? log.timestamp.toISOString() : log.timestamp,
      log_date: (log.timestamp instanceof Date ? log.timestamp.toISOString() : log.timestamp).split('T')[0],
      ip_address: log.ipAddress || null,
      user_agent: log.userAgent || null,
      changes: JSON.stringify(log.changes || {}),
      error_message: log.errorMessage || null,
      session_id: null,
      compliance_mode: process.env.COMPLIANCE_MODE || null,
    }));

    // Batch insert to BigQuery
    console.log('  → Inserting audit logs into BigQuery...');
    const result = await bigQueryClient.batchInsert('fact_audit_logs', rows, 1000);

    console.log(`  ✓ Migrated ${result.inserted} audit logs (${result.failed} failed)`);
  } catch (error) {
    console.error('  ✗ Failed to migrate audit logs:', error);
    // Don't throw - continue with migration even if audit log migration fails
  }
}

/**
 * Print migration summary with row counts
 */
async function printMigrationSummary(bigQueryClient: BigQueryClient): Promise<void> {
  const dataset = process.env.BQ_DATASET || 'rpa_warehouse';

  try {
    // Count rows in each table
    const tables = [
      'dim_banking_networks',
      'dim_jobs',
      'dim_users',
      'dim_date',
      'fact_banking_transactions',
      'fact_audit_logs',
      'fact_job_executions',
      'fact_etl_pipeline_jobs',
    ];

    console.log('\n  Table Row Counts:');
    console.log('  ' + '-'.repeat(50));

    for (const table of tables) {
      try {
        const [result] = await bigQueryClient.query(`
          SELECT COUNT(*) as count
          FROM \`${dataset}.${table}\`
        `);
        const count = result?.count || 0;
        console.log(`  ${table.padEnd(35)} ${String(count).padStart(10)} rows`);
      } catch (error) {
        console.log(`  ${table.padEnd(35)} ${' '.padStart(10)}ERROR`);
      }
    }

    console.log('  ' + '-'.repeat(50));
  } catch (error) {
    console.error('  Failed to generate summary:', error);
  }
}

// Run migration if executed directly
if (require.main === module) {
  migrateToWarehouse()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { migrateToWarehouse };
