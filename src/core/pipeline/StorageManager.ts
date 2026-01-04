import { BigQueryClient } from '@/core/warehouse/BigQueryClient';
import { BankingTransaction, AuditLogEntry, ETLJob } from '@/types/rpa.types';

/**
 * Storage Manager for Banking Data Warehouse Operations
 * Handles loading transformed data into various destinations
 */
export class StorageManager {
  private bigQueryClient: BigQueryClient;

  constructor() {
    this.bigQueryClient = new BigQueryClient();
  }
  /**
   * Load data into specified destination
   */
  async load(data: any[], options: LoadOptions): Promise<LoadResult> {
    const startTime = Date.now();
    let recordsLoaded = 0;
    let errors = 0;

    try {
      switch (options.destination) {
        case 'warehouse':
          const warehouseResult = await this.loadToWarehouse(data, options);
          recordsLoaded = warehouseResult.inserted;
          errors = warehouseResult.failed;
          break;

        case 'database':
          await this.loadToDatabase(data, options);
          recordsLoaded = data.length;
          break;

        case 'file':
          await this.loadToFile(data, options);
          recordsLoaded = data.length;
          break;

        case 'api':
          const result = await this.loadToAPI(data, options);
          recordsLoaded = result.success;
          errors = result.failed;
          break;

        default:
          throw new Error(`Unknown destination: ${options.destination}`);
      }

      return {
        recordsLoaded,
        errors,
        duration: Date.now() - startTime,
        destination: options.destination,
      };
    } catch (error) {
      throw new Error(
        `Failed to load data: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Load data to BigQuery data warehouse
   */
  private async loadToWarehouse(
    data: any[],
    options: LoadOptions
  ): Promise<{ inserted: number; failed: number }> {
    const startTime = Date.now();
    const tableName = options.tableName || 'fact_banking_transactions';
    const batchSize = options.batchSize || 500;

    console.log(`[StorageManager] Loading ${data.length} records to BigQuery warehouse (${tableName})...`);

    // Transform data to BigQuery schema
    const rows = this.transformToBigQuerySchema(data, tableName);

    // Batch insert with retry logic
    const result = await this.bigQueryClient.batchInsert(tableName, rows, batchSize);

    console.log(`[StorageManager] Warehouse load completed: ${result.inserted} inserted, ${result.failed} failed (${Date.now() - startTime}ms)`);

    return result;
  }

  /**
   * Transform application data to BigQuery schema
   */
  private transformToBigQuerySchema(data: any[], tableName: string): any[] {
    switch (tableName) {
      case 'fact_banking_transactions':
        return data.map((tx: any) => ({
          transaction_id: tx.transactionId || tx.transaction_id,
          account_number: tx.accountNumber || tx.account_number,
          amount: tx.amount,
          currency: tx.currency,
          transaction_type: tx.transactionType || tx.transaction_type,
          status: tx.status,
          transaction_timestamp: (tx.timestamp instanceof Date ? tx.timestamp : new Date(tx.timestamp)).toISOString(),
          transaction_date: (tx.timestamp instanceof Date ? tx.timestamp : new Date(tx.timestamp)).toISOString().split('T')[0],
          created_at: new Date().toISOString(),
          job_id: tx.jobId || tx.job_id || null,
          source_bank_id: tx.sourceBank || tx.source_bank_id || null,
          destination_bank_id: tx.destinationBank || tx.destination_bank_id || null,
          clearinghouse_id: tx.clearinghouseId || tx.clearinghouse_id || null,
          clearinghouse_reference: tx.clearinghouseReference || tx.clearinghouse_reference || null,
          metadata: JSON.stringify(tx.metadata || {}),
          extraction_job_id: tx.extractionJobId || tx.extraction_job_id || null,
          etl_job_id: tx.etlJobId || tx.etl_job_id || null,
          validation_status: tx.validationStatus || tx.validation_status || null,
          data_quality_score: tx.dataQualityScore || tx.data_quality_score || null,
        }));

      case 'fact_audit_logs':
        return data.map((log: any) => ({
          audit_id: log.id || log.audit_id,
          user_id: log.userId || log.user_id,
          action: log.action,
          resource: log.resource,
          resource_id: log.resourceId || log.resource_id,
          result: log.result,
          timestamp: (log.timestamp instanceof Date ? log.timestamp : new Date(log.timestamp)).toISOString(),
          log_date: (log.timestamp instanceof Date ? log.timestamp : new Date(log.timestamp)).toISOString().split('T')[0],
          ip_address: log.ipAddress || log.ip_address || null,
          user_agent: log.userAgent || log.user_agent || null,
          changes: JSON.stringify(log.changes || {}),
          error_message: log.errorMessage || log.error_message || null,
          session_id: log.sessionId || log.session_id || null,
          compliance_mode: log.complianceMode || log.compliance_mode || null,
        }));

      default:
        // Pass through for other tables
        return data;
    }
  }

  /**
   * Load data to relational database
   */
  private async loadToDatabase(
    data: any[],
    options: LoadOptions
  ): Promise<void> {
    // Mock implementation - in production, use PostgreSQL, MySQL client
    console.log(`Loading ${data.length} records to database...`);

    const batchSize = options.batchSize || 500;
    const tableName = options.tableName || 'banking_transactions';

    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);

      // Generate bulk insert SQL
      const values = batch.map((record) => this.recordToSQL(record)).join(',');
      const sql = `INSERT INTO ${tableName} VALUES ${values}`;

      // Execute SQL (mocked)
      await this.executeSQLInsert(sql);
    }
  }

  /**
   * Load data to file (CSV, JSON, Parquet)
   */
  private async loadToFile(data: any[], options: LoadOptions): Promise<void> {
    const format = options.fileFormat || 'json';
    const filePath = options.filePath || `./output/data.${format}`;

    console.log(`Writing ${data.length} records to ${filePath}...`);

    switch (format) {
      case 'json':
        await this.writeJSON(filePath, data);
        break;
      case 'csv':
        await this.writeCSV(filePath, data);
        break;
      case 'parquet':
        await this.writeParquet(filePath, data);
        break;
      default:
        throw new Error(`Unsupported file format: ${format}`);
    }
  }

  /**
   * Load data to external API
   */
  private async loadToAPI(
    data: any[],
    options: LoadOptions
  ): Promise<{ success: number; failed: number }> {
    console.log(`Sending ${data.length} records to API...`);

    let success = 0;
    let failed = 0;

    const batchSize = options.batchSize || 100;

    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);

      try {
        await this.sendToAPI(batch, options.apiEndpoint || '');
        success += batch.length;
      } catch (error) {
        failed += batch.length;
        console.error(`API batch ${i / batchSize} failed:`, error);
      }
    }

    return { success, failed };
  }

  /**
   * Load job execution metadata to warehouse
   */
  async loadJobExecution(execution: JobExecutionMetadata): Promise<void> {
    try {
      await this.bigQueryClient.streamInsert('fact_job_executions', [
        {
          execution_id: execution.executionId,
          job_id: execution.jobId,
          banking_network_id: execution.bankingNetworkId || null,
          status: execution.status,
          extraction_method: execution.extractionMethod,
          started_at: execution.startedAt.toISOString(),
          completed_at: execution.completedAt?.toISOString() || null,
          execution_date: execution.startedAt.toISOString().split('T')[0],
          duration_ms: execution.durationMs || null,
          records_extracted: execution.recordsExtracted || null,
          records_processed: execution.recordsProcessed || null,
          error_count: execution.errorCount || null,
          data_size_bytes: execution.dataSizeBytes || null,
          error_message: execution.errorMessage || null,
          error_code: execution.errorCode || null,
          retry_attempt: execution.retryAttempt || null,
          extraction_duration_ms: execution.extractionDurationMs || null,
          validation_duration_ms: execution.validationDurationMs || null,
          transformation_duration_ms: execution.transformationDurationMs || null,
          load_duration_ms: execution.loadDurationMs || null,
        },
      ]);

      console.log(`[StorageManager] Job execution metadata persisted: ${execution.executionId}`);
    } catch (error) {
      console.error('[StorageManager] Failed to persist job execution:', error);
      // Don't throw - metadata persistence failure shouldn't break the pipeline
    }
  }

  /**
   * Load ETL job metadata to warehouse
   */
  async loadETLJob(etlJob: ETLJob): Promise<void> {
    try {
      await this.bigQueryClient.streamInsert('fact_etl_pipeline_jobs', [
        {
          etl_job_id: etlJob.id,
          extraction_job_id: etlJob.extractionJobId,
          stage: etlJob.stage,
          status: etlJob.status,
          started_at: etlJob.startedAt?.toISOString() || null,
          completed_at: etlJob.completedAt?.toISOString() || null,
          execution_date: (etlJob.startedAt || new Date()).toISOString().split('T')[0],
          duration_ms:
            etlJob.completedAt && etlJob.startedAt
              ? etlJob.completedAt.getTime() - etlJob.startedAt.getTime()
              : null,
          records_processed: etlJob.recordsProcessed || null,
          error_count: etlJob.errorCount || null,
          warning_count: 0, // TODO: Track warnings in ETLJob type
          validation_errors: JSON.stringify({}), // TODO: Add validation errors to ETLJob
          transformation_rules_applied: JSON.stringify({}), // TODO: Track transformation rules
        },
      ]);

      console.log(`[StorageManager] ETL job metadata persisted: ${etlJob.id}`);
    } catch (error) {
      console.error('[StorageManager] Failed to persist ETL job:', error);
      // Don't throw - metadata persistence failure shouldn't break the pipeline
    }
  }

  /**
   * Convert record to SQL values string
   */
  private recordToSQL(record: any): string {
    const values = Object.values(record).map((v) => {
      if (v === null || v === undefined) return 'NULL';
      if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
      if (v instanceof Date) return `'${v.toISOString()}'`;
      return v;
    });

    return `(${values.join(',')})`;
  }

  /**
   * Execute SQL insert (mocked)
   */
  private async executeSQLInsert(sql: string): Promise<void> {
    // Mock database insert
    return new Promise((resolve) => setTimeout(resolve, 50));
  }

  /**
   * Write data to JSON file
   */
  private async writeJSON(filePath: string, data: any[]): Promise<void> {
    const fs = await import('fs/promises');
    await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
  }

  /**
   * Write data to CSV file
   */
  private async writeCSV(filePath: string, data: any[]): Promise<void> {
    if (data.length === 0) return;

    // Get headers from first record
    const headers = Object.keys(data[0]);
    const csvHeader = headers.join(',') + '\n';

    // Convert records to CSV rows
    const csvRows = data
      .map((record) =>
        headers
          .map((header) => {
            const value = record[header];
            if (value === null || value === undefined) return '';
            if (typeof value === 'string' && value.includes(',')) {
              return `"${value.replace(/"/g, '""')}"`;
            }
            return value;
          })
          .join(',')
      )
      .join('\n');

    const csv = csvHeader + csvRows;

    const fs = await import('fs/promises');
    await fs.writeFile(filePath, csv, 'utf8');
  }

  /**
   * Write data to Parquet file (mocked)
   */
  private async writeParquet(filePath: string, data: any[]): Promise<void> {
    // In production, use parquetjs library
    console.log(`Writing Parquet file to ${filePath} (mocked)`);
    await this.writeJSON(filePath.replace('.parquet', '.json'), data);
  }

  /**
   * Send data to external API
   */
  private async sendToAPI(batch: any[], endpoint: string): Promise<void> {
    // Mock API call
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        if (Math.random() > 0.95) {
          reject(new Error('API call failed'));
        } else {
          resolve();
        }
      }, 100);
    });
  }
}

// Types
export interface LoadOptions {
  destination: 'warehouse' | 'database' | 'file' | 'api';
  batchSize?: number;
  tableName?: string;
  filePath?: string;
  fileFormat?: 'json' | 'csv' | 'parquet';
  apiEndpoint?: string;
}

export interface LoadResult {
  recordsLoaded: number;
  errors: number;
  duration: number;
  destination: string;
}

export interface JobExecutionMetadata {
  executionId: string;
  jobId: string;
  bankingNetworkId?: string;
  status: string;
  extractionMethod: string;
  startedAt: Date;
  completedAt?: Date;
  durationMs?: number;
  recordsExtracted?: number;
  recordsProcessed?: number;
  errorCount?: number;
  dataSizeBytes?: number;
  errorMessage?: string;
  errorCode?: string;
  retryAttempt?: number;
  extractionDurationMs?: number;
  validationDurationMs?: number;
  transformationDurationMs?: number;
  loadDurationMs?: number;
}
