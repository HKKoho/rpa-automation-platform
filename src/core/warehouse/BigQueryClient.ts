/**
 * BigQuery Client for Data Warehouse Operations
 *
 * Handles connection pooling, streaming inserts, batch operations, and error handling
 * for Google BigQuery data warehouse integration with Power BI reporting.
 */

import { BigQuery, Table, InsertRowsOptions, Dataset } from '@google-cloud/bigquery';
import { CredentialVault } from '@/core/security/CredentialVault';

export interface BatchInsertResult {
  inserted: number;
  failed: number;
}

/**
 * BigQuery Client Service
 *
 * Provides methods for:
 * - Streaming inserts (real-time data ingestion)
 * - Batch inserts with retry logic
 * - File-based bulk loading
 * - SQL query execution
 * - IAM and access control management
 */
export class BigQueryClient {
  private client: BigQuery;
  private dataset: string;
  private vault: CredentialVault;
  private initialized: boolean = false;

  constructor() {
    this.dataset = process.env.BQ_DATASET || 'rpa_warehouse';
    this.vault = new CredentialVault();
  }

  /**
   * Initialize BigQuery client with credentials from CredentialVault
   * Lazy initialization - called on first use
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;

    try {
      // Check if credentials exist in vault, otherwise use environment
      let credentials: any;

      try {
        const credsJson = await this.vault.retrieve('gcp-bigquery-service-account');
        credentials = JSON.parse(credsJson);
      } catch (error) {
        // Fallback to key file path if not in vault
        if (process.env.GCP_KEY_FILE) {
          credentials = undefined; // Will use keyFilename instead
        } else {
          throw new Error(
            'BigQuery credentials not found. Set GCP_KEY_FILE or store credentials in CredentialVault with ID "gcp-bigquery-service-account"'
          );
        }
      }

      this.client = new BigQuery({
        projectId: process.env.GCP_PROJECT_ID,
        credentials: credentials,
        keyFilename: credentials ? undefined : process.env.GCP_KEY_FILE,
      });

      this.initialized = true;
      console.log(`[BigQueryClient] Initialized for project: ${process.env.GCP_PROJECT_ID}, dataset: ${this.dataset}`);
    } catch (error) {
      console.error('[BigQueryClient] Initialization failed:', error);
      throw error;
    }
  }

  /**
   * Stream insert rows into BigQuery table
   * Uses streaming API for real-time inserts (< 5 second latency)
   *
   * @param tableName - Name of the table in the dataset
   * @param rows - Array of row objects to insert
   * @param options - Optional insert configuration
   */
  async streamInsert(
    tableName: string,
    rows: any[],
    options?: InsertRowsOptions
  ): Promise<void> {
    await this.ensureInitialized();

    if (!rows || rows.length === 0) {
      console.warn('[BigQueryClient] streamInsert called with empty rows array');
      return;
    }

    const table = this.client.dataset(this.dataset).table(tableName);

    try {
      await table.insert(rows, {
        raw: true, // Skip automatic schema detection
        skipInvalidRows: false, // Fail on any invalid row
        ignoreUnknownValues: false, // Fail on unknown columns
        ...options,
      });

      console.log(`[BigQueryClient] Successfully inserted ${rows.length} rows into ${tableName}`);
    } catch (error: any) {
      // Handle partial failures
      if (error.name === 'PartialFailureError') {
        const failedRows = error.errors?.length || 0;
        console.error(`[BigQueryClient] Partial insert failure: ${failedRows} rows failed`, error.errors);
        throw new Error(`Failed to insert ${failedRows} rows into ${tableName}`);
      }

      console.error(`[BigQueryClient] Stream insert failed for table ${tableName}:`, error);
      throw error;
    }
  }

  /**
   * Batch insert with retry logic
   * Splits large datasets into batches and retries on failure with exponential backoff
   *
   * @param tableName - Name of the table in the dataset
   * @param rows - Array of row objects to insert
   * @param batchSize - Number of rows per batch (default: 500)
   * @returns Summary of inserted and failed rows
   */
  async batchInsert(
    tableName: string,
    rows: any[],
    batchSize: number = 500
  ): Promise<BatchInsertResult> {
    await this.ensureInitialized();

    if (!rows || rows.length === 0) {
      return { inserted: 0, failed: 0 };
    }

    const maxRetries = parseInt(process.env.BQ_MAX_RETRIES || '3');
    let inserted = 0;
    let failed = 0;

    // Process in batches
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      let retries = 0;
      let success = false;

      while (retries < maxRetries && !success) {
        try {
          await this.streamInsert(tableName, batch);
          inserted += batch.length;
          success = true;
          console.log(`[BigQueryClient] Batch ${Math.floor(i / batchSize) + 1}: ${batch.length} rows inserted`);
        } catch (error) {
          retries++;
          if (retries >= maxRetries) {
            failed += batch.length;
            console.error(`[BigQueryClient] Batch failed after ${maxRetries} retries:`, error);
          } else {
            // Exponential backoff: 1s, 2s, 4s, ...
            const backoffMs = Math.pow(2, retries) * 1000;
            console.warn(`[BigQueryClient] Retry ${retries}/${maxRetries} after ${backoffMs}ms...`);
            await this.sleep(backoffMs);
          }
        }
      }
    }

    console.log(`[BigQueryClient] Batch insert completed: ${inserted} inserted, ${failed} failed`);
    return { inserted, failed };
  }

  /**
   * Load data from JSON file to BigQuery table
   * Useful for batch processing large datasets from file storage
   *
   * @param tableName - Name of the table in the dataset
   * @param filePath - Path to source file (JSON, CSV, or Parquet)
   * @param format - File format (default: JSON)
   */
  async loadFromFile(
    tableName: string,
    filePath: string,
    format: 'JSON' | 'CSV' | 'PARQUET' = 'JSON'
  ): Promise<void> {
    await this.ensureInitialized();

    const table = this.client.dataset(this.dataset).table(tableName);

    try {
      const [job] = await table.load(filePath, {
        sourceFormat: format,
        writeDisposition: 'WRITE_APPEND', // Append to existing table
        autodetect: false, // Don't auto-detect schema, use table schema
      });

      console.log(`[BigQueryClient] Load job started: ${job.id}`);

      // Wait for job completion
      await job.promise();
      console.log(`[BigQueryClient] Successfully loaded data from ${filePath} into ${tableName}`);
    } catch (error) {
      console.error(`[BigQueryClient] File load failed:`, error);
      throw error;
    }
  }

  /**
   * Execute SQL query and return results
   *
   * @param sql - SQL query string
   * @returns Array of result rows
   */
  async query<T = any>(sql: string): Promise<T[]> {
    await this.ensureInitialized();

    try {
      const [rows] = await this.client.query({ query: sql });
      return rows as T[];
    } catch (error) {
      console.error('[BigQueryClient] Query failed:', error);
      throw error;
    }
  }

  /**
   * Create read-only user for Power BI
   * Grants dataset viewer and job user roles to a service account
   *
   * @param email - Service account email (e.g., powerbi-readonly@PROJECT.iam.gserviceaccount.com)
   */
  async createReadOnlyUser(email: string): Promise<void> {
    await this.ensureInitialized();

    try {
      const dataset = this.client.dataset(this.dataset);
      const [metadata] = await dataset.getMetadata();

      // Add read-only permissions
      const policy = {
        bindings: [
          ...(metadata.access || []),
          {
            role: 'roles/bigquery.dataViewer',
            members: [`serviceAccount:${email}`],
          },
          {
            role: 'roles/bigquery.jobUser',
            members: [`serviceAccount:${email}`],
          },
        ],
      };

      await dataset.setMetadata({ access: policy.bindings });
      console.log(`[BigQueryClient] Granted read-only access to ${email}`);
    } catch (error) {
      console.error('[BigQueryClient] Failed to create read-only user:', error);
      throw error;
    }
  }

  /**
   * Get dataset reference
   * Useful for advanced operations not covered by this client
   */
  getDataset(): Dataset {
    if (!this.initialized) {
      throw new Error('BigQueryClient not initialized. Call a method that triggers initialization first.');
    }
    return this.client.dataset(this.dataset);
  }

  /**
   * Get table reference
   * Useful for advanced operations not covered by this client
   */
  getTable(tableName: string): Table {
    if (!this.initialized) {
      throw new Error('BigQueryClient not initialized. Call a method that triggers initialization first.');
    }
    return this.client.dataset(this.dataset).table(tableName);
  }

  /**
   * Sleep utility for retry backoff
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
