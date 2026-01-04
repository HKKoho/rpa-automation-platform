// Vercel-compatible stub version
// For full implementation with BigQuery, see BigQueryClient.local.ts

/**
 * BigQuery Client Stub for Vercel Deployment
 *
 * This is a placeholder that allows the documentation site to build on Vercel.
 * For local development with full BigQuery functionality, use package.local.json
 * which includes @google-cloud/bigquery dependency.
 */

export interface BatchInsertResult {
  success: boolean;
  insertedRows: number;
  inserted: number;
  failed: number;
  errors: any[];
}

export class BigQueryClient {
  private client: any = null;
  private dataset: any = null;
  private projectId: string;
  private datasetId: string;

  constructor() {
    this.projectId = process.env.GCP_PROJECT_ID || '';
    this.datasetId = process.env.BQ_DATASET || 'rpa_warehouse';
    console.warn('BigQueryClient stub loaded - BigQuery functionality not available on Vercel');
  }

  async streamInsert(tableName: string, rows: any[]): Promise<void> {
    throw new Error(
      'BigQuery functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async batchInsert(tableName: string, rows: any[], options?: any): Promise<BatchInsertResult> {
    throw new Error(
      'BigQuery functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async loadFromFile(tableName: string, filePath: string, options?: any): Promise<void> {
    throw new Error(
      'BigQuery functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async createReadOnlyUser(email: string): Promise<void> {
    throw new Error(
      'BigQuery functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  getDataset(): any {
    return null;
  }

  getTable(tableName: string): any {
    return null;
  }

  async query<T = any>(sql: string, options?: any): Promise<T[]> {
    throw new Error(
      'BigQuery functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }
}
