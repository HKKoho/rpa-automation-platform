// Vercel-compatible stub version
// For full implementation with BigQuery, see DimensionSync.local.ts

import type { RPAJob } from '@/types/rpa.types';

/**
 * Dimension Sync Stub for Vercel Deployment
 *
 * This is a placeholder that allows the documentation site to build on Vercel.
 * For local development with full dimension sync functionality, use package.local.json.
 */

export class DimensionSync {
  private bqClient: any = null;

  constructor() {
    console.warn('DimensionSync stub loaded - dimension sync functionality not available on Vercel');
  }

  async syncBankingNetworks(): Promise<void> {
    throw new Error(
      'Dimension sync functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async upsertJob(job: RPAJob): Promise<void> {
    throw new Error(
      'Dimension sync functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async upsertUser(
    userId: string,
    email: string,
    fullName: string,
    role: string,
    department?: string
  ): Promise<void> {
    throw new Error(
      'Dimension sync functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async generateDateDimension(startYear: number = 2020, endYear: number = 2030): Promise<void> {
    throw new Error(
      'Dimension sync functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }

  async syncAll(): Promise<void> {
    throw new Error(
      'Dimension sync functionality not implemented on Vercel. Use Google Cloud Functions or external service.'
    );
  }
}
