/**
 * Dimension Synchronization Service
 *
 * Synchronizes dimension tables in BigQuery with application state.
 * Implements Slowly Changing Dimension (SCD) Type 2 for job history tracking.
 */

import { BigQueryClient } from './BigQueryClient';
import { getAllBankingNetworks, BankingNetworkSource } from '@/config/bankingNetworks';
import { RPAJob } from '@/types/rpa.types';

/**
 * DimensionSync Service
 *
 * Handles:
 * - Banking networks dimension (full refresh)
 * - Jobs dimension (SCD Type 2 for history tracking)
 * - Date dimension (pre-population)
 * - Users dimension (upsert)
 */
export class DimensionSync {
  private bigQueryClient: BigQueryClient;

  constructor() {
    this.bigQueryClient = new BigQueryClient();
  }

  /**
   * Sync banking networks dimension from configuration
   * Performs full refresh (truncate and reload)
   *
   * Note: Filters out future networks (ACH_NACHA, SWIFT, FedWire, CHIPS)
   * as per the banking networks configuration.
   */
  async syncBankingNetworks(): Promise<void> {
    try {
      console.log('[DimensionSync] Starting banking networks sync...');

      const networks = getAllBankingNetworks();
      console.log(`[DimensionSync] Found ${networks.length} active banking networks (future networks filtered out)`);

      // Transform to BigQuery schema
      const rows = networks.map((network: BankingNetworkSource) => ({
        network_id: network.id,
        network_name: network.name,
        network_type: network.type,
        category: network.category,
        supports_real_time: network.capabilities.realTime,
        supports_batch: network.capabilities.batch,
        supports_webhooks: network.capabilities.webhooks,
        protocols: network.protocols,
        auth_methods: network.authMethods,
        api_endpoint: network.apiEndpoint || null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }));

      // Truncate and reload (full refresh)
      await this.bigQueryClient.query('DELETE FROM `' + process.env.BQ_DATASET + '.dim_banking_networks` WHERE TRUE');
      console.log('[DimensionSync] Truncated dim_banking_networks table');

      if (rows.length > 0) {
        await this.bigQueryClient.batchInsert('dim_banking_networks', rows, 100);
        console.log(`[DimensionSync] Successfully synced ${rows.length} banking networks`);
      } else {
        console.warn('[DimensionSync] No banking networks to sync');
      }
    } catch (error) {
      console.error('[DimensionSync] Failed to sync banking networks:', error);
      throw error;
    }
  }

  /**
   * Upsert job dimension using Slowly Changing Dimension (SCD) Type 2
   * Maintains full history of job configuration changes
   *
   * @param job - RPA job configuration
   */
  async upsertJob(job: RPAJob): Promise<void> {
    try {
      console.log(`[DimensionSync] Upserting job: ${job.id} (${job.name})`);

      // Step 1: Expire current version by setting is_current=FALSE and effective_to=NOW
      await this.bigQueryClient.query(`
        UPDATE \`${process.env.BQ_DATASET}.dim_jobs\`
        SET is_current = FALSE, effective_to = CURRENT_TIMESTAMP()
        WHERE job_id = '${job.id}' AND is_current = TRUE
      `);

      // Step 2: Insert new version with is_current=TRUE
      const newVersion = {
        job_id: job.id,
        job_name: job.name,
        cron_expression: job.schedule.expression,
        timezone: job.schedule.timezone || 'UTC',
        schedule_enabled: job.schedule.enabled !== false,
        banking_network_id: job.source.bankingNetwork?.name || null,
        extraction_method: job.extractionMethod,
        max_retry_attempts: job.retryConfig.maxAttempts,
        backoff_strategy: job.retryConfig.backoffStrategy,
        created_at: job.createdAt.toISOString(),
        updated_at: job.updatedAt.toISOString(),
        deleted_at: null,
        tags: job.tags || [],
        description: job.description || null,
        is_current: true,
        effective_from: new Date().toISOString(),
        effective_to: null,
      };

      await this.bigQueryClient.streamInsert('dim_jobs', [newVersion]);
      console.log(`[DimensionSync] Successfully upserted job dimension for ${job.id}`);
    } catch (error) {
      console.error(`[DimensionSync] Failed to upsert job ${job.id}:`, error);
      throw error;
    }
  }

  /**
   * Upsert user dimension
   * Simple upsert based on user_id
   *
   * @param userId - User ID
   * @param username - Username
   * @param email - Email address
   * @param role - User role
   * @param department - Department (optional)
   */
  async upsertUser(
    userId: string,
    username?: string,
    email?: string,
    role?: string,
    department?: string
  ): Promise<void> {
    try {
      // Check if user exists
      const existingUsers = await this.bigQueryClient.query<{ user_id: string }>(`
        SELECT user_id FROM \`${process.env.BQ_DATASET}.dim_users\`
        WHERE user_id = '${userId}'
        LIMIT 1
      `);

      if (existingUsers.length > 0) {
        // Update existing user
        await this.bigQueryClient.query(`
          UPDATE \`${process.env.BQ_DATASET}.dim_users\`
          SET
            username = '${username || ''}',
            email = '${email || ''}',
            role = '${role || ''}',
            department = '${department || ''}',
            last_login_at = CURRENT_TIMESTAMP()
          WHERE user_id = '${userId}'
        `);
        console.log(`[DimensionSync] Updated user: ${userId}`);
      } else {
        // Insert new user
        const newUser = {
          user_id: userId,
          username: username || null,
          email: email || null,
          role: role || null,
          department: department || null,
          created_at: new Date().toISOString(),
          last_login_at: new Date().toISOString(),
          is_active: true,
        };

        await this.bigQueryClient.streamInsert('dim_users', [newUser]);
        console.log(`[DimensionSync] Inserted new user: ${userId}`);
      }
    } catch (error) {
      console.error(`[DimensionSync] Failed to upsert user ${userId}:`, error);
      throw error;
    }
  }

  /**
   * Generate date dimension table
   * Pre-populates date table with calendar attributes for time intelligence in Power BI
   *
   * @param startYear - Start year (default: 2020)
   * @param endYear - End year (default: 2030)
   */
  async generateDateDimension(startYear: number = 2020, endYear: number = 2030): Promise<void> {
    try {
      console.log(`[DimensionSync] Generating date dimension from ${startYear} to ${endYear}...`);

      const dates: any[] = [];
      const start = new Date(startYear, 0, 1);
      const end = new Date(endYear, 11, 31);

      // Generate all dates in range
      for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
        const currentDate = new Date(d); // Clone date to avoid mutation issues

        dates.push({
          date_key: currentDate.toISOString().split('T')[0],
          year: currentDate.getFullYear(),
          quarter: Math.floor(currentDate.getMonth() / 3) + 1,
          month: currentDate.getMonth() + 1,
          month_name: currentDate.toLocaleString('en-US', { month: 'long' }),
          week: this.getWeekNumber(currentDate),
          day_of_month: currentDate.getDate(),
          day_of_week: currentDate.getDay(),
          day_name: currentDate.toLocaleString('en-US', { weekday: 'long' }),
          is_weekend: currentDate.getDay() === 0 || currentDate.getDay() === 6,
          is_holiday: false, // Default to false, can be updated manually
          fiscal_year: currentDate.getMonth() >= 9 ? currentDate.getFullYear() + 1 : currentDate.getFullYear(),
          fiscal_quarter: Math.floor((currentDate.getMonth() + 3) % 12 / 3) + 1,
        });
      }

      console.log(`[DimensionSync] Generated ${dates.length} date records`);

      // Truncate and reload
      await this.bigQueryClient.query('DELETE FROM `' + process.env.BQ_DATASET + '.dim_date` WHERE TRUE');
      console.log('[DimensionSync] Truncated dim_date table');

      // Insert in batches of 1000
      await this.bigQueryClient.batchInsert('dim_date', dates, 1000);
      console.log(`[DimensionSync] Successfully populated date dimension with ${dates.length} records`);
    } catch (error) {
      console.error('[DimensionSync] Failed to generate date dimension:', error);
      throw error;
    }
  }

  /**
   * Calculate ISO week number for a given date
   * Week 1 is the week containing the first Thursday of the year
   *
   * @param date - Date to calculate week number for
   * @returns ISO week number (1-53)
   */
  private getWeekNumber(date: Date): number {
    // Copy date to avoid mutation
    const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));

    // Set to nearest Thursday: current date + 4 - current day number
    // Make Sunday's day number 7
    const dayNum = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() + 4 - dayNum);

    // Get first day of year
    const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));

    // Calculate full weeks to nearest Thursday
    const weekNumber = Math.ceil((((d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);

    return weekNumber;
  }

  /**
   * Sync all dimensions
   * Convenience method to synchronize all dimension tables
   */
  async syncAll(): Promise<void> {
    console.log('[DimensionSync] Starting full dimension sync...');

    try {
      await this.syncBankingNetworks();
      await this.generateDateDimension();
      console.log('[DimensionSync] Full dimension sync completed successfully');
    } catch (error) {
      console.error('[DimensionSync] Full dimension sync failed:', error);
      throw error;
    }
  }
}
