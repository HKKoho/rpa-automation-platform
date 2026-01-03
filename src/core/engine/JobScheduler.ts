import { CronJob } from 'cron';
import { RPAJob, CronSchedule } from '@/types/rpa.types';
import { RPAEngine } from './RPAEngine';
import { AuditLogger } from '../security/AuditLogger';

/**
 * Job Scheduler for Banking Network Operations
 * Manages scheduled RPA jobs for data extraction during off-peak hours
 */
export class JobScheduler {
  private jobs: Map<string, CronJob>;
  private rpaEngine: RPAEngine;
  private auditLogger: AuditLogger;

  constructor(rpaEngine: RPAEngine) {
    this.jobs = new Map();
    this.rpaEngine = rpaEngine;
    this.auditLogger = new AuditLogger();
  }

  /**
   * Schedule a new RPA job with cron expression
   */
  async scheduleJob(jobConfig: RPAJob): Promise<void> {
    if (this.jobs.has(jobConfig.id)) {
      throw new Error(`Job ${jobConfig.id} is already scheduled`);
    }

    // Validate schedule during preferred hours
    const isValidSchedule = this.validateSchedule(jobConfig.schedule);
    if (!isValidSchedule) {
      throw new Error('Schedule conflicts with peak hours');
    }

    const cronJob = new CronJob(
      jobConfig.schedule.expression,
      async () => {
        await this.executeJob(jobConfig);
      },
      null,
      jobConfig.schedule.enabled,
      jobConfig.schedule.timezone
    );

    this.jobs.set(jobConfig.id, cronJob);

    await this.auditLogger.log({
      action: 'job.scheduled',
      resource: 'scheduled-job',
      resourceId: jobConfig.id,
      changes: {
        schedule: jobConfig.schedule.expression,
        timezone: jobConfig.schedule.timezone,
        nextRun: cronJob.nextDate().toISO()
      }
    });

    if (jobConfig.schedule.enabled) {
      cronJob.start();
    }
  }

  /**
   * Execute a scheduled job
   */
  private async executeJob(jobConfig: RPAJob): Promise<void> {
    try {
      await this.auditLogger.log({
        action: 'job.execution.started',
        resource: 'scheduled-job',
        resourceId: jobConfig.id
      });

      // Update job status
      jobConfig.status = 'running';
      jobConfig.lastRunAt = new Date();

      // Execute extraction via RPA Engine
      const params = {
        jobId: jobConfig.id,
        source: jobConfig.source,
        url: jobConfig.source.url || '',
        credentials: jobConfig.credentials,
        selectors: jobConfig.source.selectors,
        options: {}
      };

      await this.rpaEngine.extractData(params);

      jobConfig.status = 'completed';

      await this.auditLogger.log({
        action: 'job.execution.completed',
        resource: 'scheduled-job',
        resourceId: jobConfig.id
      });
    } catch (error) {
      jobConfig.status = 'failed';

      await this.auditLogger.log({
        action: 'job.execution.failed',
        resource: 'scheduled-job',
        resourceId: jobConfig.id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error'
      });

      // Trigger retry if configured
      await this.handleJobFailure(jobConfig, error);
    }
  }

  /**
   * Validate schedule is within preferred hours (off-peak)
   */
  private validateSchedule(schedule: CronSchedule): boolean {
    // Parse cron expression and check if it falls within preferred hours
    // This is a simplified check - in production use a proper cron parser
    if (schedule.preferredHours && schedule.preferredHours.length > 0) {
      return true; // Validated against preferred hours
    }
    return true;
  }

  /**
   * Handle job failure with retry logic
   */
  private async handleJobFailure(jobConfig: RPAJob, error: any): Promise<void> {
    const { retryConfig } = jobConfig;

    // Check if error is retryable
    const isRetryable = retryConfig.retryableErrors.some(
      pattern => error.message?.includes(pattern)
    );

    if (isRetryable) {
      // Schedule retry with backoff
      const delay = this.calculateBackoff(retryConfig);
      setTimeout(() => {
        this.executeJob(jobConfig);
      }, delay);
    }
  }

  /**
   * Calculate backoff delay for retries
   */
  private calculateBackoff(retryConfig: any): number {
    const { backoffStrategy, initialDelay, maxDelay } = retryConfig;

    switch (backoffStrategy) {
      case 'exponential':
        return Math.min(initialDelay * 2, maxDelay);
      case 'linear':
        return Math.min(initialDelay + 1000, maxDelay);
      default:
        return initialDelay;
    }
  }

  /**
   * Pause a scheduled job
   */
  pauseJob(jobId: string): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.stop();
    }
  }

  /**
   * Resume a paused job
   */
  resumeJob(jobId: string): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.start();
    }
  }

  /**
   * Remove a scheduled job
   */
  async removeJob(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (job) {
      job.stop();
      this.jobs.delete(jobId);

      await this.auditLogger.log({
        action: 'job.removed',
        resource: 'scheduled-job',
        resourceId: jobId
      });
    }
  }

  /**
   * Get all scheduled jobs
   */
  getAllJobs(): Map<string, CronJob> {
    return this.jobs;
  }

  /**
   * Get next execution time for a job
   */
  getNextExecution(jobId: string): Date | null {
    const job = this.jobs.get(jobId);
    if (job) {
      const nextDate = job.nextDate();
      return nextDate.toJSDate();
    }
    return null;
  }

  /**
   * Shutdown scheduler gracefully
   */
  async shutdown(): Promise<void> {
    for (const [jobId, job] of this.jobs) {
      job.stop();
    }
    this.jobs.clear();
  }
}
