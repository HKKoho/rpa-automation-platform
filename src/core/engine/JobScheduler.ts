// Vercel-compatible stub version
// For full implementation with cron, see JobScheduler.local.ts

import type { RPAJob, CronSchedule } from '@/types/rpa.types';
import type { RPAEngine } from './RPAEngine';
import { AuditLogger } from '../security/AuditLogger';

/**
 * Job Scheduler Stub for Vercel Deployment
 *
 * This is a placeholder that allows the documentation site to build on Vercel.
 * For local development with full cron functionality, use package.local.json
 * which includes cron dependency.
 *
 * Production deployment requires:
 * - Vercel Cron (configured in vercel.json)
 * - Or external scheduling service (Upstash QStash, Cronitor)
 */
export class JobScheduler {
  private jobs: Map<string, any>;
  private rpaEngine: RPAEngine;
  private auditLogger: AuditLogger;

  constructor(rpaEngine: RPAEngine) {
    this.jobs = new Map();
    this.rpaEngine = rpaEngine;
    this.auditLogger = new AuditLogger();
    console.warn('JobScheduler stub loaded - cron functionality not available on Vercel');
  }

  /**
   * Schedule a new RPA job with cron expression
   */
  async scheduleJob(jobConfig: RPAJob): Promise<void> {
    throw new Error(
      'Job scheduling not implemented on Vercel. Use Vercel Cron or external scheduling service.'
    );
  }

  pauseJob(jobId: string): void {
    // No-op
  }

  resumeJob(jobId: string): void {
    // No-op
  }

  async removeJob(jobId: string): Promise<void> {
    this.jobs.delete(jobId);
  }

  getAllJobs(): Map<string, any> {
    return this.jobs;
  }

  getNextExecution(jobId: string): Date | null {
    return null;
  }

  async shutdown(): Promise<void> {
    this.jobs.clear();
  }
}
