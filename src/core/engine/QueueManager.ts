// Vercel-compatible stub version
// For full implementation with BullMQ and Redis, see QueueManager.local.ts

import type { QueueJob, WorkerConfig } from '@/types/rpa.types';

/**
 * Queue Manager Stub for Vercel Deployment
 *
 * This is a placeholder that allows the documentation site to build on Vercel.
 * For local development with full queue functionality, use package.local.json
 * which includes bullmq and ioredis dependencies.
 *
 * Production deployment requires:
 * - @vercel/kv for Redis-compatible storage
 * - Vercel Cron for scheduled jobs
 * - Or external queue service (Upstash, QStash)
 */
export class QueueManager {
  private queues: Map<string, any>;
  private workers: Map<string, any>;

  constructor() {
    this.queues = new Map();
    this.workers = new Map();
    console.warn('QueueManager stub loaded - queue functionality not available on Vercel');
  }

  createQueue(queueName: string): any {
    throw new Error('Queue functionality not implemented on Vercel. Use @vercel/kv or external queue service.');
  }

  async addJob<T = any>(
    queueName: string,
    jobName: string,
    data: T,
    options?: any
  ): Promise<any> {
    throw new Error('Queue functionality not implemented on Vercel. Use @vercel/kv or external queue service.');
  }

  createWorker(
    queueName: string,
    processor: (job: any) => Promise<any>,
    config?: WorkerConfig
  ): any {
    throw new Error('Worker functionality not implemented on Vercel. Use Vercel Cron or external workers.');
  }

  async getJobStatus(jobId: string): Promise<any> {
    return null;
  }

  async cancelJob(jobId: string): Promise<void> {
    // No-op
  }

  async getQueueStats(queueName: string): Promise<any> {
    return {
      queueName,
      waiting: 0,
      active: 0,
      completed: 0,
      failed: 0,
      delayed: 0,
      total: 0,
    };
  }

  async pauseQueue(queueName: string): Promise<void> {
    // No-op
  }

  async resumeQueue(queueName: string): Promise<void> {
    // No-op
  }

  async cleanQueue(queueName: string, grace: number = 86400000): Promise<void> {
    // No-op
  }

  async shutdown(): Promise<void> {
    this.workers.clear();
    this.queues.clear();
  }
}
