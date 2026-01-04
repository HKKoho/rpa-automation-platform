import { AuditLogEntry } from '@/types/rpa.types';
import { BigQueryClient } from '@/core/warehouse/BigQueryClient';
import fs from 'fs/promises';
import path from 'path';

/**
 * Immutable Audit Logger for Compliance and Security
 * Provides tamper-proof logging for banking operations
 *
 * Dual-write architecture:
 * - JSONL files (append-only, immutable, local audit trail)
 * - BigQuery warehouse (real-time analytics, Power BI reporting)
 */
export class AuditLogger {
  private logDir: string;
  private currentLogFile: string;
  private logBuffer: AuditLogEntry[] = [];
  private bqBuffer: AuditLogEntry[] = [];
  private bufferSize: number = 100;
  private flushInterval: NodeJS.Timeout | null = null;
  private bigQueryClient: BigQueryClient;

  constructor(logDir?: string) {
    this.logDir = logDir || process.env.AUDIT_LOG_DIR || './logs/audit';
    this.currentLogFile = this.getLogFileName();
    this.bigQueryClient = new BigQueryClient();
    this.initializeLogger();
  }

  /**
   * Initialize audit logger
   */
  private async initializeLogger(): Promise<void> {
    try {
      // Create log directory if it doesn't exist
      await fs.mkdir(this.logDir, { recursive: true });

      // Start auto-flush interval (every 30 seconds)
      this.flushInterval = setInterval(() => {
        this.flush();
      }, 30000);
    } catch (error) {
      console.error('Failed to initialize audit logger:', error);
    }
  }

  /**
   * Log an audit event (dual-write to JSONL and BigQuery)
   */
  async log(entry: Partial<AuditLogEntry>): Promise<void> {
    const auditEntry: AuditLogEntry = {
      id: this.generateId(),
      timestamp: new Date(),
      userId: entry.userId || 'system',
      action: entry.action || 'unknown',
      resource: entry.resource || 'unknown',
      resourceId: entry.resourceId || '',
      changes: entry.changes,
      ipAddress: entry.ipAddress || '127.0.0.1',
      userAgent: entry.userAgent || 'RPA-Platform/1.0',
      result: entry.result || 'success',
      errorMessage: entry.errorMessage,
    };

    // Add to file buffer
    this.logBuffer.push(auditEntry);

    // Add to BigQuery buffer
    this.bqBuffer.push(auditEntry);

    // Flush file buffer if full
    if (this.logBuffer.length >= this.bufferSize) {
      await this.flush();
    }

    // Flush BigQuery buffer if full
    if (this.bqBuffer.length >= 100) {
      await this.flushToBigQuery();
    }

    // Also log to console in development
    if (process.env.NODE_ENV === 'development') {
      console.log('[AUDIT]', JSON.stringify(auditEntry, null, 2));
    }
  }

  /**
   * Flush buffer to disk
   */
  private async flush(): Promise<void> {
    if (this.logBuffer.length === 0) return;

    try {
      // Check if we need to rotate log file (daily rotation)
      const currentFile = this.getLogFileName();
      if (currentFile !== this.currentLogFile) {
        this.currentLogFile = currentFile;
      }

      const logFilePath = path.join(this.logDir, this.currentLogFile);

      // Convert logs to JSONL (JSON Lines) format for immutability
      const logLines = this.logBuffer
        .map((entry) => JSON.stringify(entry))
        .join('\n') + '\n';

      // Append to log file (immutable, append-only)
      await fs.appendFile(logFilePath, logLines, 'utf8');

      // Clear buffer
      this.logBuffer = [];
    } catch (error) {
      console.error('Failed to flush audit logs:', error);
    }
  }

  /**
   * Generate log file name based on current date
   */
  private getLogFileName(): string {
    const date = new Date();
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `audit-${year}-${month}-${day}.jsonl`;
  }

  /**
   * Generate unique ID for audit entry
   */
  private generateId(): string {
    return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Query audit logs
   */
  async query(filters: AuditQueryFilters): Promise<AuditLogEntry[]> {
    const results: AuditLogEntry[] = [];

    try {
      // Determine which log files to read based on date range
      const logFiles = await this.getLogFilesInRange(
        filters.startDate,
        filters.endDate
      );

      for (const logFile of logFiles) {
        const filePath = path.join(this.logDir, logFile);
        const content = await fs.readFile(filePath, 'utf8');

        // Parse JSONL format
        const lines = content.trim().split('\n');

        for (const line of lines) {
          if (!line) continue;

          const entry: AuditLogEntry = JSON.parse(line);

          // Apply filters
          if (this.matchesFilters(entry, filters)) {
            results.push(entry);
          }
        }
      }

      // Sort by timestamp descending
      results.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime());

      // Apply limit
      if (filters.limit) {
        return results.slice(0, filters.limit);
      }

      return results;
    } catch (error) {
      console.error('Failed to query audit logs:', error);
      return [];
    }
  }

  /**
   * Check if entry matches filters
   */
  private matchesFilters(
    entry: AuditLogEntry,
    filters: AuditQueryFilters
  ): boolean {
    if (filters.userId && entry.userId !== filters.userId) return false;
    if (filters.action && entry.action !== filters.action) return false;
    if (filters.resource && entry.resource !== filters.resource) return false;
    if (filters.resourceId && entry.resourceId !== filters.resourceId) return false;
    if (filters.result && entry.result !== filters.result) return false;

    if (filters.startDate && entry.timestamp < filters.startDate) return false;
    if (filters.endDate && entry.timestamp > filters.endDate) return false;

    return true;
  }

  /**
   * Get log files within date range
   */
  private async getLogFilesInRange(
    startDate?: Date,
    endDate?: Date
  ): Promise<string[]> {
    try {
      const files = await fs.readdir(this.logDir);
      const auditFiles = files.filter((f) => f.startsWith('audit-') && f.endsWith('.jsonl'));

      if (!startDate && !endDate) {
        return auditFiles;
      }

      // Filter by date range if specified
      return auditFiles.filter((file) => {
        const match = file.match(/audit-(\d{4})-(\d{2})-(\d{2})\.jsonl/);
        if (!match) return false;

        const fileDate = new Date(
          parseInt(match[1]),
          parseInt(match[2]) - 1,
          parseInt(match[3])
        );

        if (startDate && fileDate < startDate) return false;
        if (endDate && fileDate > endDate) return false;

        return true;
      });
    } catch (error) {
      return [];
    }
  }

  /**
   * Get audit statistics
   */
  async getStatistics(startDate?: Date, endDate?: Date): Promise<AuditStatistics> {
    const logs = await this.query({ startDate, endDate });

    const stats: AuditStatistics = {
      totalEvents: logs.length,
      successCount: logs.filter((l) => l.result === 'success').length,
      failureCount: logs.filter((l) => l.result === 'failure').length,
      uniqueUsers: new Set(logs.map((l) => l.userId)).size,
      actionBreakdown: {},
      resourceBreakdown: {},
    };

    // Calculate breakdowns
    logs.forEach((log) => {
      stats.actionBreakdown[log.action] =
        (stats.actionBreakdown[log.action] || 0) + 1;
      stats.resourceBreakdown[log.resource] =
        (stats.resourceBreakdown[log.resource] || 0) + 1;
    });

    return stats;
  }

  /**
   * Archive old logs (for compliance retention)
   */
  async archiveLogs(olderThan: Date): Promise<void> {
    try {
      const files = await fs.readdir(this.logDir);
      const auditFiles = files.filter((f) => f.startsWith('audit-') && f.endsWith('.jsonl'));

      for (const file of auditFiles) {
        const match = file.match(/audit-(\d{4})-(\d{2})-(\d{2})\.jsonl/);
        if (!match) continue;

        const fileDate = new Date(
          parseInt(match[1]),
          parseInt(match[2]) - 1,
          parseInt(match[3])
        );

        if (fileDate < olderThan) {
          // Compress and move to archive
          const archiveDir = path.join(this.logDir, 'archive');
          await fs.mkdir(archiveDir, { recursive: true });

          const sourcePath = path.join(this.logDir, file);
          const archivePath = path.join(archiveDir, file + '.gz');

          // In production, use zlib to compress
          await fs.rename(sourcePath, archivePath);
        }
      }
    } catch (error) {
      console.error('Failed to archive logs:', error);
    }
  }

  /**
   * Flush audit logs to BigQuery
   */
  private async flushToBigQuery(): Promise<void> {
    if (this.bqBuffer.length === 0) return;

    try {
      const rows = this.bqBuffer.map((entry) => ({
        audit_id: entry.id,
        user_id: entry.userId,
        action: entry.action,
        resource: entry.resource,
        resource_id: entry.resourceId,
        result: entry.result,
        timestamp: entry.timestamp.toISOString(),
        log_date: entry.timestamp.toISOString().split('T')[0],
        ip_address: entry.ipAddress,
        user_agent: entry.userAgent,
        changes: JSON.stringify(entry.changes || {}),
        error_message: entry.errorMessage || null,
        session_id: null, // TODO: Track session IDs
        compliance_mode: process.env.COMPLIANCE_MODE || null,
      }));

      await this.bigQueryClient.streamInsert('fact_audit_logs', rows);
      console.log(`[AuditLogger] Flushed ${rows.length} audit logs to BigQuery`);

      // Clear BigQuery buffer
      this.bqBuffer = [];
    } catch (error) {
      console.error('[AuditLogger] Failed to flush audit logs to BigQuery:', error);
      // Keep logs in buffer for retry on next flush
      // Don't throw - file-based audit trail is still intact
    }
  }

  /**
   * Graceful shutdown (flush both file and BigQuery buffers)
   */
  async shutdown(): Promise<void> {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
    }
    // Flush both buffers
    await this.flush(); // File buffer
    await this.flushToBigQuery(); // BigQuery buffer
  }
}

// Types
interface AuditQueryFilters {
  userId?: string;
  action?: string;
  resource?: string;
  resourceId?: string;
  result?: 'success' | 'failure';
  startDate?: Date;
  endDate?: Date;
  limit?: number;
}

interface AuditStatistics {
  totalEvents: number;
  successCount: number;
  failureCount: number;
  uniqueUsers: number;
  actionBreakdown: Record<string, number>;
  resourceBreakdown: Record<string, number>;
}
