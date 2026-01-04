import { EventEmitter } from 'events';
import { ExtractedData, ETLJob, TransformationRule } from '@/types/rpa.types';
import { DataValidator } from './DataValidator';
import { Transformer } from './Transformer';
import { StorageManager } from './StorageManager';
import { AuditLogger } from '../security/AuditLogger';

/**
 * ETL Pipeline for Banking Network Data Processing
 * Handles Extract, Transform, Load operations for banking transactions
 */
export class ETLPipeline extends EventEmitter {
  private validator: DataValidator;
  private transformer: Transformer;
  private storage: StorageManager;
  private auditLogger: AuditLogger;

  constructor() {
    super();
    this.validator = new DataValidator();
    this.transformer = new Transformer();
    this.storage = new StorageManager();
    this.auditLogger = new AuditLogger();
  }

  /**
   * Process extracted data through ETL pipeline
   */
  async process(data: ExtractedData): Promise<ETLJob> {
    const etlJob: ETLJob = {
      id: this.generateJobId(),
      extractionJobId: data.jobId,
      stage: 'extract',
      status: 'processing',
      startedAt: new Date(),
      recordsProcessed: 0,
      errorCount: 0,
    };

    try {
      // Stage 1: Validate
      this.emit('pipeline:started', { jobId: etlJob.id, stage: 'validate' });
      etlJob.stage = 'validate';

      const validationResult = await this.validator.validate(data.rawData);

      if (!validationResult.isValid && validationResult.errors.some(e => e.severity === 'critical')) {
        throw new Error('Critical validation errors found');
      }

      await this.auditLogger.log({
        action: 'etl.validation.completed',
        resource: 'etl-job',
        resourceId: etlJob.id,
        changes: {
          errors: validationResult.errors.length,
          warnings: validationResult.warnings.length,
        },
      });

      // Stage 2: Transform
      this.emit('pipeline:stage-change', { jobId: etlJob.id, stage: 'transform' });
      etlJob.stage = 'transform';

      const transformedData = await this.transformer.transform(
        data.rawData,
        this.getTransformationRules()
      );

      await this.auditLogger.log({
        action: 'etl.transformation.completed',
        resource: 'etl-job',
        resourceId: etlJob.id,
        changes: {
          recordsTransformed: transformedData.length,
        },
      });

      // Stage 3: Load
      this.emit('pipeline:stage-change', { jobId: etlJob.id, stage: 'load' });
      etlJob.stage = 'load';

      const loadResult = await this.storage.load(transformedData, {
        destination: 'warehouse',
        batchSize: 1000,
      });

      etlJob.recordsProcessed = loadResult.recordsLoaded;
      etlJob.errorCount = loadResult.errors;
      etlJob.completedAt = new Date();
      etlJob.status = 'completed';

      await this.auditLogger.log({
        action: 'etl.pipeline.completed',
        resource: 'etl-job',
        resourceId: etlJob.id,
        changes: {
          recordsProcessed: etlJob.recordsProcessed,
          errorCount: etlJob.errorCount,
          duration: etlJob.completedAt.getTime() - etlJob.startedAt!.getTime(),
        },
      });

      // Persist ETL job metadata to warehouse
      await this.storage.loadETLJob(etlJob);

      this.emit('pipeline:completed', etlJob);
      return etlJob;
    } catch (error) {
      etlJob.status = 'failed';
      etlJob.completedAt = new Date();

      await this.auditLogger.log({
        action: 'etl.pipeline.failed',
        resource: 'etl-job',
        resourceId: etlJob.id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });

      // Persist failed ETL job metadata to warehouse
      await this.storage.loadETLJob(etlJob);

      this.emit('pipeline:failed', { jobId: etlJob.id, error });
      throw error;
    }
  }

  /**
   * Process data in batches for large datasets
   */
  async processBatch(
    dataArray: ExtractedData[],
    options?: BatchOptions
  ): Promise<ETLJob[]> {
    const batchSize = options?.batchSize || 10;
    const results: ETLJob[] = [];

    for (let i = 0; i < dataArray.length; i += batchSize) {
      const batch = dataArray.slice(i, i + batchSize);

      const batchPromises = batch.map((data) => this.process(data));
      const batchResults = await Promise.allSettled(batchPromises);

      batchResults.forEach((result) => {
        if (result.status === 'fulfilled') {
          results.push(result.value);
        }
      });

      // Progress update
      this.emit('pipeline:batch-progress', {
        processed: i + batch.length,
        total: dataArray.length,
      });
    }

    return results;
  }

  /**
   * Get transformation rules for banking data
   */
  private getTransformationRules(): TransformationRule[] {
    return [
      {
        id: 'normalize-currency',
        name: 'Normalize Currency Format',
        inputFields: ['amount'],
        outputFields: ['normalizedAmount'],
        transformationType: 'mapping',
        expression: 'parseFloat(value.replace(/[^0-9.-]/g, ""))',
      },
      {
        id: 'standardize-dates',
        name: 'Standardize Date Format',
        inputFields: ['date', 'timestamp'],
        outputFields: ['standardizedDate'],
        transformationType: 'mapping',
        expression: 'new Date(value).toISOString()',
      },
      {
        id: 'enrich-account-data',
        name: 'Enrich Account Information',
        inputFields: ['accountNumber'],
        outputFields: ['accountType', 'bankName'],
        transformationType: 'enrichment',
        expression: 'lookupAccountDetails(value)',
      },
      {
        id: 'calculate-totals',
        name: 'Calculate Transaction Totals',
        inputFields: ['amount', 'fee'],
        outputFields: ['totalAmount'],
        transformationType: 'calculation',
        expression: 'parseFloat(amount) + parseFloat(fee)',
      },
    ];
  }

  /**
   * Generate unique ETL job ID
   */
  private generateJobId(): string {
    return `etl-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Get pipeline statistics
   */
  async getStatistics(): Promise<PipelineStatistics> {
    // Implementation would query database for actual stats
    return {
      totalJobsProcessed: 0,
      successfulJobs: 0,
      failedJobs: 0,
      averageProcessingTime: 0,
      totalRecordsProcessed: 0,
    };
  }
}

// Types
interface BatchOptions {
  batchSize?: number;
  concurrent?: boolean;
}

interface PipelineStatistics {
  totalJobsProcessed: number;
  successfulJobs: number;
  failedJobs: number;
  averageProcessingTime: number;
  totalRecordsProcessed: number;
}
