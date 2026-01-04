import { EventEmitter } from 'events';
// Vercel stub: bullmq removed (see QueueManager.ts for stub implementation)
import { RPAJob, ExtractedData } from '@/types/rpa.types';
import { WebAutomation } from '../extraction/WebAutomation';
import { APIExtractor } from '../extraction/APIExtractor';
import { QueueManager } from './QueueManager';
import { AuditLogger } from '../security/AuditLogger';
import { StorageManager } from '../pipeline/StorageManager';
import { UiPathConnector } from '@/integrations/uipath/UiPathConnector';
import { RobocorpConnector } from '@/integrations/robocorp/RobocorpConnector';

/**
 * Core RPA Engine for Banking Network Utility Operations
 * Handles job scheduling, execution, and data extraction from banking networks
 */
export class RPAEngine extends EventEmitter {
  private queueManager: QueueManager;
  private auditLogger: AuditLogger;
  private storage: StorageManager;
  private extractors: Map<string, any>;
  private uiPathConnector?: UiPathConnector;
  private robocorpConnector?: RobocorpConnector;

  constructor() {
    super();
    this.queueManager = new QueueManager();
    this.auditLogger = new AuditLogger();
    this.storage = new StorageManager();
    this.extractors = new Map();

    // Initialize external RPA connectors if configured
    if (process.env.UIPATH_ORCHESTRATOR_URL) {
      this.uiPathConnector = new UiPathConnector();
    }
    if (process.env.ROBOCORP_WORKSPACE_ID) {
      this.robocorpConnector = new RobocorpConnector();
    }

    // Register extractors
    this.extractors.set('web-automation', WebAutomation);
    this.extractors.set('api', APIExtractor);
  }

  /**
   * Schedule a new RPA job for banking data extraction
   */
  async scheduleJob(jobConfig: RPAJob): Promise<string> {
    try {
      // Log job scheduling
      await this.auditLogger.log({
        action: 'job.schedule',
        resource: 'rpa-job',
        resourceId: jobConfig.id,
        changes: { name: jobConfig.name, schedule: jobConfig.schedule }
      });

      // Add job to queue
      const job = await this.queueManager.addJob(
        'rpa-jobs',
        jobConfig.name,
        jobConfig,
        {
          repeat: {
            pattern: jobConfig.schedule.expression,
            tz: jobConfig.schedule.timezone,
          },
          removeOnComplete: false,
          removeOnFail: false,
          attempts: jobConfig.retryConfig.maxAttempts,
          backoff: {
            type: jobConfig.retryConfig.backoffStrategy,
            delay: jobConfig.retryConfig.initialDelay,
          },
        }
      );

      this.emit('job:scheduled', { jobId: job.id, config: jobConfig });
      return job.id as string;
    } catch (error) {
      await this.auditLogger.log({
        action: 'job.schedule.failed',
        resource: 'rpa-job',
        resourceId: jobConfig.id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error'
      });
      throw error;
    }
  }

  /**
   * Extract data from banking network using configured method
   */
  async extractData(params: ExtractionParams): Promise<ExtractedData> {
    const startTime = Date.now();
    const executionId = this.generateExecutionId();
    const startedAt = new Date();

    try {
      // Decide whether to delegate to external RPA platform
      const delegationDecision = this.decideDelegation(params);

      if (delegationDecision.shouldDelegate) {
        return await this.extractViaExternalPlatform(
          delegationDecision.platform!,
          params,
          executionId,
          startTime,
          startedAt
        );
      }

      // Use internal automation
      const Extractor = this.selectExtractor(params.source);
      const extractor = new Extractor(params.options || {});

      // Perform extraction
      this.emit('extraction:started', { jobId: params.jobId, source: params.source });
      const data = await extractor.extract(params);

      // Validate extracted data
      const validationResult = await this.validateData(data);
      data.validationStatus = validationResult;

      // Stage data for ETL pipeline
      await this.stageData(data);

      const duration = Date.now() - startTime;
      const completedAt = new Date();

      // Log successful extraction
      await this.auditLogger.log({
        action: 'data.extraction.success',
        resource: 'extracted-data',
        resourceId: data.jobId,
        changes: {
          duration,
          recordCount: data.metadata.recordCount,
          dataSize: data.dataSize
        }
      });

      // Track job execution in warehouse
      await this.storage.loadJobExecution({
        executionId,
        jobId: params.jobId,
        bankingNetworkId: params.source.bankingNetwork?.name,
        status: 'success',
        extractionMethod: params.source.extractionMethod || 'web-automation',
        startedAt,
        completedAt,
        durationMs: duration,
        recordsExtracted: data.metadata.recordCount,
        recordsProcessed: data.metadata.recordCount,
        errorCount: 0,
        dataSizeBytes: data.dataSize,
        extractionDurationMs: duration,
      });

      this.emit('extraction:completed', { jobId: params.jobId, data });
      return data;
    } catch (error) {
      const duration = Date.now() - startTime;
      const completedAt = new Date();

      await this.auditLogger.log({
        action: 'data.extraction.failed',
        resource: 'extracted-data',
        resourceId: params.jobId,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        changes: { duration }
      });

      // Track failed execution in warehouse
      await this.storage.loadJobExecution({
        executionId,
        jobId: params.jobId,
        bankingNetworkId: params.source.bankingNetwork?.name,
        status: 'failed',
        extractionMethod: params.source.extractionMethod || 'web-automation',
        startedAt,
        completedAt,
        durationMs: duration,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        errorCount: 1,
      });

      this.emit('extraction:failed', { jobId: params.jobId, error });
      throw error;
    }
  }

  /**
   * Generate unique execution ID
   */
  private generateExecutionId(): string {
    return `exec-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Select appropriate extractor based on source configuration
   */
  private selectExtractor(source: any): any {
    const extractionMethod = source.extractionMethod || 'web-automation';
    const Extractor = this.extractors.get(extractionMethod);

    if (!Extractor) {
      throw new Error(`Unsupported extraction method: ${extractionMethod}`);
    }

    return Extractor;
  }

  /**
   * Validate extracted data against banking network schemas
   */
  private async validateData(data: ExtractedData): Promise<any> {
    // Implement validation logic
    const errors: any[] = [];
    const warnings: any[] = [];

    // Basic validation checks
    if (!data.rawData || Object.keys(data.rawData).length === 0) {
      errors.push({
        field: 'rawData',
        message: 'Extracted data is empty',
        severity: 'critical',
        code: 'EMPTY_DATA'
      });
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
      validatedAt: new Date(),
      validationRules: ['schema-check', 'data-completeness', 'format-validation']
    };
  }

  /**
   * Stage data in temporary storage for ETL processing
   */
  private async stageData(data: ExtractedData): Promise<void> {
    // Implementation for staging data
    this.emit('data:staged', { jobId: data.jobId });
  }

  /**
   * Get job status and execution history
   */
  async getJobStatus(jobId: string): Promise<any> {
    return await this.queueManager.getJobStatus(jobId);
  }

  /**
   * Cancel a running job
   */
  async cancelJob(jobId: string): Promise<void> {
    await this.queueManager.cancelJob(jobId);
    await this.auditLogger.log({
      action: 'job.cancelled',
      resource: 'rpa-job',
      resourceId: jobId
    });
  }

  /**
   * Decide whether to delegate job to external RPA platform
   */
  private decideDelegation(params: ExtractionParams): {
    shouldDelegate: boolean;
    platform?: 'uipath' | 'robocorp';
    reason?: string;
  } {
    // Check if force delegation is specified
    if (params.options?.forceDelegate) {
      return {
        shouldDelegate: true,
        platform: params.options.forceDelegate,
        reason: 'Force delegation requested',
      };
    }

    // Decide based on complexity and capabilities
    const complexityScore = this.calculateComplexityScore(params);

    // Delegate to UiPath for complex enterprise jobs (score > 7)
    if (complexityScore > 7 && this.uiPathConnector) {
      return {
        shouldDelegate: true,
        platform: 'uipath',
        reason: `High complexity (score: ${complexityScore})`,
      };
    }

    // Delegate to Robocorp for medium complexity jobs (score 4-7)
    if (complexityScore >= 4 && complexityScore <= 7 && this.robocorpConnector) {
      return {
        shouldDelegate: true,
        platform: 'robocorp',
        reason: `Medium complexity (score: ${complexityScore})`,
      };
    }

    // Use internal automation for simple jobs
    return {
      shouldDelegate: false,
      reason: `Low complexity (score: ${complexityScore}), using internal automation`,
    };
  }

  /**
   * Calculate job complexity score (0-10)
   */
  private calculateComplexityScore(params: ExtractionParams): number {
    let score = 0;

    // Complex web automation indicators
    if (params.source.selectors && Object.keys(params.source.selectors).length > 10) {
      score += 3;
    }
    if (params.source.requiresCaptcha) score += 3;
    if (params.source.requiresMFA) score += 2;
    if (params.source.documentProcessing) score += 4;
    if (params.source.multiPageWorkflow) score += 2;
    if (params.source.dynamicElements) score += 2;

    // Banking complexity indicators
    if (params.source.bankingNetwork?.type === 'clearinghouse') score += 1;
    if (params.source.bankingNetwork?.protocol === 'ISO20022') score += 1;

    return Math.min(score, 10);
  }

  /**
   * Extract data via external RPA platform
   */
  private async extractViaExternalPlatform(
    platform: 'uipath' | 'robocorp',
    params: ExtractionParams,
    executionId: string,
    startTime: number,
    startedAt: Date
  ): Promise<ExtractedData> {
    const connector = platform === 'uipath' ? this.uiPathConnector : this.robocorpConnector;

    if (!connector) {
      throw new Error(`${platform} connector not configured`);
    }

    this.emit('extraction:delegated', {
      jobId: params.jobId,
      platform,
    });

    try {
      // Prepare input arguments
      const inputArgs = {
        sourceUrl: params.source.url,
        accountNumbers: params.source.accountIdentifiers,
        credentialsId: params.credentials?.id,
        extractionType: params.source.extractionMethod,
        bankingNetwork: params.source.bankingNetwork?.name,
      };

      // Start job on external platform
      const externalJobId =
        platform === 'uipath'
          ? await (connector as UiPathConnector).startJob('BankingDataExtraction', inputArgs)
          : await (connector as RobocorpConnector).startRun(
              process.env.ROBOCORP_PROCESS_ID || '',
              inputArgs
            );

      // Wait for completion
      const status =
        platform === 'uipath'
          ? await (connector as UiPathConnector).waitForCompletion(externalJobId)
          : await (connector as RobocorpConnector).waitForCompletion(externalJobId);

      // Check for failure
      if (
        (platform === 'uipath' && (status as any).state !== 'Successful') ||
        (platform === 'robocorp' && (status as any).state !== 'COMPLETED')
      ) {
        throw new Error(`${platform} job failed: ${status.errorMessage}`);
      }

      // Extract results
      const outputData =
        platform === 'uipath'
          ? (status as any).outputArguments
          : (status as any).outputData;

      const duration = Date.now() - startTime;
      const completedAt = new Date();

      // Build extracted data
      const extractedData: ExtractedData = {
        jobId: params.jobId,
        timestamp: new Date(),
        rawData: outputData.ExtractedData || outputData.extractedData || {},
        metadata: {
          source: platform,
          extractionDuration: duration,
          recordCount: outputData.RecordCount || outputData.recordCount || 0,
          checksumHash: outputData.ChecksumHash || outputData.checksumHash || '',
          compressionUsed: false,
        },
        validationStatus: {
          isValid: true,
          errors: [],
          warnings: [],
          validatedAt: new Date(),
          validationRules: [],
        },
        dataSize: JSON.stringify(outputData).length,
      };

      // Track in warehouse
      await this.storage.loadJobExecution({
        executionId,
        jobId: params.jobId,
        bankingNetworkId: params.source.bankingNetwork?.name,
        status: 'success',
        extractionMethod: platform,
        startedAt,
        completedAt,
        durationMs: duration,
        recordsExtracted: extractedData.metadata.recordCount,
        recordsProcessed: extractedData.metadata.recordCount,
        errorCount: 0,
        dataSizeBytes: extractedData.dataSize,
        extractionDurationMs: duration,
      });

      this.emit('extraction:completed', { jobId: params.jobId, data: extractedData });
      return extractedData;
    } catch (error) {
      const duration = Date.now() - startTime;
      const completedAt = new Date();

      // Track failed execution
      await this.storage.loadJobExecution({
        executionId,
        jobId: params.jobId,
        bankingNetworkId: params.source.bankingNetwork?.name,
        status: 'failed',
        extractionMethod: platform,
        startedAt,
        completedAt,
        durationMs: duration,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        errorCount: 1,
      });

      this.emit('extraction:failed', { jobId: params.jobId, error });
      throw error;
    }
  }

  /**
   * Graceful shutdown
   */
  async shutdown(): Promise<void> {
    await this.queueManager.shutdown();
    this.removeAllListeners();
  }
}

// Additional type definitions
interface ExtractionParams {
  jobId: string;
  source: any;
  url: string;
  credentials: any;
  selectors?: any;
  options?: any;
}
