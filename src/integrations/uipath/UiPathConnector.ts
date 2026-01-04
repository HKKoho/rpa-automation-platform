/**
 * UiPath Orchestrator API Integration
 *
 * Routes complex web automation jobs to UiPath robots
 * while keeping job scheduling, credentials, and analytics in our platform
 */

import axios, { AxiosInstance } from 'axios';
import { CredentialVault } from '@/core/security/CredentialVault';
import { AuditLogger } from '@/core/security/AuditLogger';

export interface UiPathJobStatus {
  id: string;
  state: 'Pending' | 'Running' | 'Successful' | 'Failed' | 'Stopped';
  outputArguments: Record<string, any>;
  startTime: Date;
  endTime?: Date;
  errorMessage?: string;
}

export interface UiPathProcess {
  name: string;
  releaseKey: string;
  description?: string;
}

/**
 * UiPath Orchestrator Connector
 *
 * Provides integration with UiPath Orchestrator for enterprise-grade
 * robotic process automation with advanced capabilities:
 * - Complex web automation with AI-powered selectors
 * - Document Understanding (invoice, statement parsing)
 * - CAPTCHA solving
 * - Multi-step authentication flows
 * - Banking-specific connectors (SWIFT, ACH, SAP)
 */
export class UiPathConnector {
  private orchestratorUrl: string;
  private tenantName: string;
  private vault: CredentialVault;
  private auditLogger: AuditLogger;
  private client: AxiosInstance;
  private accessToken?: string;
  private tokenExpiry?: Date;

  constructor() {
    this.orchestratorUrl = process.env.UIPATH_ORCHESTRATOR_URL || '';
    this.tenantName = process.env.UIPATH_TENANT_NAME || 'Default';
    this.vault = new CredentialVault();
    this.auditLogger = new AuditLogger();

    this.client = axios.create({
      baseURL: this.orchestratorUrl,
      timeout: 30000,
    });
  }

  /**
   * Authenticate with UiPath Orchestrator using OAuth
   */
  async authenticate(): Promise<void> {
    if (this.accessToken && this.tokenExpiry && this.tokenExpiry > new Date()) {
      return; // Token still valid
    }

    try {
      // Retrieve UiPath credentials from vault
      const creds = await this.vault.retrieve('uipath-orchestrator');

      const response = await this.client.post('/api/Account/Authenticate', {
        tenancyName: this.tenantName,
        usernameOrEmailAddress: creds.username,
        password: creds.password,
      });

      this.accessToken = response.data.result;
      this.tokenExpiry = new Date(Date.now() + 60 * 60 * 1000); // 1 hour

      await this.auditLogger.log({
        action: 'uipath.authenticated',
        resource: 'uipath-orchestrator',
        result: 'success',
      });

      console.log('[UiPathConnector] Successfully authenticated with UiPath Orchestrator');
    } catch (error) {
      await this.auditLogger.log({
        action: 'uipath.authentication.failed',
        resource: 'uipath-orchestrator',
        result: 'failure',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });

      throw new Error(
        `Failed to authenticate with UiPath Orchestrator: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Start a UiPath process (robot job)
   *
   * @param processName - Name of the UiPath process to start
   * @param inputArguments - Input parameters for the process
   * @returns Job ID for tracking
   */
  async startJob(
    processName: string,
    inputArguments: Record<string, any>
  ): Promise<string> {
    await this.authenticate();

    try {
      // Get release key for the process
      const releaseKey = await this.getReleaseKey(processName);

      // Start the job
      const response = await this.client.post(
        '/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs',
        {
          startInfo: {
            ReleaseKey: releaseKey,
            Strategy: 'Specific',
            RobotIds: [], // Auto-select available robot
            JobsCount: 1,
            InputArguments: JSON.stringify(inputArguments),
          },
        },
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
            'Content-Type': 'application/json',
            'X-UIPATH-TenantName': this.tenantName,
          },
        }
      );

      const jobId = response.data.value[0].Key;

      await this.auditLogger.log({
        action: 'uipath.job.started',
        resource: 'uipath-job',
        resourceId: jobId,
        changes: { processName, inputArguments },
      });

      console.log(`[UiPathConnector] Started job ${jobId} for process ${processName}`);

      return jobId;
    } catch (error) {
      await this.auditLogger.log({
        action: 'uipath.job.start.failed',
        resource: 'uipath-process',
        resourceId: processName,
        result: 'failure',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });

      throw new Error(
        `Failed to start UiPath job: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Get job status from UiPath Orchestrator
   *
   * @param jobId - UiPath job ID
   * @returns Current job status with output arguments
   */
  async getJobStatus(jobId: string): Promise<UiPathJobStatus> {
    await this.authenticate();

    try {
      const response = await this.client.get(`/odata/Jobs(${jobId})`, {
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          'X-UIPATH-TenantName': this.tenantName,
        },
      });

      const job = response.data;

      return {
        id: job.Key,
        state: job.State,
        outputArguments: job.OutputArguments ? JSON.parse(job.OutputArguments) : {},
        startTime: new Date(job.StartTime),
        endTime: job.EndTime ? new Date(job.EndTime) : undefined,
        errorMessage: job.Info,
      };
    } catch (error) {
      throw new Error(
        `Failed to get UiPath job status: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Cancel a running job
   *
   * @param jobId - UiPath job ID to cancel
   */
  async cancelJob(jobId: string): Promise<void> {
    await this.authenticate();

    try {
      await this.client.post(
        `/odata/Jobs(${jobId})/UiPath.Server.Configuration.OData.StopJob`,
        { strategy: 'Kill' },
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
            'X-UIPATH-TenantName': this.tenantName,
          },
        }
      );

      await this.auditLogger.log({
        action: 'uipath.job.cancelled',
        resource: 'uipath-job',
        resourceId: jobId,
      });

      console.log(`[UiPathConnector] Cancelled job ${jobId}`);
    } catch (error) {
      throw new Error(
        `Failed to cancel UiPath job: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * List available processes in UiPath Orchestrator
   *
   * @returns Array of available processes
   */
  async listProcesses(): Promise<UiPathProcess[]> {
    await this.authenticate();

    try {
      const response = await this.client.get('/odata/Releases', {
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          'X-UIPATH-TenantName': this.tenantName,
        },
      });

      return response.data.value.map((release: any) => ({
        name: release.ProcessKey,
        releaseKey: release.Key,
        description: release.Description,
      }));
    } catch (error) {
      throw new Error(
        `Failed to list UiPath processes: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Get release key for a process (required to start jobs)
   *
   * @param processName - Name of the process
   * @returns Release key
   */
  private async getReleaseKey(processName: string): Promise<string> {
    await this.authenticate();

    try {
      const response = await this.client.get('/odata/Releases', {
        params: {
          $filter: `ProcessKey eq '${processName}'`,
        },
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          'X-UIPATH-TenantName': this.tenantName,
        },
      });

      if (response.data.value.length === 0) {
        throw new Error(`Process '${processName}' not found in UiPath Orchestrator`);
      }

      return response.data.value[0].Key;
    } catch (error) {
      throw new Error(
        `Failed to get release key: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Wait for job completion with polling
   *
   * @param jobId - Job ID to wait for
   * @param pollIntervalMs - Polling interval in milliseconds (default: 5000)
   * @param timeoutMs - Timeout in milliseconds (default: 600000 - 10 minutes)
   * @returns Final job status
   */
  async waitForCompletion(
    jobId: string,
    pollIntervalMs: number = 5000,
    timeoutMs: number = 600000
  ): Promise<UiPathJobStatus> {
    const startTime = Date.now();

    while (true) {
      const status = await this.getJobStatus(jobId);

      // Check if job is in terminal state
      if (
        status.state === 'Successful' ||
        status.state === 'Failed' ||
        status.state === 'Stopped'
      ) {
        return status;
      }

      // Check timeout
      if (Date.now() - startTime > timeoutMs) {
        throw new Error(`Job ${jobId} timed out after ${timeoutMs}ms`);
      }

      // Wait before next poll
      await this.sleep(pollIntervalMs);
    }
  }

  /**
   * Sleep utility for polling
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
