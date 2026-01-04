/**
 * Robocorp Cloud API Integration
 *
 * Manages robot execution in Robocorp Cloud for cost-effective,
 * developer-friendly automation with Python Robot Framework
 */

import axios, { AxiosInstance } from 'axios';
import { CredentialVault } from '@/core/security/CredentialVault';
import { AuditLogger } from '@/core/security/AuditLogger';

export interface RobocorpRunStatus {
  id: string;
  state: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';
  startedAt: Date;
  completedAt?: Date;
  outputData: Record<string, any>;
  errorMessage?: string;
}

export interface RobocorpProcess {
  id: string;
  name: string;
  description?: string;
}

export interface RobocorpWebhookConfig {
  url: string;
  events: string[];
  secret?: string;
}

/**
 * Robocorp Cloud Connector
 *
 * Provides integration with Robocorp Cloud for modern, cloud-native
 * automation with capabilities:
 * - Python Robot Framework automation
 * - API-first architecture with webhooks
 * - Container-based workers (Docker/Kubernetes)
 * - Cost-effective (free tier: 5,000 runs/month)
 * - Developer-friendly (Git-native, CI/CD ready)
 */
export class RobocorpConnector {
  private apiUrl: string;
  private workspaceId: string;
  private vault: CredentialVault;
  private auditLogger: AuditLogger;
  private client: AxiosInstance;
  private apiKey?: string;

  constructor() {
    this.apiUrl = process.env.ROBOCORP_API_URL || 'https://api.eu1.robocorp.com';
    this.workspaceId = process.env.ROBOCORP_WORKSPACE_ID || '';
    this.vault = new CredentialVault();
    this.auditLogger = new AuditLogger();

    this.client = axios.create({
      baseURL: this.apiUrl,
      timeout: 30000,
    });
  }

  /**
   * Initialize API key from vault
   */
  private async getApiKey(): Promise<string> {
    if (this.apiKey) {
      return this.apiKey;
    }

    try {
      const creds = await this.vault.retrieve('robocorp-api-key');
      this.apiKey = creds.key || creds.token || '';

      if (!this.apiKey) {
        throw new Error('Robocorp API key not found in credential vault');
      }

      return this.apiKey;
    } catch (error) {
      throw new Error(
        `Failed to retrieve Robocorp API key: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Get authorization headers
   */
  private async getAuthHeaders(): Promise<Record<string, string>> {
    const apiKey = await this.getApiKey();
    return {
      Authorization: `RC-WSKEY ${apiKey}`,
      'Content-Type': 'application/json',
    };
  }

  /**
   * Start a robot run
   *
   * @param processId - Robocorp process ID (from Control Room)
   * @param inputData - Input work items for the robot
   * @returns Run ID for tracking
   */
  async startRun(processId: string, inputData: Record<string, any>): Promise<string> {
    const headers = await this.getAuthHeaders();

    try {
      // Create work item with input data
      const workItemResponse = await this.client.post(
        `/workspaces/${this.workspaceId}/work-items`,
        {
          payload: inputData,
        },
        { headers }
      );

      const workItemId = workItemResponse.data.id;

      await this.auditLogger.log({
        action: 'robocorp.work-item.created',
        resource: 'robocorp-work-item',
        resourceId: workItemId,
        changes: { processId, inputData },
      });

      // Trigger process run with work item
      const runResponse = await this.client.post(
        `/workspaces/${this.workspaceId}/processes/${processId}/runs`,
        {
          workItemIds: [workItemId],
        },
        { headers }
      );

      const runId = runResponse.data.id;

      await this.auditLogger.log({
        action: 'robocorp.run.started',
        resource: 'robocorp-run',
        resourceId: runId,
        changes: { processId, workItemId },
      });

      console.log(`[RobocorpConnector] Started run ${runId} for process ${processId}`);

      return runId;
    } catch (error) {
      await this.auditLogger.log({
        action: 'robocorp.run.start.failed',
        resource: 'robocorp-process',
        resourceId: processId,
        result: 'failure',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });

      throw new Error(
        `Failed to start Robocorp run: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Get run status
   *
   * @param runId - Run ID to query
   * @returns Current run status with output data
   */
  async getRunStatus(runId: string): Promise<RobocorpRunStatus> {
    const headers = await this.getAuthHeaders();

    try {
      const response = await this.client.get(
        `/workspaces/${this.workspaceId}/runs/${runId}`,
        { headers }
      );

      const run = response.data;

      // Get output from work items
      let outputData = {};
      if (run.state === 'COMPLETED') {
        try {
          const workItemsResponse = await this.client.get(
            `/workspaces/${this.workspaceId}/runs/${runId}/work-items`,
            { headers }
          );

          const outputWorkItem = workItemsResponse.data.find(
            (item: any) => item.type === 'output'
          );

          outputData = outputWorkItem?.payload || {};
        } catch (error) {
          console.warn(
            `[RobocorpConnector] Failed to fetch output work items for run ${runId}:`,
            error
          );
        }
      }

      return {
        id: run.id,
        state: run.state,
        startedAt: new Date(run.startedAt),
        completedAt: run.completedAt ? new Date(run.completedAt) : undefined,
        outputData,
        errorMessage: run.errorMessage,
      };
    } catch (error) {
      throw new Error(
        `Failed to get Robocorp run status: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Cancel a running job
   *
   * @param runId - Run ID to cancel
   */
  async cancelRun(runId: string): Promise<void> {
    const headers = await this.getAuthHeaders();

    try {
      await this.client.post(
        `/workspaces/${this.workspaceId}/runs/${runId}/cancel`,
        {},
        { headers }
      );

      await this.auditLogger.log({
        action: 'robocorp.run.cancelled',
        resource: 'robocorp-run',
        resourceId: runId,
      });

      console.log(`[RobocorpConnector] Cancelled run ${runId}`);
    } catch (error) {
      throw new Error(
        `Failed to cancel Robocorp run: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * List available processes in Robocorp workspace
   *
   * @returns Array of available processes
   */
  async listProcesses(): Promise<RobocorpProcess[]> {
    const headers = await this.getAuthHeaders();

    try {
      const response = await this.client.get(
        `/workspaces/${this.workspaceId}/processes`,
        { headers }
      );

      return response.data.map((process: any) => ({
        id: process.id,
        name: process.name,
        description: process.description,
      }));
    } catch (error) {
      throw new Error(
        `Failed to list Robocorp processes: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Set up webhook for real-time notifications
   *
   * @param config - Webhook configuration
   */
  async setupWebhook(config: RobocorpWebhookConfig): Promise<void> {
    const headers = await this.getAuthHeaders();

    try {
      const response = await this.client.post(
        `/workspaces/${this.workspaceId}/webhooks`,
        {
          url: config.url,
          events: config.events,
          secret: config.secret,
        },
        { headers }
      );

      await this.auditLogger.log({
        action: 'robocorp.webhook.created',
        resource: 'robocorp-webhook',
        resourceId: response.data.id,
        changes: config,
      });

      console.log(
        `[RobocorpConnector] Webhook created: ${config.url} for events ${config.events.join(', ')}`
      );
    } catch (error) {
      throw new Error(
        `Failed to setup Robocorp webhook: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * List active webhooks
   *
   * @returns Array of webhook configurations
   */
  async listWebhooks(): Promise<RobocorpWebhookConfig[]> {
    const headers = await this.getAuthHeaders();

    try {
      const response = await this.client.get(
        `/workspaces/${this.workspaceId}/webhooks`,
        { headers }
      );

      return response.data.map((webhook: any) => ({
        url: webhook.url,
        events: webhook.events,
        secret: webhook.secret,
      }));
    } catch (error) {
      throw new Error(
        `Failed to list Robocorp webhooks: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    }
  }

  /**
   * Wait for run completion with polling
   *
   * @param runId - Run ID to wait for
   * @param pollIntervalMs - Polling interval in milliseconds (default: 5000)
   * @param timeoutMs - Timeout in milliseconds (default: 600000 - 10 minutes)
   * @returns Final run status
   */
  async waitForCompletion(
    runId: string,
    pollIntervalMs: number = 5000,
    timeoutMs: number = 600000
  ): Promise<RobocorpRunStatus> {
    const startTime = Date.now();

    while (true) {
      const status = await this.getRunStatus(runId);

      // Check if run is in terminal state
      if (status.state === 'COMPLETED' || status.state === 'FAILED') {
        return status;
      }

      // Check timeout
      if (Date.now() - startTime > timeoutMs) {
        throw new Error(`Run ${runId} timed out after ${timeoutMs}ms`);
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
