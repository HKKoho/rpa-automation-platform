/**
 * Robocorp Webhook Handler
 *
 * Receives real-time notifications from Robocorp Cloud when runs complete or fail
 */

import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';
import { RobocorpConnector } from '@/integrations/robocorp/RobocorpConnector';
import type { APIResponse } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Mapping of Robocorp run IDs to internal job IDs
const runJobMapping = new Map<string, string>();

/**
 * POST /api/webhooks/robocorp
 * Handle Robocorp webhook notifications
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const webhook = await request.json();

    const { event, runId, workspaceId, state, errorMessage } = webhook;

    console.log(`[Webhook] Received Robocorp event: ${event} for run ${runId}`);

    // Verify workspace ID matches
    if (workspaceId !== process.env.ROBOCORP_WORKSPACE_ID) {
      console.warn(`[Webhook] Workspace ID mismatch: ${workspaceId}`);
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'INVALID_WORKSPACE',
            message: 'Workspace ID does not match',
          },
          timestamp: new Date(),
        },
        { status: 403 }
      );
    }

    // Get internal job ID from mapping
    const jobId = runJobMapping.get(runId);
    if (!jobId) {
      console.warn(`[Webhook] No job mapping found for run ${runId}`);
      // Still acknowledge webhook
      return NextResponse.json({ received: true });
    }

    // Handle different event types
    if (event === 'run.completed' && state === 'COMPLETED') {
      await handleRunCompleted(runId, jobId);
    } else if (event === 'run.failed' || state === 'FAILED') {
      await handleRunFailed(runId, jobId, errorMessage);
    }

    // Clean up mapping
    runJobMapping.delete(runId);

    const response: APIResponse<{ runId: string; event: string }> = {
      success: true,
      data: { runId, event },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[Webhook] Error processing Robocorp webhook:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'WEBHOOK_ERROR',
        message: error.message || 'Failed to process webhook',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * Handle successful run completion
 */
async function handleRunCompleted(runId: string, jobId: string): Promise<void> {
  try {
    const connector = new RobocorpConnector();
    const status = await connector.getRunStatus(runId);

    console.log(`[Webhook] Run ${runId} completed successfully for job ${jobId}`);
    console.log(`[Webhook] Output data:`, status.outputData);

    // TODO: Trigger ETL pipeline with extracted data
    // const pipeline = new ETLPipeline();
    // await pipeline.process(status.outputData);

    // Emit event for real-time UI updates
    // this.emit('extraction:completed', { jobId, data: status.outputData });
  } catch (error) {
    console.error(`[Webhook] Error handling completed run ${runId}:`, error);
  }
}

/**
 * Handle failed run
 */
async function handleRunFailed(
  runId: string,
  jobId: string,
  errorMessage?: string
): Promise<void> {
  console.error(
    `[Webhook] Run ${runId} failed for job ${jobId}: ${errorMessage || 'Unknown error'}`
  );

  // TODO: Update job status in database
  // TODO: Trigger failure notifications (email, Slack, etc.)
  // TODO: Implement retry logic if configured

  // Emit event for real-time UI updates
  // this.emit('extraction:failed', { jobId, error: errorMessage });
}

/**
 * Register run ID to job ID mapping
 * Called by RPAEngine when delegating to Robocorp
 */
export function registerRunJobMapping(runId: string, jobId: string): void {
  runJobMapping.set(runId, jobId);
}

/**
 * GET /api/webhooks/robocorp
 * Verify webhook endpoint (for Robocorp setup)
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  return NextResponse.json({
    service: 'RPA Automation Platform - Robocorp Webhook Handler',
    status: 'active',
    timestamp: new Date(),
  });
}
