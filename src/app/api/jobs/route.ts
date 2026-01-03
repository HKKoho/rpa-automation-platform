import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';
import type { APIResponse, JobCreateRequest, JobListResponse } from '@/types/api.types';
import type { RPAJob } from '@/types/rpa.types';

export const dynamic = 'force-dynamic';

// Initialize RPA Engine (singleton pattern)
let rpaEngine: RPAEngine;
function getRPAEngine(): RPAEngine {
  if (!rpaEngine) {
    rpaEngine = new RPAEngine();
  }
  return rpaEngine;
}

/**
 * GET /api/jobs
 * List all scheduled jobs
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const searchParams = request.nextUrl.searchParams;
    const page = parseInt(searchParams.get('page') || '1');
    const pageSize = parseInt(searchParams.get('pageSize') || '20');
    const status = searchParams.get('status');

    const engine = getRPAEngine();

    // TODO: Implement job storage/retrieval system
    // For now, return empty list
    const allJobs: RPAJob[] = [];

    // Filter by status if provided
    let filteredJobs = allJobs;
    if (status) {
      filteredJobs = allJobs.filter((job) => job.status === status);
    }

    // Pagination
    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const paginatedJobs = filteredJobs.slice(startIndex, endIndex);

    const response: APIResponse<JobListResponse> = {
      success: true,
      data: {
        jobs: paginatedJobs,
        total: filteredJobs.length,
        page,
        pageSize,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] GET /api/jobs error:', error);

    const response: APIResponse<JobListResponse> = {
      success: false,
      error: {
        code: 'JOB_LIST_ERROR',
        message: error.message || 'Failed to retrieve jobs',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * POST /api/jobs
 * Create and schedule a new job
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const body: JobCreateRequest = await request.json();

    // Validate required fields
    if (!body.name || !body.schedule || !body.source || !body.credentialId) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'VALIDATION_ERROR',
            message: 'Missing required fields: name, schedule, source, credentialId',
          },
          timestamp: new Date(),
        },
        { status: 400 }
      );
    }

    const engine = getRPAEngine();

    // Create job configuration matching RPAJob interface
    const jobConfig: Partial<RPAJob> = {
      name: body.name,
      schedule: {
        expression: body.schedule.expression,
        timezone: body.schedule.timezone || 'UTC',
        preferredHours: body.schedule.preferredHours || [],
        enabled: true,
      },
      source: {
        type: body.source.type === 'clearinghouse'
          ? 'banking'
          : body.source.type === 'payment-processor'
          ? 'banking'
          : body.source.type === 'shared-infrastructure'
          ? 'banking'
          : 'custom',
        url: body.source.url,
        apiEndpoint: body.source.apiEndpoint,
        selectors: body.source.selectors,
        accountIdentifiers: [], // Default empty, can be added later
      },
      credentials: {
        vaultId: body.credentialId,
        type: 'oauth', // Will be determined by credential vault
        encryptedData: '', // Will be populated by credential vault during execution
      },
      extractionMethod: body.extractionMethod,
      status: 'idle',
      retryConfig: {
        maxAttempts: body.retryConfig?.maxAttempts || 3,
        backoffStrategy: body.retryConfig?.backoffStrategy || 'exponential',
        initialDelay: body.retryConfig?.initialDelay || 5000,
        maxDelay: body.retryConfig?.maxDelay || 60000,
        retryableErrors: ['NETWORK_ERROR', 'TIMEOUT'],
      },
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    // Schedule the job
    const jobId = await engine.scheduleJob(jobConfig as RPAJob);

    const response: APIResponse<{ jobId: string; status: string }> = {
      success: true,
      data: {
        jobId,
        status: 'scheduled',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 201 });
  } catch (error: any) {
    console.error('[API] POST /api/jobs error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'JOB_CREATE_ERROR',
        message: error.message || 'Failed to create job',
        details: error,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
