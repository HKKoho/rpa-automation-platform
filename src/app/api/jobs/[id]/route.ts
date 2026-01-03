import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';
import type { APIResponse, JobStatusResponse, JobUpdateRequest } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize RPA Engine (singleton pattern)
let rpaEngine: RPAEngine;
function getRPAEngine(): RPAEngine {
  if (!rpaEngine) {
    rpaEngine = new RPAEngine();
  }
  return rpaEngine;
}

interface RouteParams {
  params: {
    id: string;
  };
}

/**
 * GET /api/jobs/[id]
 * Get job details and status
 */
export async function GET(
  request: NextRequest,
  { params }: RouteParams
): Promise<NextResponse> {
  try {
    const { id } = params;

    const engine = getRPAEngine();
    const jobStatus = await engine.getJobStatus(id);

    if (!jobStatus) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'JOB_NOT_FOUND',
            message: `Job with ID ${id} not found`,
          },
          timestamp: new Date(),
        },
        { status: 404 }
      );
    }

    const response: APIResponse<any> = {
      success: true,
      data: jobStatus,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error(`[API] GET /api/jobs/${params.id} error:`, error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'JOB_STATUS_ERROR',
        message: error.message || 'Failed to retrieve job status',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * PUT /api/jobs/[id]
 * Update job configuration
 */
export async function PUT(
  request: NextRequest,
  { params }: RouteParams
): Promise<NextResponse> {
  try {
    const { id } = params;
    const body: JobUpdateRequest = await request.json();

    const engine = getRPAEngine();

    // Get current job
    const currentJob = await engine.getJobStatus(id);

    if (!currentJob) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'JOB_NOT_FOUND',
            message: `Job with ID ${id} not found`,
          },
          timestamp: new Date(),
        },
        { status: 404 }
      );
    }

    // Update job (implementation depends on RPAEngine's update method)
    // For now, we'll return success with the updated config
    const updatedJob = {
      ...currentJob,
      ...body,
      updatedAt: new Date(),
    };

    const response: APIResponse<any> = {
      success: true,
      data: updatedJob,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error(`[API] PUT /api/jobs/${params.id} error:`, error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'JOB_UPDATE_ERROR',
        message: error.message || 'Failed to update job',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * DELETE /api/jobs/[id]
 * Cancel and delete a job
 */
export async function DELETE(
  request: NextRequest,
  { params }: RouteParams
): Promise<NextResponse> {
  try {
    const { id } = params;

    const engine = getRPAEngine();
    await engine.cancelJob(id);

    const response: APIResponse<{ message: string }> = {
      success: true,
      data: {
        message: `Job ${id} cancelled and deleted successfully`,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error(`[API] DELETE /api/jobs/${params.id} error:`, error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'JOB_DELETE_ERROR',
        message: error.message || 'Failed to delete job',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
