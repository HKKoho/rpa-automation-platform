import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';
import type { APIResponse, DashboardAnalyticsResponse } from '@/types/api.types';

// Initialize RPA Engine (singleton)
let rpaEngine: RPAEngine;
function getRPAEngine(): RPAEngine {
  if (!rpaEngine) {
    rpaEngine = new RPAEngine();
  }
  return rpaEngine;
}

/**
 * GET /api/analytics
 * Get dashboard analytics
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const engine = getRPAEngine();

    // Get all jobs for analytics
    const allJobs = await engine.getAllJobs();

    // Calculate analytics
    const analytics: DashboardAnalyticsResponse = {
      jobs: {
        total: allJobs.length,
        active: allJobs.filter((j) => j.status === 'running').length,
        completed: allJobs.filter((j) => j.status === 'completed').length,
        failed: allJobs.filter((j) => j.status === 'failed').length,
      },
      extraction: {
        totalRecords: 0,
        todayRecords: 0,
        successRate: allJobs.length > 0
          ? (allJobs.filter((j) => j.status === 'completed').length / allJobs.length) * 100
          : 0,
        averageTimeMs: 5000, // Mock value
      },
      pipeline: {
        processing: 0,
        completed: 0,
        failed: 0,
        throughput: 0,
      },
      system: {
        status: 'healthy',
        uptime: process.uptime(),
        version: '1.0.0',
      },
    };

    const response: APIResponse<DashboardAnalyticsResponse> = {
      success: true,
      data: analytics,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] GET /api/analytics error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'ANALYTICS_ERROR',
        message: error.message || 'Failed to retrieve analytics',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
