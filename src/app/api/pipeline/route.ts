import { NextRequest, NextResponse } from 'next/server';
import { ETLPipeline } from '@/core/pipeline/ETLPipeline';
import type { APIResponse, PipelineProcessRequest } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize ETL Pipeline (singleton)
let etlPipeline: ETLPipeline;
function getETLPipeline(): ETLPipeline {
  if (!etlPipeline) {
    etlPipeline = new ETLPipeline();
  }
  return etlPipeline;
}

/**
 * POST /api/pipeline
 * Process extracted data through ETL pipeline
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const body: PipelineProcessRequest = await request.json();

    // Validate required fields
    if (!body.extractedDataId || !body.destination) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'VALIDATION_ERROR',
            message: 'Missing required fields: extractedDataId, destination',
          },
          timestamp: new Date(),
        },
        { status: 400 }
      );
    }

    const pipeline = getETLPipeline();

    // Note: In a real implementation, we would fetch the extracted data by ID
    // For now, we'll create a mock extracted data object
    const extractedData = {
      jobId: body.extractedDataId,
      timestamp: new Date(),
      rawData: {},
      metadata: {
        source: 'api',
        extractionDuration: 0,
        recordCount: 0,
        compressionUsed: false,
        checksumHash: '',
      },
      validationStatus: {
        isValid: true,
        errors: [],
        warnings: [],
        validatedAt: new Date(),
        validationRules: [],
      },
      dataSize: 0,
    };

    // Process through pipeline
    const result = await pipeline.process(extractedData);

    const response: APIResponse<any> = {
      success: true,
      data: result,
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 200 });
  } catch (error: any) {
    console.error('[API] POST /api/pipeline error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'PIPELINE_ERROR',
        message: error.message || 'Failed to process pipeline',
        details: error,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * GET /api/pipeline/statistics
 * Get pipeline statistics
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const pipeline = getETLPipeline();

    // Mock statistics for now
    const statistics = {
      totalJobsProcessed: 0,
      successRate: 0,
      averageProcessingTime: 0,
      recordsByStage: {
        extracted: 0,
        validated: 0,
        transformed: 0,
        loaded: 0,
      },
      errorsByType: {},
    };

    const response: APIResponse<any> = {
      success: true,
      data: statistics,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] GET /api/pipeline/statistics error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'STATISTICS_ERROR',
        message: error.message || 'Failed to retrieve statistics',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
