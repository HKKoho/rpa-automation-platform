import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';
import type { APIResponse, ExtractionTriggerRequest, ExtractionDataResponse } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize RPA Engine (singleton)
let rpaEngine: RPAEngine;
function getRPAEngine(): RPAEngine {
  if (!rpaEngine) {
    rpaEngine = new RPAEngine();
  }
  return rpaEngine;
}

/**
 * POST /api/extraction
 * Trigger a data extraction job
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const body: ExtractionTriggerRequest = await request.json();

    // Validate required fields
    if (!body.source || !body.credentialId || !body.extractionMethod) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'VALIDATION_ERROR',
            message: 'Missing required fields: source, credentialId, extractionMethod',
          },
          timestamp: new Date(),
        },
        { status: 400 }
      );
    }

    const engine = getRPAEngine();

    // Prepare extraction parameters
    const extractionParams = {
      jobId: body.jobId || `extract-${Date.now()}`,
      source: {
        type: body.source.type === 'clearinghouse' ? 'banking' :
              body.source.type === 'payment-processor' ? 'banking' :
              body.source.type === 'shared-infrastructure' ? 'banking' : 'custom',
        url: body.source.url,
        apiEndpoint: body.source.apiEndpoint,
        selectors: body.selectors,
        accountIdentifiers: [],
      },
      url: body.source.url || body.source.apiEndpoint || '',
      credentials: {
        vaultId: body.credentialId,
      },
      selectors: body.selectors,
    };

    // Trigger extraction
    const extractedData = await engine.extractData(extractionParams);

    const response: APIResponse<any> = {
      success: true,
      data: extractedData,
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 200 });
  } catch (error: any) {
    console.error('[API] POST /api/extraction error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'EXTRACTION_ERROR',
        message: error.message || 'Failed to extract data',
        details: error,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
