import { NextRequest, NextResponse } from 'next/server';
import {
  getAllBankingNetworks,
  getBankingNetworkById,
  getBankingNetworksByType,
  BANKING_NETWORKS,
} from '@/config/bankingNetworks';
import type { APIResponse } from '@/types/api.types';

export const dynamic = 'force-dynamic';

/**
 * GET /api/sources
 * Get all banking network sources or filter by type
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const searchParams = request.nextUrl.searchParams;
    const type = searchParams.get('type') as
      | 'clearinghouse'
      | 'payment-processor'
      | 'shared-infrastructure'
      | 'direct-bank'
      | null;

    let sources;

    if (type) {
      sources = getBankingNetworksByType(type);
    } else {
      sources = getAllBankingNetworks();
    }

    const response: APIResponse<any> = {
      success: true,
      data: {
        sources,
        total: sources.length,
        categories: {
          clearinghouses: BANKING_NETWORKS.clearinghouses.length,
          paymentProcessors: BANKING_NETWORKS.paymentProcessors.length,
          sharedInfrastructure: BANKING_NETWORKS.sharedInfrastructure.length,
          directBanks: BANKING_NETWORKS.directBanks.length,
        },
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] GET /api/sources error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'SOURCES_ERROR',
        message: error.message || 'Failed to retrieve banking sources',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
