import { NextRequest, NextResponse } from 'next/server';
import { AuditLogger } from '@/core/security/AuditLogger';
import type { APIResponse, AuditLogQueryRequest, AuditLogQueryResponse } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize Audit Logger (singleton)
let auditLogger: AuditLogger;
function getAuditLogger(): AuditLogger {
  if (!auditLogger) {
    auditLogger = new AuditLogger();
  }
  return auditLogger;
}

/**
 * GET /api/audit
 * Query audit logs with filters
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const searchParams = request.nextUrl.searchParams;

    const queryParams: AuditLogQueryRequest = {
      userId: searchParams.get('userId') || undefined,
      action: searchParams.get('action') || undefined,
      resource: searchParams.get('resource') || undefined,
      resourceId: searchParams.get('resourceId') || undefined,
      result: searchParams.get('result') as 'success' | 'failure' | undefined,
      startDate: searchParams.get('startDate') ? new Date(searchParams.get('startDate')!) : undefined,
      endDate: searchParams.get('endDate') ? new Date(searchParams.get('endDate')!) : undefined,
      limit: searchParams.get('limit') ? parseInt(searchParams.get('limit')!) : 100,
      offset: searchParams.get('offset') ? parseInt(searchParams.get('offset')!) : 0,
    };

    const logger = getAuditLogger();

    // Query audit logs
    const logs = await logger.query(queryParams);

    const response: APIResponse<AuditLogQueryResponse> = {
      success: true,
      data: {
        logs,
        total: logs.length,
        page: Math.floor((queryParams.offset || 0) / (queryParams.limit || 100)) + 1,
        pageSize: queryParams.limit || 100,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] GET /api/audit error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'AUDIT_QUERY_ERROR',
        message: error.message || 'Failed to query audit logs',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * GET /api/audit/statistics
 * Get audit statistics
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const logger = getAuditLogger();
    const stats = await logger.getStatistics();

    const response: APIResponse<any> = {
      success: true,
      data: stats,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] POST /api/audit/statistics error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'AUDIT_STATS_ERROR',
        message: error.message || 'Failed to retrieve audit statistics',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
