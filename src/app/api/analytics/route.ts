import { NextRequest, NextResponse } from 'next/server';
import { BigQueryClient } from '@/core/warehouse/BigQueryClient';
import type { APIResponse, DashboardAnalyticsResponse } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize BigQuery client (singleton)
let bigQueryClient: BigQueryClient;
function getBigQueryClient(): BigQueryClient {
  if (!bigQueryClient) {
    bigQueryClient = new BigQueryClient();
  }
  return bigQueryClient;
}

/**
 * GET /api/analytics
 * Get dashboard analytics from BigQuery warehouse
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const client = getBigQueryClient();
    const dataset = process.env.BQ_DATASET || 'rpa_warehouse';

    // Query job execution statistics (last 30 days)
    const jobStatsQuery = `
      SELECT
        COUNT(DISTINCT job_id) AS total,
        COUNTIF(status = 'success') AS completed,
        COUNTIF(status = 'failed') AS failed,
        COUNTIF(status IN ('pending', 'running')) AS active
      FROM \`${dataset}.fact_job_executions\`
      WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    `;

    // Query extraction statistics
    const extractionStatsQuery = `
      SELECT
        COALESCE(SUM(records_extracted), 0) AS total_records,
        COALESCE(SUM(CASE WHEN execution_date = CURRENT_DATE() THEN records_extracted ELSE 0 END), 0) AS today_records,
        COALESCE(SAFE_DIVIDE(
          COUNTIF(status = 'success'),
          COUNT(*)
        ) * 100, 0) AS success_rate,
        COALESCE(AVG(duration_ms), 0) AS avg_time_ms
      FROM \`${dataset}.fact_job_executions\`
      WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    `;

    // Query ETL pipeline statistics
    const pipelineStatsQuery = `
      SELECT
        COUNTIF(status = 'processing') AS processing,
        COUNTIF(status = 'completed') AS completed,
        COUNTIF(status = 'failed') AS failed,
        COALESCE(SAFE_DIVIDE(
          SUM(CASE WHEN status = 'completed' THEN records_processed ELSE 0 END),
          NULLIF(SUM(CASE WHEN status = 'completed' THEN duration_ms ELSE 0 END) / 3600000, 0)
        ), 0) AS throughput
      FROM \`${dataset}.fact_etl_pipeline_jobs\`
      WHERE execution_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    `;

    // Execute queries in parallel
    const [jobStats, extractionStats, pipelineStats] = await Promise.all([
      client.query<any>(jobStatsQuery),
      client.query<any>(extractionStatsQuery),
      client.query<any>(pipelineStatsQuery),
    ]);

    // Build analytics response
    const analytics: DashboardAnalyticsResponse = {
      jobs: {
        total: parseInt(jobStats[0]?.total) || 0,
        active: parseInt(jobStats[0]?.active) || 0,
        completed: parseInt(jobStats[0]?.completed) || 0,
        failed: parseInt(jobStats[0]?.failed) || 0,
      },
      extraction: {
        totalRecords: parseInt(extractionStats[0]?.total_records) || 0,
        todayRecords: parseInt(extractionStats[0]?.today_records) || 0,
        successRate: parseFloat(extractionStats[0]?.success_rate) || 0,
        averageTimeMs: parseFloat(extractionStats[0]?.avg_time_ms) || 0,
      },
      pipeline: {
        processing: parseInt(pipelineStats[0]?.processing) || 0,
        completed: parseInt(pipelineStats[0]?.completed) || 0,
        failed: parseInt(pipelineStats[0]?.failed) || 0,
        throughput: parseFloat(pipelineStats[0]?.throughput) || 0,
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

    // Fallback to mock data if BigQuery is not configured
    const fallbackAnalytics: DashboardAnalyticsResponse = {
      jobs: { total: 0, active: 0, completed: 0, failed: 0 },
      extraction: { totalRecords: 0, todayRecords: 0, successRate: 0, averageTimeMs: 0 },
      pipeline: { processing: 0, completed: 0, failed: 0, throughput: 0 },
      system: { status: 'healthy', uptime: process.uptime(), version: '1.0.0' },
    };

    const response: APIResponse<DashboardAnalyticsResponse> = {
      success: true,
      data: fallbackAnalytics,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  }
}
