export default function ImplementationPage() {
  return (
    <>
      {/* Page Title */}
      <div className="card-glass mb-8">
        <h1 className="text-4xl font-bold mb-4 text-gray-800">Implementation Guide</h1>
        <p className="text-gray-600 text-lg">
          Learn how to build with the RPA platform using best practices
        </p>
      </div>

      {/* Quick Start */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Quick Start</h2>
        <div className="space-y-4">
          <div className="p-5 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border-l-4 border-blue-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">1. Install Dependencies</h3>
            <div className="code-block">
              <code className="text-sm">
                npm install<br/>
                cp .env.example .env<br/>
                npm run db:migrate
              </code>
            </div>
          </div>

          <div className="p-5 bg-gradient-to-r from-purple-50 to-purple-100 rounded-lg border-l-4 border-purple-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">2. Start Development Servers</h3>
            <div className="code-block">
              <code className="text-sm">
                npm run dev          # Next.js (port 3000)<br/>
                npm run worker       # Extraction worker<br/>
                npm run scheduler    # Scheduler worker<br/>
                npm run websocket    # WebSocket server (port 3001)
              </code>
            </div>
          </div>

          <div className="p-5 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border-l-4 border-green-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">3. Access the Platform</h3>
            <div className="text-sm text-gray-600 space-y-2">
              <p>Frontend: <code className="px-2 py-1 bg-white rounded">http://localhost:3000</code></p>
              <p>WebSocket: <code className="px-2 py-1 bg-white rounded">ws://localhost:3001</code></p>
            </div>
          </div>
        </div>
      </div>

      {/* API Route Pattern */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">API Route Pattern</h2>
        <p className="text-gray-600 mb-4">
          All API routes follow a standardized pattern with singleton services and type-safe responses.
        </p>
        <div className="code-block">
          <code className="text-sm">
            {`import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';
import type { APIResponse } from '@/types/api.types';

// Singleton pattern for core services
let rpaEngine: RPAEngine;
function getRPAEngine(): RPAEngine {
  if (!rpaEngine) {
    rpaEngine = new RPAEngine();
  }
  return rpaEngine;
}

export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const engine = getRPAEngine();
    const data = await engine.someMethod();

    const response: APIResponse<SomeType> = {
      success: true,
      data,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'ERROR_CODE',
        message: error.message || 'Failed',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}`}
          </code>
        </div>
      </div>

      {/* Core Service Usage */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Core Service Usage</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* RPAEngine */}
          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-blue-600">⚡</span>
              RPAEngine
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Central orchestrator for job scheduling and management
            </p>
            <div className="code-block">
              <code className="text-xs">
                {`import { RPAEngine } from '@/core/engine/RPAEngine';

const engine = new RPAEngine();

// Schedule a new job
await engine.scheduleJob({
  name: 'Daily Payment Extract',
  schedule: '0 0 * * *',
  dataSource: { /* config */ }
});

// Get job status
const status = await engine.getJobStatus(jobId);

// Cancel a job
await engine.cancelJob(jobId);`}
              </code>
            </div>
          </div>

          {/* CredentialVault */}
          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-purple-600">🔐</span>
              CredentialVault
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Secure credential storage with AES-256-GCM encryption
            </p>
            <div className="code-block">
              <code className="text-xs">
                {`import { CredentialVault } from '@/core/security/CredentialVault';

const vault = new CredentialVault();

// Store credentials
await vault.store('bank-api-key', {
  username: 'admin',
  password: 'secret'
}, { expiresAt: futureDate });

// Retrieve credentials
const creds = await vault.retrieve('bank-api-key');

// Rotate credentials
await vault.rotate('bank-api-key');`}
              </code>
            </div>
          </div>

          {/* ETLPipeline */}
          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-green-600">🔄</span>
              ETLPipeline
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Automated data validation, transformation, and loading
            </p>
            <div className="code-block">
              <code className="text-xs">
                {`import { ETLPipeline } from '@/core/pipeline/ETLPipeline';

const pipeline = new ETLPipeline();

// Process extracted data
const result = await pipeline.process({
  source: 'Stripe-API',
  data: extractedData,
  schema: transactionSchema
});

// Returns: {
//   success: true,
//   validated: [...],
//   transformed: [...],
//   loaded: true
// }`}
              </code>
            </div>
          </div>

          {/* WebAutomation */}
          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-yellow-600">🤖</span>
              WebAutomation
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Headless browser automation with Puppeteer/Playwright
            </p>
            <div className="code-block">
              <code className="text-xs">
                {`import { WebAutomation } from '@/core/extraction/WebAutomation';

const automation = new WebAutomation();

await automation.navigate('https://bank.example.com');
await automation.fillForm({
  username: '#user',
  password: '#pass'
});
await automation.click('#login-btn');

const data = await automation.extractTable('.transactions');
await automation.captureScreenshot('audit.png');`}
              </code>
            </div>
          </div>
        </div>
      </div>

      {/* TypeScript Imports */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Path Aliases & Imports</h2>
        <p className="text-gray-600 mb-4">
          Always use TypeScript path aliases for clean, maintainable imports.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="code-block">
            <div className="text-xs font-bold text-gray-700 mb-2">Core Services</div>
            <code className="text-xs">
              {`import { RPAEngine } from '@/core/engine/RPAEngine';
import { CredentialVault } from '@/core/security/CredentialVault';
import { ETLPipeline } from '@/core/pipeline/ETLPipeline';
import { WebAutomation } from '@/core/extraction/WebAutomation';`}
            </code>
          </div>

          <div className="code-block">
            <div className="text-xs font-bold text-gray-700 mb-2">Types</div>
            <code className="text-xs">
              {`import type { RPAJob, DataSource } from '@/types/rpa.types';
import type { APIResponse } from '@/types/api.types';
import type { BankingTransaction } from '@/types/rpa.types';`}
            </code>
          </div>

          <div className="code-block">
            <div className="text-xs font-bold text-gray-700 mb-2">Components</div>
            <code className="text-xs">
              {`import { Header } from '@/components/layout/Header';
import { Navigation } from '@/components/layout/Navigation';`}
            </code>
          </div>

          <div className="code-block">
            <div className="text-xs font-bold text-gray-700 mb-2">Utilities</div>
            <code className="text-xs">
              {`import { cn } from '@/lib/utils';
import { useWebSocket } from '@/hooks/useWebSocket';`}
            </code>
          </div>
        </div>
      </div>

      {/* WebSocket Integration */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">WebSocket Processing Updates</h2>
        <p className="text-gray-600 mb-4">
          Subscribe to call on or scheduled events for job updates and progress tracking.
        </p>
        <div className="code-block">
          <code className="text-sm">
            {`'use client';

import { useWebSocket } from '@/hooks/useWebSocket';
import { useEffect } from 'react';

export function JobMonitor() {
  const { subscribe, isConnected } = useWebSocket();

  useEffect(() => {
    // Subscribe to job events
    const unsubscribeCompleted = subscribe('job:completed', (data) => {
      console.log('Job completed:', data);
    });

    const unsubscribeFailed = subscribe('job:failed', (data) => {
      console.error('Job failed:', data);
    });

    const unsubscribeProgress = subscribe('extraction:progress', (data) => {
      console.log('Progress:', data.percentage + '%');
    });

    // Cleanup subscriptions
    return () => {
      unsubscribeCompleted();
      unsubscribeFailed();
      unsubscribeProgress();
    };
  }, [subscribe]);

  return (
    <div>
      <div>WebSocket: {isConnected ? '🟢 Connected' : '🔴 Disconnected'}</div>
    </div>
  );
}`}
          </code>
        </div>
      </div>

      {/* Environment Variables */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Environment Variables</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Required</h3>
            <div className="space-y-2 text-sm">
              <div className="p-3 bg-red-50 rounded border-l-4 border-red-500">
                <code className="font-bold">ENCRYPTION_MASTER_KEY</code>
                <p className="text-gray-600 mt-1">32-byte hex key for credential encryption</p>
              </div>
              <div className="p-3 bg-red-50 rounded border-l-4 border-red-500">
                <code className="font-bold">DATABASE_URL</code>
                <p className="text-gray-600 mt-1">PostgreSQL connection string</p>
              </div>
              <div className="p-3 bg-red-50 rounded border-l-4 border-red-500">
                <code className="font-bold">REDIS_HOST</code> / <code className="font-bold">REDIS_PORT</code>
                <p className="text-gray-600 mt-1">Redis for queue management</p>
              </div>
              <div className="p-3 bg-red-50 rounded border-l-4 border-red-500">
                <code className="font-bold">AUDIT_LOG_DIR</code>
                <p className="text-gray-600 mt-1">Audit log storage directory</p>
              </div>
            </div>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Optional</h3>
            <div className="space-y-2 text-sm">
              <div className="p-3 bg-blue-50 rounded border-l-4 border-blue-500">
                <code className="font-bold">NEXT_PUBLIC_WS_URL</code>
                <p className="text-gray-600 mt-1">WebSocket server URL (default: ws://localhost:3001)</p>
              </div>
              <div className="p-3 bg-blue-50 rounded border-l-4 border-blue-500">
                <code className="font-bold">ENABLE_AI_DETECTION</code>
                <p className="text-gray-600 mt-1">Enable TensorFlow.js element detection</p>
              </div>
              <div className="p-3 bg-blue-50 rounded border-l-4 border-blue-500">
                <code className="font-bold">COMPLIANCE_MODE</code>
                <p className="text-gray-600 mt-1">PCI-DSS, GDPR, SOC2, ISO27001</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Best Practices */}
      <div className="card-glass">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Best Practices</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-5 bg-green-50 rounded-lg border-l-4 border-green-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">✓ DO</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Use TypeScript path aliases (@/core/*, @/types/*)</li>
              <li>• Implement singleton pattern for core services</li>
              <li>• Use APIResponse type for all API routes</li>
              <li>• Subscribe to WebSocket events for real-time updates</li>
              <li>• Validate all external data with ETL pipeline</li>
              <li>• Store credentials in CredentialVault</li>
              <li>• Use Tailwind utility classes from globals.css</li>
            </ul>
          </div>

          <div className="p-5 bg-red-50 rounded-lg border-l-4 border-red-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">✗ DON'T</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Modify backend core services (src/core/*)</li>
              <li>• Store credentials in plain text</li>
              <li>• Skip data validation before loading</li>
              <li>• Use relative imports instead of path aliases</li>
              <li>• Create multiple instances of core services</li>
              <li>• Disable audit logging</li>
              <li>• Hard-code sensitive configuration</li>
            </ul>
          </div>
        </div>
      </div>
    </>
  );
}
