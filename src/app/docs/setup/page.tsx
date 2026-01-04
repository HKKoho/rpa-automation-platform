/**
 * BigQuery & Power BI Setup Page
 * Instructions for connecting analytics and reporting
 */

export const metadata = {
  title: 'BigQuery & Power BI Setup | RPA Platform',
  description: 'Complete guide for connecting Power BI to BigQuery data warehouse',
};

export default function SetupPage() {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-slate-100 p-8">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-4xl font-bold text-gray-800 mb-4">
          BigQuery & Power BI Setup
        </h1>
        <p className="text-gray-600 text-lg mb-8">
          Connect Power BI to your BigQuery data warehouse for real-time analytics
        </p>

        {/* Quick Start */}
        <div className="bg-blue-50 shadow-lg rounded-lg p-6 mb-8 border border-blue-200">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Quick Start (3 Steps)</h2>
          <div className="grid md:grid-cols-3 gap-4">
            <div className="bg-blue-50 p-4 rounded border border-blue-100">
              <div className="text-3xl font-bold text-blue-600 mb-2">1</div>
              <h3 className="text-gray-800 font-semibold mb-2">Setup BigQuery</h3>
              <p className="text-gray-600 text-sm">Create dataset and tables</p>
              <code className="block mt-2 text-xs bg-gray-100 p-2 rounded text-green-700">
                tsx server/database/bigquery-setup.ts
              </code>
            </div>
            <div className="bg-blue-50 p-4 rounded border border-blue-100">
              <div className="text-3xl font-bold text-blue-600 mb-2">2</div>
              <h3 className="text-gray-800 font-semibold mb-2">Migrate Data</h3>
              <p className="text-gray-600 text-sm">Load historical data</p>
              <code className="block mt-2 text-xs bg-gray-100 p-2 rounded text-green-700">
                tsx server/database/migrate-to-warehouse.ts
              </code>
            </div>
            <div className="bg-blue-50 p-4 rounded border border-blue-100">
              <div className="text-3xl font-bold text-blue-600 mb-2">3</div>
              <h3 className="text-gray-800 font-semibold mb-2">Connect Power BI</h3>
              <p className="text-gray-600 text-sm">Use BigQuery connector</p>
              <code className="block mt-2 text-xs bg-gray-100 p-2 rounded text-green-700">
                DirectQuery mode
              </code>
            </div>
          </div>
        </div>

        {/* AI Configuration (NEW in v2.0.0) */}
        <div className="bg-purple-50 shadow-lg rounded-lg p-6 mb-8 border-2 border-purple-300">
          <h2 className="text-2xl font-bold text-purple-700 mb-4">🧠 AI Configuration (Optional - Professional+ Tiers)</h2>
          <p className="text-gray-700 mb-4">
            Enable generative AI features for adaptive automation and intelligent data processing
          </p>
          <div className="bg-white p-4 rounded mb-4">
            <h3 className="text-gray-800 font-semibold mb-3">Environment Variables</h3>
            <code className="block text-xs bg-gray-100 p-4 rounded text-green-700 whitespace-pre">
{`# Enable AI Features (Professional+ tiers)
ENABLE_AI_FEATURES=true

# AI Provider (choose one: openai, gemini, deepseek, local)
AI_PROVIDER=deepseek

# API Keys (based on provider)
OPENAI_API_KEY=sk-...                # For OpenAI GPT-4
GEMINI_API_KEY=AIza...               # For Google Gemini
DEEPSEEK_API_KEY=ds-...              # For DeepSeek (recommended)
LOCAL_LLM_ENDPOINT=http://localhost:8000  # For local Llama 3

# AI Configuration
AI_DATA_MASKING=true                 # Mask PII before AI calls (recommended)
AI_MAX_CALLS_PER_DAY=1000           # Rate limit for AI calls`}
            </code>
          </div>
          <div className="grid md:grid-cols-3 gap-4 mb-4">
            <div className="bg-blue-100 p-4 rounded">
              <div className="font-bold text-blue-800 mb-2">DeepSeek (Recommended)</div>
              <div className="text-sm text-gray-700">Cost-effective: $0.14/1M tokens</div>
              <div className="text-xs text-gray-600 mt-1">Best for: General automation</div>
            </div>
            <div className="bg-green-100 p-4 rounded">
              <div className="font-bold text-green-800 mb-2">Google Gemini</div>
              <div className="text-sm text-gray-700">Multimodal: $0.075/1M tokens</div>
              <div className="text-xs text-gray-600 mt-1">Best for: Document extraction</div>
            </div>
            <div className="bg-purple-100 p-4 rounded">
              <div className="font-bold text-purple-800 mb-2">OpenAI GPT-4</div>
              <div className="text-sm text-gray-700">Premium: $2.50/1M tokens</div>
              <div className="text-xs text-gray-600 mt-1">Best for: Complex reasoning</div>
            </div>
          </div>
          <div className="bg-yellow-100 p-3 rounded border border-yellow-300">
            <div className="text-sm text-gray-800">
              <strong>🔒 Security Note:</strong> For banking compliance, enable <code>AI_DATA_MASKING=true</code> to strip PII before AI processing.
              Enterprise+ tier supports on-premise local LLMs for air-gapped environments.
            </div>
          </div>
        </div>

        {/* Architecture */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Architecture Overview</h2>
          <div className="bg-blue-50 shadow-lg rounded-lg p-6 border border-blue-200">
            <pre className="text-sm text-gray-700 overflow-x-auto">
{`Banking Networks → RPA Engine (AI-Assisted) → ETL Pipeline (AI Normalization) → BigQuery
                                                                                      ↓
                                   AI Services Layer                        Power BI Dashboards
                          (Adaptive Selectors, Error Recovery,
                           Data Normalization, Doc Understanding)`}
            </pre>
          </div>
        </section>

        {/* Prerequisites */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Prerequisites</h2>
          <div className="grid md:grid-cols-2 gap-4">
            <div className="bg-blue-50 shadow-lg rounded-lg p-4 border border-blue-200">
              <h3 className="text-gray-800 font-bold mb-2">✅ Required</h3>
              <ul className="text-gray-600 space-y-1 text-sm">
                <li>• GCP account with billing enabled</li>
                <li>• BigQuery API enabled</li>
                <li>• Service account with dataViewer + jobUser roles</li>
                <li>• Power BI Desktop or Pro license</li>
                <li>• Environment variables configured</li>
              </ul>
            </div>
            <div className="bg-blue-50 shadow-lg rounded-lg p-4 border border-blue-200">
              <h3 className="text-gray-800 font-bold mb-2">📝 Environment Variables</h3>
              <code className="block text-xs bg-gray-100 p-3 rounded text-green-700">
                {`GCP_PROJECT_ID=your-project\nGCP_KEY_FILE=./credentials/gcp.json\nBQ_DATASET=rpa_warehouse\nBQ_LOCATION=US`}
              </code>
            </div>
          </div>
        </section>

        {/* BigQuery Setup */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Step 1: BigQuery Setup</h2>
          <div className="bg-blue-50 shadow-lg rounded-lg p-6 border border-blue-200">
            <h3 className="text-gray-800 font-semibold mb-3">Create Data Warehouse Schema</h3>
            <div className="bg-gray-100 p-4 rounded mb-4">
              <code className="text-green-700 text-sm">
                $ tsx server/database/bigquery-setup.ts
              </code>
            </div>
            <p className="text-gray-600 mb-4">This creates:</p>
            <ul className="text-gray-600 space-y-2 ml-4">
              <li>• <strong>Fact Tables:</strong> fact_banking_transactions, fact_job_executions, fact_audit_logs, fact_etl_pipeline_jobs</li>
              <li>• <strong>Dimensions:</strong> dim_jobs, dim_banking_networks, dim_users, dim_date</li>
              <li>• <strong>Materialized Views:</strong> Daily summaries, job performance, network metrics</li>
              <li>• <strong>Partitioning:</strong> By date for cost optimization</li>
              <li>• <strong>Clustering:</strong> By frequently queried fields</li>
            </ul>
          </div>
        </section>

        {/* Data Migration */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Step 2: Migrate Historical Data</h2>
          <div className="bg-blue-50 shadow-lg rounded-lg p-6 border border-blue-200">
            <h3 className="text-gray-800 font-semibold mb-3">Load Existing Data</h3>
            <div className="bg-gray-100 p-4 rounded mb-4">
              <code className="text-green-700 text-sm">
                $ tsx server/database/migrate-to-warehouse.ts
              </code>
            </div>
            <p className="text-gray-600 mb-4">This migrates:</p>
            <ul className="text-gray-600 space-y-1 ml-4 text-sm">
              <li>• Audit logs from JSONL files (last 90 days)</li>
              <li>• Job execution history from Redis</li>
              <li>• Banking network metadata</li>
              <li>• Date dimension (2020-2030)</li>
            </ul>
          </div>
        </section>

        {/* Power BI Connection */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Step 3: Connect Power BI</h2>
          <div className="bg-blue-50 shadow-lg rounded-lg p-6 border border-blue-200">
            <div className="grid md:grid-cols-2 gap-6">
              <div>
                <h3 className="text-gray-800 font-semibold mb-3">Connection Steps:</h3>
                <ol className="text-gray-600 space-y-2 text-sm list-decimal ml-4">
                  <li>Open Power BI Desktop</li>
                  <li>Get Data → More → Google BigQuery</li>
                  <li>Select "Service Account" authentication</li>
                  <li>Upload your GCP service account JSON key</li>
                  <li>Select dataset: rpa_warehouse</li>
                  <li>Choose DirectQuery mode (real-time)</li>
                  <li>Select tables and create relationships</li>
                </ol>
              </div>
              <div>
                <h3 className="text-gray-800 font-semibold mb-3">Recommended Settings:</h3>
                <div className="bg-blue-50 p-4 rounded border border-blue-100 space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Storage Mode:</span>
                    <span className="text-gray-800 font-semibold">DirectQuery</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Refresh Rate:</span>
                    <span className="text-gray-800 font-semibold">Real-time</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Date Filter:</span>
                    <span className="text-gray-800 font-semibold">Last 30 days</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Query Timeout:</span>
                    <span className="text-gray-800 font-semibold">600 seconds</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Pre-Built Dashboards */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Pre-Built Dashboards</h2>
          <div className="grid md:grid-cols-2 gap-4">
            {[
              { name: 'Executive Overview', metrics: 'TX volume, success rates, network performance', icon: '📊' },
              { name: 'Operational Monitoring', metrics: 'Real-time jobs, active queues, error distribution', icon: '⚡' },
              { name: 'Compliance & Audit', metrics: 'Audit events, failed operations, compliance scores', icon: '🔒' },
              { name: 'Network Performance', metrics: 'Latency, uptime, peak hours, throughput', icon: '🌐' },
            ].map((dashboard) => (
              <div key={dashboard.name} className="bg-blue-50 shadow-lg rounded-lg p-4 border border-blue-200">
                <div className="text-3xl mb-2">{dashboard.icon}</div>
                <h3 className="text-gray-800 font-bold mb-2">{dashboard.name}</h3>
                <p className="text-gray-600 text-sm">{dashboard.metrics}</p>
              </div>
            ))}
          </div>
        </section>

        {/* Cost Estimate */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Cost Estimate (USD/month)</h2>
          <div className="bg-blue-50 shadow-lg rounded-lg p-6 border border-blue-200">
            <div className="grid md:grid-cols-4 gap-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">$10</div>
                <div className="text-gray-600 text-sm">Storage (500GB)</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">$10</div>
                <div className="text-gray-600 text-sm">Queries (2TB)</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-purple-600">$150</div>
                <div className="text-gray-600 text-sm">Streaming (3TB)</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-yellow-600">$50</div>
                <div className="text-gray-600 text-sm">Power BI Pro (5 users)</div>
              </div>
            </div>
            <div className="mt-4 pt-4 border-t border-gray-300 text-center">
              <div className="text-3xl font-bold text-gray-800">~$220/month</div>
              <div className="text-gray-500 text-sm">(Professional tier analytics)</div>
            </div>
          </div>
        </section>

        {/* Documentation Links */}
        <section className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Detailed Documentation</h2>
          <div className="grid md:grid-cols-2 gap-4">
            <div className="bg-blue-50 shadow-lg rounded-lg p-4 border border-blue-200">
              <h3 className="text-gray-800 font-bold mb-2">📚 Complete Guides</h3>
              <ul className="space-y-2">
                <li><a href="/docs/setup" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">Power BI Setup Guide</a></li>
                <li><a href="https://learn.microsoft.com/en-us/power-bi/connect-data/service-google-bigquery-connector" target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">Power BI BigQuery Connector →</a></li>
                <li><a href="https://cloud.google.com/bigquery/docs" target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">BigQuery Documentation →</a></li>
                <li><a href="/docs/pricing" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">Deployment Options & Pricing</a></li>
              </ul>
            </div>
            <div className="bg-blue-50 shadow-lg rounded-lg p-4 border border-blue-200">
              <h3 className="text-gray-800 font-bold mb-2">🔗 Related Pages</h3>
              <ul className="space-y-2">
                <li><a href="/docs/networks" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">Banking Networks</a></li>
                <li><a href="/docs/pricing" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">Pricing & Budget</a></li>
                <li><a href="/docs/integrations" className="text-blue-600 hover:text-blue-700 text-sm hover:underline">RPA Software Options</a></li>
              </ul>
            </div>
          </div>
        </section>

        {/* Back Button */}
        <div className="text-center">
          <a href="/" className="btn-primary inline-block px-8 py-3">
            ← Back to Dashboard
          </a>
        </div>
      </div>
    </div>
  );
}
