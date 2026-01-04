export default function HomePage() {
  return (
    <>
      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <div className="stat-card">
          <div className="stat-number">24/7</div>
          <div className="stat-label">Automation</div>
        </div>
        <div className="stat-card">
          <div className="stat-number">4-Layer</div>
          <div className="stat-label">AI Architecture</div>
        </div>
        <div className="stat-card">
          <div className="stat-number">Hybrid AI</div>
          <div className="stat-label">CV + GenAI</div>
        </div>
        <div className="stat-card">
          <div className="stat-number">Enterprise</div>
          <div className="stat-label">Security</div>
        </div>
      </div>

      {/* Data Flow Diagram */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">🔄 Automated Data Flow</h2>
        <div className="flow-diagram">
          <div className="flow-step">Data Sources</div>
          <div className="flow-step">Extraction</div>
          <div className="flow-step">Validation</div>
          <div className="flow-step">Transformation</div>
          <div className="flow-step">Loading</div>
          <div className="flow-step">Analytics</div>
        </div>
      </div>

      {/* Core Capabilities */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">💡 Core Capabilities</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="feature-card">
            <div className="feature-icon">🤖</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">Web Automation</h3>
            <p className="text-gray-600 text-sm mb-4">
              Headless browser automation using Puppeteer/Playwright with intelligent form detection and AI-powered element selection
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Multi-tab session management</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Dynamic element detection</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Screenshot & PDF capture</span>
              </li>
            </ul>
          </div>

          <div className="feature-card">
            <div className="feature-icon">🧠</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">Hybrid AI Integration</h3>
            <p className="text-gray-600 text-sm mb-4">
              Combines TensorFlow.js computer vision with generative AI (GPT-4, Gemini, DeepSeek) for intelligent automation
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Adaptive selector recovery</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Smart data normalization</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Natural language interface</span>
              </li>
            </ul>
          </div>

          <div className="feature-card">
            <div className="feature-icon">🔒</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">Enterprise Security</h3>
            <p className="text-gray-600 text-sm mb-4">
              Bank-grade security with AES-256-GCM encryption, credential vaulting, and comprehensive audit logging
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>End-to-end encryption</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Role-based access control</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Compliance ready (PCI-DSS, GDPR)</span>
              </li>
            </ul>
          </div>
        </div>
      </div>

      {/* Technology Stack */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">⚙️ Technology Stack</h2>
        <div className="space-y-4">
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-2">Core Technologies</h3>
            <div className="flex flex-wrap gap-3">
              <span className="tech-tag">TypeScript</span>
              <span className="tech-tag">Next.js 14</span>
              <span className="tech-tag">React</span>
              <span className="tech-tag">Node.js</span>
              <span className="tech-tag">Puppeteer</span>
              <span className="tech-tag">Playwright</span>
            </div>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-2">AI & Machine Learning</h3>
            <div className="flex flex-wrap gap-3">
              <span className="tech-tag bg-purple-100 text-purple-700">TensorFlow.js</span>
              <span className="tech-tag bg-purple-100 text-purple-700">OpenAI GPT-4</span>
              <span className="tech-tag bg-purple-100 text-purple-700">Google Gemini</span>
              <span className="tech-tag bg-purple-100 text-purple-700">DeepSeek</span>
              <span className="tech-tag bg-purple-100 text-purple-700">Local Llama 3</span>
            </div>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-2">Infrastructure</h3>
            <div className="flex flex-wrap gap-3">
              <span className="tech-tag">BullMQ</span>
              <span className="tech-tag">Redis</span>
              <span className="tech-tag">PostgreSQL</span>
              <span className="tech-tag">BigQuery</span>
              <span className="tech-tag">Docker</span>
              <span className="tech-tag">Kubernetes</span>
              <span className="tech-tag">Google Cloud</span>
            </div>
          </div>
        </div>
      </div>

      {/* Banking Networks Supported */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">🏦 Banking Networks Supported</h2>
        <div>
          <h3 className="text-lg font-semibold mb-4 text-gray-700">Payment Processors & Infrastructure</h3>
          <ul className="space-y-3">
            <li className="flex items-start gap-3 p-3 bg-purple-50 rounded-lg">
              <span className="text-2xl">💳</span>
              <div>
                <div className="font-medium text-gray-800">Visa & Mastercard</div>
                <div className="text-sm text-gray-600">Global card payment networks</div>
              </div>
            </li>
            <li className="flex items-start gap-3 p-3 bg-purple-50 rounded-lg">
              <span className="text-2xl">💰</span>
              <div>
                <div className="font-medium text-gray-800">PayPal & Stripe</div>
                <div className="text-sm text-gray-600">Online payment processors</div>
              </div>
            </li>
            <li className="flex items-start gap-3 p-3 bg-purple-50 rounded-lg">
              <span className="text-2xl">🔧</span>
              <div>
                <div className="font-medium text-gray-800">FIS Global & Fiserv</div>
                <div className="text-sm text-gray-600">Shared banking infrastructure</div>
              </div>
            </li>
            <li className="flex items-start gap-3 p-3 bg-purple-50 rounded-lg">
              <span className="text-2xl">🌐</span>
              <div>
                <div className="font-medium text-gray-800">Direct Bank Websites</div>
                <div className="text-sm text-gray-600">Web automation templates</div>
              </div>
            </li>
          </ul>
        </div>
      </div>

      {/* Documentation Links */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">📚 Documentation & Resources</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <a href="/docs/networks" className="block p-6 bg-blue-50 hover:bg-blue-100 rounded-lg border-2 border-blue-200 hover:border-blue-400 transition-all group">
            <div className="text-3xl mb-3">🏦</div>
            <h3 className="font-bold text-gray-800 mb-2 group-hover:text-blue-600">Banking Networks</h3>
            <p className="text-sm text-gray-600">Complete list of supported networks and integration options</p>
          </a>

          <a href="/docs/setup" className="block p-6 bg-green-50 hover:bg-green-100 rounded-lg border-2 border-green-200 hover:border-green-400 transition-all group">
            <div className="text-3xl mb-3">🔧</div>
            <h3 className="font-bold text-gray-800 mb-2 group-hover:text-green-600">Setup Guide</h3>
            <p className="text-sm text-gray-600">BigQuery & Power BI integration instructions</p>
          </a>

          <a href="/docs/pricing" className="block p-6 bg-purple-50 hover:bg-purple-100 rounded-lg border-2 border-purple-200 hover:border-purple-400 transition-all group">
            <div className="text-3xl mb-3">💰</div>
            <h3 className="font-bold text-gray-800 mb-2 group-hover:text-purple-600">Pricing & Budget</h3>
            <p className="text-sm text-gray-600">Deployment tiers and cost estimates</p>
          </a>

          <a href="/docs/integrations" className="block p-6 bg-orange-50 hover:bg-orange-100 rounded-lg border-2 border-orange-200 hover:border-orange-400 transition-all group">
            <div className="text-3xl mb-3">🤖</div>
            <h3 className="font-bold text-gray-800 mb-2 group-hover:text-orange-600">RPA Integrations</h3>
            <p className="text-sm text-gray-600">UiPath & Robocorp software options</p>
          </a>
        </div>
      </div>

      {/* System Information */}
      <div className="card-glass">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">ℹ️ System Information</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Current Status</h3>
            <div className="space-y-3">
              <div className="flex items-center justify-between p-3 bg-green-50 rounded-lg border border-green-200">
                <div className="flex items-center gap-3">
                  <div className="w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
                  <span className="font-medium text-gray-800">Next.js Server</span>
                </div>
                <span className="badge-success">Running</span>
              </div>
              <div className="flex items-center justify-between p-3 bg-blue-50 rounded-lg border border-blue-200">
                <div className="flex items-center gap-3">
                  <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
                  <span className="font-medium text-gray-800">Backend Services</span>
                </div>
                <span className="badge-info">Ready</span>
              </div>
              <div className="flex items-center justify-between p-3 bg-yellow-50 rounded-lg border border-yellow-200">
                <div className="flex items-center gap-3">
                  <div className="w-3 h-3 bg-yellow-500 rounded-full"></div>
                  <span className="font-medium text-gray-800">WebSocket Server</span>
                </div>
                <span className="badge-warning">Start with npm run websocket</span>
              </div>
            </div>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Quick Info</h3>
            <div className="space-y-3 text-sm">
              <div className="p-3 bg-gray-50 rounded-lg">
                <div className="font-medium text-gray-700 mb-1">Version</div>
                <div className="text-gray-600">2.0.0 (AI-Enhanced)</div>
              </div>
              <div className="p-3 bg-gray-50 rounded-lg">
                <div className="font-medium text-gray-700 mb-1">Environment</div>
                <div className="text-gray-600">Development</div>
              </div>
              <div className="p-3 bg-gray-50 rounded-lg">
                <div className="font-medium text-gray-700 mb-1">Framework</div>
                <div className="text-gray-600">Next.js 14 (App Router)</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
