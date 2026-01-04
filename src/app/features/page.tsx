export default function FeaturesPage() {
  return (
    <>
      {/* Page Title */}
      <div className="card-glass mb-8">
        <h1 className="text-4xl font-bold mb-4 text-gray-800">Platform Features</h1>
        <p className="text-gray-600 text-lg">
          Comprehensive RPA capabilities for enterprise banking operations
        </p>
      </div>

      {/* Core Features Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        {/* Web Automation */}
        <div className="feature-card">
          <div className="feature-icon">🤖</div>
          <h3 className="text-xl font-bold mb-3 text-gray-800">Web Automation</h3>
          <p className="text-gray-600 text-sm mb-4">
            Headless browser automation with intelligent form detection and AI-powered element selection
          </p>
          <ul className="text-sm text-gray-600 space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Puppeteer & Playwright support for cross-browser automation</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Multi-tab session management with cookie persistence</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Dynamic element detection using CSS selectors and XPath</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Screenshot and PDF capture for audit trails</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Automatic retry logic with exponential backoff</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>CAPTCHA detection and solving integration</span>
            </li>
          </ul>
        </div>

        {/* API Integration */}
        <div className="feature-card">
          <div className="feature-icon">🔌</div>
          <h3 className="text-xl font-bold mb-3 text-gray-800">API Integration</h3>
          <p className="text-gray-600 text-sm mb-4">
            Multi-protocol API extraction supporting REST, SOAP, FIX, and ISO 20022
          </p>
          <ul className="text-sm text-gray-600 space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>REST API with OAuth 2.0, JWT, and API key authentication</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>SOAP/XML web services with WSDL parsing</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>FIX Protocol for financial message exchange</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>ISO 20022 XML messaging (SWIFT MX messages)</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Rate limiting and throttling controls</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Webhook support for event-driven data push</span>
            </li>
          </ul>
        </div>

        {/* AI-Powered Extraction */}
        <div className="feature-card">
          <div className="feature-icon">🧠</div>
          <h3 className="text-xl font-bold mb-3 text-gray-800">AI Integration</h3>
          <p className="text-gray-600 text-sm mb-4">
            TensorFlow.js powered computer vision for intelligent data extraction
          </p>
          <ul className="text-sm text-gray-600 space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Layout analysis and visual pattern recognition</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>OCR (Optical Character Recognition) for text extraction</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Adaptive element detection that learns from failures</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Table structure detection and data extraction</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>CAPTCHA solving with ML models</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Anomaly detection for data quality monitoring</span>
            </li>
          </ul>
        </div>

        {/* Job Scheduling */}
        <div className="feature-card">
          <div className="feature-icon">⏰</div>
          <h3 className="text-xl font-bold mb-3 text-gray-800">Job Scheduling</h3>
          <p className="text-gray-600 text-sm mb-4">
            Flexible cron-based scheduling with distributed queue management
          </p>
          <ul className="text-sm text-gray-600 space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Cron expressions for complex scheduling patterns</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>BullMQ-powered distributed job queue with Redis</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Job priority and concurrency limits</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Automatic retry with exponential backoff</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Job dependencies and conditional execution</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Real-time job monitoring and progress tracking</span>
            </li>
          </ul>
        </div>

        {/* Security & Compliance */}
        <div className="feature-card">
          <div className="feature-icon">🔒</div>
          <h3 className="text-xl font-bold mb-3 text-gray-800">Enterprise Security</h3>
          <p className="text-gray-600 text-sm mb-4">
            Bank-grade security with encryption, vaulting, and audit logging
          </p>
          <ul className="text-sm text-gray-600 space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>AES-256-GCM encryption for credential storage</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>PBKDF2 key derivation (100,000 iterations)</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Automatic credential rotation and expiration</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Immutable audit logs with tamper detection</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>PCI-DSS, GDPR, SOC2, ISO27001 compliance modes</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Role-based access control (RBAC)</span>
            </li>
          </ul>
        </div>

        {/* Data Pipeline */}
        <div className="feature-card">
          <div className="feature-icon">🔄</div>
          <h3 className="text-xl font-bold mb-3 text-gray-800">ETL Pipeline</h3>
          <p className="text-gray-600 text-sm mb-4">
            Automated Extract, Transform, Load pipeline with validation
          </p>
          <ul className="text-sm text-gray-600 space-y-2">
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Schema validation with detailed error reporting</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Data type conversion and normalization</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Field mapping and transformation rules</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Batch processing with progress tracking</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Multi-destination loading (PostgreSQL, S3, Data Warehouse)</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-600">✓</span>
              <span>Data quality metrics and monitoring</span>
            </li>
          </ul>
        </div>
      </div>

      {/* Authentication Methods */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Supported Authentication Methods</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-5 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border-l-4 border-blue-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">OAuth 2.0</h3>
            <p className="text-sm text-gray-600 mb-3">
              Token-based authentication with automatic refresh
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Authorization Code Flow</li>
              <li>• Client Credentials Flow</li>
              <li>• Token rotation and expiration</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-r from-purple-50 to-purple-100 rounded-lg border-l-4 border-purple-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Multi-Factor Authentication</h3>
            <p className="text-sm text-gray-600 mb-3">
              Support for various MFA methods
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• TOTP (Time-based One-Time Password)</li>
              <li>• SMS/Email verification codes</li>
              <li>• Hardware token support</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border-l-4 border-green-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">API Keys</h3>
            <p className="text-sm text-gray-600 mb-3">
              Custom header-based authentication
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• X-API-Key headers</li>
              <li>• Custom authentication headers</li>
              <li>• Key rotation support</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-r from-yellow-50 to-yellow-100 rounded-lg border-l-4 border-yellow-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Certificate-Based</h3>
            <p className="text-sm text-gray-600 mb-3">
              X.509 certificates and mutual TLS
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Client certificate authentication</li>
              <li>• Mutual TLS (mTLS)</li>
              <li>• Certificate chain validation</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Real-time Features */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Real-time Capabilities</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="text-center p-6">
            <div className="text-4xl mb-4">📡</div>
            <h3 className="text-lg font-bold mb-2 text-gray-800">WebSocket Events</h3>
            <p className="text-sm text-gray-600">
              Live updates for job status, extraction progress, and pipeline stages
            </p>
          </div>

          <div className="text-center p-6">
            <div className="text-4xl mb-4">📊</div>
            <h3 className="text-lg font-bold mb-2 text-gray-800">Progress Tracking</h3>
            <p className="text-sm text-gray-600">
              Real-time progress bars and status indicators for all operations
            </p>
          </div>

          <div className="text-center p-6">
            <div className="text-4xl mb-4">🔔</div>
            <h3 className="text-lg font-bold mb-2 text-gray-800">Notifications</h3>
            <p className="text-sm text-gray-600">
              Toast notifications for job completion, errors, and system alerts
            </p>
          </div>
        </div>
      </div>

      {/* Banking Network Support */}
      <div className="card-glass">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Banking Network Coverage</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="p-4 bg-red-50 rounded-lg text-center">
            <div className="text-3xl mb-2">💳</div>
            <div className="font-bold text-gray-800">Visa/Mastercard</div>
            <div className="text-xs text-gray-600">Card Networks</div>
          </div>
          <div className="p-4 bg-indigo-50 rounded-lg text-center">
            <div className="text-3xl mb-2">💰</div>
            <div className="font-bold text-gray-800">PayPal/Stripe</div>
            <div className="text-xs text-gray-600">Online Processors</div>
          </div>
          <div className="p-4 bg-pink-50 rounded-lg text-center">
            <div className="text-3xl mb-2">🔧</div>
            <div className="font-bold text-gray-800">FIS/Fiserv</div>
            <div className="text-xs text-gray-600">Infrastructure</div>
          </div>
          <div className="p-4 bg-teal-50 rounded-lg text-center">
            <div className="text-3xl mb-2">🌐</div>
            <div className="font-bold text-gray-800">Direct Banks</div>
            <div className="text-xs text-gray-600">Web Automation</div>
          </div>
        </div>
      </div>
    </>
  );
}
