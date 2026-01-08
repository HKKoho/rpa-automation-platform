export default function ArchitecturePage() {
  return (
    <>
      {/* Page Title */}
      <div className="card-glass mb-8">
        <h1 className="text-4xl font-bold mb-4 text-gray-800">System Architecture</h1>
        <p className="text-gray-600 text-lg">
          Four-tier AI-enhanced enterprise architecture designed for scalability, security, and intelligent automation
        </p>
      </div>

      {/* Architecture Diagram */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Four-Tier AI Architecture (v2.0.0)</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {/* Tier 1: Frontend */}
          <div className="feature-card border-l-4 border-blue-500">
            <div className="text-3xl mb-4">🖥️</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">Frontend Layer</h3>
            <p className="text-sm text-gray-600 mb-4">
              Next.js 14 with App Router and React Server Components
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>Pages in src/app/ with RSC</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>API routes in src/app/api/</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>WebSocket real-time updates</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>TailwindCSS utility classes</span>
              </li>
            </ul>
          </div>

          {/* Tier 2: AI Services Layer */}
          <div className="feature-card border-l-4 border-purple-500 bg-purple-50">
            <div className="text-3xl mb-4">🧠</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">AI Services Layer</h3>
            <p className="text-sm text-gray-600 mb-4">
              Hybrid AI combining computer vision with generative AI
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span>DynamicElementDetector (TensorFlow.js)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span>AdaptiveSelectorAI (LLM)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span>DataNormalizerAI (LLM)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span>GPT-4, Gemini, DeepSeek, Local LLMs</span>
              </li>
            </ul>
          </div>

          {/* Tier 3: Backend Core Services */}
          <div className="feature-card border-l-4 border-orange-500">
            <div className="text-3xl mb-4">⚙️</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">Backend Core</h3>
            <p className="text-sm text-gray-600 mb-4">
              Orchestration, security, and data processing
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-orange-600">•</span>
                <span>RPAEngine (orchestrator)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-orange-600">•</span>
                <span>CredentialVault (AES-256-GCM)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-orange-600">•</span>
                <span>ETL Pipeline (AI-enhanced)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-orange-600">•</span>
                <span>WebAutomation (AI-assisted)</span>
              </li>
            </ul>
          </div>

          {/* Tier 4: Worker Processes */}
          <div className="feature-card border-l-4 border-green-500">
            <div className="text-3xl mb-4">🔄</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800">Worker Layer</h3>
            <p className="text-sm text-gray-600 mb-4">
              Background job processing and scheduling
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-green-600">•</span>
                <span>Extraction worker (BullMQ)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">•</span>
                <span>Scheduler worker (cron)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">•</span>
                <span>WebSocket server (real-time)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">•</span>
                <span>Redis queue management</span>
              </li>
            </ul>
          </div>
        </div>
      </div>

      {/* Data Flow */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Data Flow Architecture</h2>
        <div className="space-y-4">
          <div className="p-4 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border-l-4 border-blue-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">1️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">User Request (Frontend)</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              User initiates action via React components → Next.js API routes
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-purple-50 to-purple-100 rounded-lg border-l-4 border-purple-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">2️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">Backend Core Services</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              RPAEngine processes request → CredentialVault retrieves credentials → Queue job
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border-l-4 border-green-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">3️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">Queue System (BullMQ + Redis)</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              Job queued with priority and retry logic → Distributed across workers
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-yellow-50 to-yellow-100 rounded-lg border-l-4 border-yellow-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">4️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">Worker Processes</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              Extraction worker executes → Connects to banking networks → Extracts data
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-red-50 to-red-100 rounded-lg border-l-4 border-red-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">5️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">Banking Network Sources</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              Payment processors (Visa, Mastercard, PayPal, Stripe) • Direct bank access (web automation, email/SMS download, manual OCR) • Shared infrastructure (FIS Global, Fiserv - Professional+ tiers)
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-indigo-50 to-indigo-100 rounded-lg border-l-4 border-indigo-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">6️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">ETL Pipeline</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              Validate data quality → Transform to standard format → Load to storage
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-pink-50 to-pink-100 rounded-lg border-l-4 border-pink-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">7️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">Data Storage</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              PostgreSQL (transactional) → Microsoft Dynamic SL (storage or archives) or Microsoft Dynamics 365 (in future)
            </p>
          </div>

          <div className="p-4 bg-gradient-to-r from-teal-50 to-teal-100 rounded-lg border-l-4 border-teal-500">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-2xl">8️⃣</span>
              <h3 className="text-lg font-bold text-gray-800">WebSocket Real-time Updates</h3>
            </div>
            <p className="text-sm text-gray-600 ml-10">
              Progress events broadcast → Frontend receives updates → UI automatically refreshes
            </p>
          </div>
        </div>
      </div>

      {/* Core Components */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Core Components</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-blue-600">⚡</span>
              RPAEngine
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Central orchestration engine for job scheduling and management
            </p>
            <div className="code-block">
              <code className="text-xs">
                const engine = new RPAEngine();<br/>
                await engine.scheduleJob(config);<br/>
                await engine.extractData(params);
              </code>
            </div>
          </div>

          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-purple-600">🔐</span>
              CredentialVault
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Encrypted credential storage with AES-256-GCM encryption
            </p>
            <div className="code-block">
              <code className="text-xs">
                const vault = new CredentialVault();<br/>
                await vault.store(id, data, options);<br/>
                await vault.retrieve(id);
              </code>
            </div>
          </div>

          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800 flex items-center gap-2">
              <span className="text-green-600">🔄</span>
              ETLPipeline
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Extract, transform, load pipeline for data processing
            </p>
            <div className="code-block">
              <code className="text-xs">
                const pipeline = new ETLPipeline();<br/>
                const result = await pipeline.process(data);<br/>
                // validate → transform → load
              </code>
            </div>
          </div>

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
                const automation = new WebAutomation();<br/>
                await automation.navigate(url);<br/>
                await automation.fillForm(data);
              </code>
            </div>
          </div>
        </div>
      </div>

      {/* Technology Stack */}
      <div className="card-glass">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Infrastructure & Dependencies</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Frontend</h3>
            <ul className="space-y-2 text-sm text-gray-600">
              <li className="flex items-center gap-2">
                <span className="text-blue-600">▸</span>
                Next.js 14 (App Router)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-blue-600">▸</span>
                React 18 (Server Components)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-blue-600">▸</span>
                TailwindCSS
              </li>
              <li className="flex items-center gap-2">
                <span className="text-blue-600">▸</span>
                TypeScript
              </li>
            </ul>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Backend</h3>
            <ul className="space-y-2 text-sm text-gray-600">
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                Node.js 18+
              </li>
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                BullMQ (queue system)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                Puppeteer/Playwright
              </li>
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                Puppeteer/Playwright
              </li>
            </ul>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">AI & Machine Learning</h3>
            <ul className="space-y-2 text-sm text-gray-600">
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                TensorFlow.js (local CV)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                OpenAI GPT-4
              </li>
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                Google Gemini
              </li>
              <li className="flex items-center gap-2">
                <span className="text-purple-600">▸</span>
                DeepSeek / Local Llama 3
              </li>
            </ul>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Infrastructure</h3>
            <ul className="space-y-2 text-sm text-gray-600">
              <li className="flex items-center gap-2">
                <span className="text-green-600">▸</span>
                PostgreSQL
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-600">▸</span>
                Redis
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-600">▸</span>
                Dynamics 365 Business Central
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-600">▸</span>
                Microsoft Azure
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-600">▸</span>
                Power BI Premium
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-600">▸</span>
                Oracle Finance (Alternative)
              </li>
            </ul>
          </div>
        </div>
      </div>
    </>
  );
}
