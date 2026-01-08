export default function PipelinePage() {
  return (
    <>
      {/* Page Title */}
      <div className="card-glass mb-8">
        <h1 className="text-4xl font-bold mb-4 text-gray-800">ETL Pipeline</h1>
        <p className="text-gray-600 text-lg">
          Extract, Transform, Load pipeline with automated validation and quality checks
        </p>
      </div>

      {/* Pipeline Stages */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Pipeline Stages</h2>
        <div className="space-y-4">
          <div className="p-6 bg-gradient-to-r from-blue-50 to-blue-100 rounded-lg border-l-4 border-blue-500">
            <div className="flex items-center gap-3 mb-3">
              <span className="text-3xl">📥</span>
              <h3 className="text-xl font-bold text-gray-800">1. Extract</h3>
            </div>
            <p className="text-gray-600 mb-4">
              Pull data from banking networks using web automation, REST APIs, SOAP, FIX, or ISO 20022
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Data Sources</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Clearing houses (ACH, SWIFT, FedWire)</li>
                  <li>• Payment processors (Visa, Mastercard, PayPal)</li>
                  <li>• Banking portals (web automation)</li>
                  <li>• Direct API integrations</li>
                </ul>
              </div>
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Extraction Methods</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Puppeteer/Playwright (web scraping)</li>
                  <li>• REST API calls with OAuth/JWT</li>
                  <li>• SOAP web services</li>
                  <li>• FIX Protocol messages</li>
                </ul>
              </div>
            </div>
          </div>

          <div className="p-6 bg-gradient-to-r from-purple-50 to-purple-100 rounded-lg border-l-4 border-purple-500">
            <div className="flex items-center gap-3 mb-3">
              <span className="text-3xl">✓</span>
              <h3 className="text-xl font-bold text-gray-800">2. Validate</h3>
            </div>
            <p className="text-gray-600 mb-4">
              Verify data quality, schema compliance, and business rules before processing
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Schema Validation</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Required field presence checks</li>
                  <li>• Data type validation (string, number, date)</li>
                  <li>• Format validation (email, phone, currency)</li>
                  <li>• Range and constraint validation</li>
                </ul>
              </div>
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Business Rules</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Transaction amount limits</li>
                  <li>• Date range validation (no future dates)</li>
                  <li>• Currency code verification (ISO 4217)</li>
                  <li>• Duplicate detection</li>
                </ul>
              </div>
            </div>
          </div>

          <div className="p-6 bg-gradient-to-r from-green-50 to-green-100 rounded-lg border-l-4 border-green-500">
            <div className="flex items-center gap-3 mb-3">
              <span className="text-3xl">🔄</span>
              <h3 className="text-xl font-bold text-gray-800">3. Transform</h3>
            </div>
            <p className="text-gray-600 mb-4">
              Normalize data to standard formats and apply mapping rules
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Normalization</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Date/time standardization (ISO 8601)</li>
                  <li>• Currency conversion to base currency</li>
                  <li>• Phone number formatting (E.164)</li>
                  <li>• Address parsing and geocoding</li>
                </ul>
              </div>
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Field Mapping</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Source to target field mapping</li>
                  <li>• Calculated fields (fees, totals)</li>
                  <li>• Enrichment from reference data</li>
                  <li>• Anonymization/masking for PII</li>
                </ul>
              </div>
            </div>
          </div>

          <div className="p-6 bg-gradient-to-r from-yellow-50 to-yellow-100 rounded-lg border-l-4 border-yellow-500">
            <div className="flex items-center gap-3 mb-3">
              <span className="text-3xl">💾</span>
              <h3 className="text-xl font-bold text-gray-800">4. Load</h3>
            </div>
            <p className="text-gray-600 mb-4">
              Write validated and transformed data to target storage systems
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Storage Targets</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• PostgreSQL (transactional data)</li>
                  <li>• Data Warehouse (analytics)</li>
                  <li>• Dynamic SL (archives and backups)</li>
                </ul>
              </div>
              <div>
                <h4 className="font-bold text-gray-700 mb-2">Load Strategies</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  <li>• Batch insert with transaction support</li>
                  <li>• Upsert (insert or update if exists)</li>
                  <li>• Incremental loading with watermarks</li>
                  <li>• Parallel loading for performance</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Data Validator */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Data Validator</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Validation Rules</h3>
            <div className="space-y-3 text-sm">
              <div className="p-3 bg-blue-50 rounded">
                <div className="font-bold text-gray-800">Required Fields</div>
                <div className="text-gray-600">Ensure critical fields are present and non-empty</div>
              </div>
              <div className="p-3 bg-purple-50 rounded">
                <div className="font-bold text-gray-800">Type Checking</div>
                <div className="text-gray-600">Validate data types match schema (string, number, boolean, date)</div>
              </div>
              <div className="p-3 bg-green-50 rounded">
                <div className="font-bold text-gray-800">Format Validation</div>
                <div className="text-gray-600">Verify formats (email, URL, phone, credit card)</div>
              </div>
              <div className="p-3 bg-yellow-50 rounded">
                <div className="font-bold text-gray-800">Range Constraints</div>
                <div className="text-gray-600">Check min/max values and string lengths</div>
              </div>
            </div>
          </div>

          <div className="p-5 bg-white rounded-lg border border-gray-200">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Error Handling</h3>
            <div className="space-y-3 text-sm">
              <div className="p-3 bg-red-50 rounded">
                <div className="font-bold text-gray-800">Field-Level Errors</div>
                <div className="text-gray-600">Detailed error messages for each invalid field</div>
              </div>
              <div className="p-3 bg-orange-50 rounded">
                <div className="font-bold text-gray-800">Error Aggregation</div>
                <div className="text-gray-600">Collect all errors before failing (not fail-fast)</div>
              </div>
              <div className="p-3 bg-yellow-50 rounded">
                <div className="font-bold text-gray-800">Quarantine Queue</div>
                <div className="text-gray-600">Invalid records moved to quarantine for manual review</div>
              </div>
              <div className="p-3 bg-blue-50 rounded">
                <div className="font-bold text-gray-800">Retry Logic</div>
                <div className="text-gray-600">Transient failures retried with exponential backoff</div>
              </div>
            </div>
          </div>
        </div>

        <div className="code-block">
          <div className="text-sm font-bold text-gray-700 mb-2">Validation Result Example</div>
          <code className="text-xs">
            {`{
  "isValid": false,
  "errors": [
    {
      "field": "amount",
      "message": "Amount must be greater than 0",
      "value": -100,
      "rule": "min"
    },
    {
      "field": "email",
      "message": "Invalid email format",
      "value": "invalid-email",
      "rule": "format"
    }
  ],
  "validRecords": 850,
  "invalidRecords": 12,
  "totalRecords": 862
}`}
          </code>
        </div>
      </div>

      {/* Transformation Engine */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Transformation Engine</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="p-5 bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Type Conversion</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• String to Number/Date parsing</li>
              <li>• Date format standardization</li>
              <li>• Boolean normalization</li>
              <li>• Null/undefined handling</li>
              <li>• Decimal precision rounding</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Field Mapping</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• One-to-one field mapping</li>
              <li>• Many-to-one aggregation</li>
              <li>• One-to-many splitting</li>
              <li>• Nested object flattening</li>
              <li>• Conditional mapping rules</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-br from-green-50 to-green-100 rounded-lg">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Data Enrichment</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Lookup from reference tables</li>
              <li>• Geocoding addresses</li>
              <li>• Currency conversion</li>
              <li>• Calculated fields</li>
              <li>• Data deduplication</li>
            </ul>
          </div>
        </div>

        <div className="mt-6 code-block">
          <div className="text-sm font-bold text-gray-700 mb-2">Transformation Configuration Example</div>
          <code className="text-xs">
            {`{
  "transformations": [
    {
      "type": "fieldMapping",
      "source": "transaction_amt",
      "target": "amount",
      "conversion": "toNumber"
    },
    {
      "type": "dateFormat",
      "source": "txn_date",
      "target": "transactionDate",
      "inputFormat": "MM/DD/YYYY",
      "outputFormat": "ISO8601"
    },
    {
      "type": "calculation",
      "target": "totalWithFees",
      "formula": "amount + processingFee"
    },
    {
      "type": "lookup",
      "source": "bank_code",
      "target": "bankName",
      "referenceTable": "banks",
      "lookupKey": "code",
      "returnField": "name"
    }
  ]
}`}
          </code>
        </div>
      </div>

      {/* Storage Manager */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Storage Manager</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-5 bg-white rounded-lg border-2 border-blue-500">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-2xl">🗄️</span>
              <h3 className="text-lg font-bold text-gray-800">PostgreSQL</h3>
            </div>
            <p className="text-sm text-gray-600 mb-3">
              Primary transactional database for real-time operational data
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• ACID transaction guarantees</li>
              <li>• Batch inserts with prepared statements</li>
              <li>• Connection pooling (pg-pool)</li>
              <li>• Automatic retry on deadlocks</li>
              <li>• Index optimization for queries</li>
            </ul>
          </div>

          <div className="p-5 bg-white rounded-lg border-2 border-purple-500">
            <div className="flex items-center gap-2 mb-3">
              <span className="text-2xl">📊</span>
              <h3 className="text-lg font-bold text-gray-800">Data Warehouse</h3>
            </div>
            <p className="text-sm text-gray-600 mb-3">
              Analytical database for reporting and business intelligence
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Star/snowflake schema design</li>
              <li>• Columnar storage for analytics</li>
              <li>• Incremental ETL with CDC</li>
              <li>• Partitioning by date/region</li>
              <li>• Materialized views for aggregations</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Pipeline Monitoring */}
      <div className="card-glass">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Pipeline Monitoring & Metrics</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="stat-card">
            <div className="stat-number">99.9%</div>
            <div className="stat-label">Success Rate</div>
          </div>
          <div className="stat-card">
            <div className="stat-number">2.3s</div>
            <div className="stat-label">Avg Processing Time</div>
          </div>
          <div className="stat-card">
            <div className="stat-number">10K/min</div>
            <div className="stat-label">Record Throughput</div>
          </div>
          <div className="stat-card">
            <div className="stat-number">0.1%</div>
            <div className="stat-label">Error Rate</div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Real-time Metrics</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>Records processed per second</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>Pipeline stage durations</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>Validation success/failure rates</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>Storage write latency</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-blue-600">•</span>
                <span>Queue depth and backlog</span>
              </li>
            </ul>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">WebSocket Events</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span><code className="text-xs bg-gray-100 px-1 rounded">pipeline:started</code> - Pipeline execution begins</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span><code className="text-xs bg-gray-100 px-1 rounded">pipeline:stage-change</code> - Stage transition events</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span><code className="text-xs bg-gray-100 px-1 rounded">pipeline:batch-progress</code> - Batch processing updates</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span><code className="text-xs bg-gray-100 px-1 rounded">pipeline:completed</code> - Successful completion</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-purple-600">•</span>
                <span><code className="text-xs bg-gray-100 px-1 rounded">pipeline:failed</code> - Pipeline errors</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </>
  );
}
