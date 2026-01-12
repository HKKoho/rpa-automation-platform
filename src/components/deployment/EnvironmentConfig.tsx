'use client';

import { useState } from 'react';

export default function EnvironmentConfig() {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div className="card-glass mb-8">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between text-left hover:opacity-80 transition-opacity"
      >
        <h2 className="text-2xl font-bold text-gray-800">Environment Configuration</h2>
        <span className="text-2xl text-gray-600">
          {isExpanded ? '▼' : '▶'}
        </span>
      </button>

      {isExpanded && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Production .env</h3>
            <div className="code-block">
              <code className="text-xs">
                {`# Application
NODE_ENV=production
NEXT_PUBLIC_APP_URL=https://rpa.example.com
NEXT_PUBLIC_WS_URL=wss://rpa.example.com/ws

# Database
DATABASE_URL=postgresql://user:pass@postgres:5432/rpa_platform
DATABASE_POOL_SIZE=20
DATABASE_SSL=true

# Redis
REDIS_HOST=redis-master
REDIS_PORT=6379
REDIS_PASSWORD=strong_password
REDIS_TLS=true

# Security
ENCRYPTION_MASTER_KEY=64_char_hex_key_here
JWT_SECRET=strong_jwt_secret
SESSION_SECRET=strong_session_secret

# Audit Logging
AUDIT_LOG_DIR=/var/log/rpa/audit
AUDIT_LOG_RETENTION_DAYS=2555

# Compliance
COMPLIANCE_MODE=PCI-DSS,SOC2
ENABLE_AUDIT_ENCRYPTION=true

# AI Features
ENABLE_AI_DETECTION=true
TENSORFLOW_BACKEND=cpu

# Monitoring
SENTRY_DSN=https://...@sentry.io/...
DATADOG_API_KEY=your_datadog_key

# Email Notifications
SMTP_HOST=smtp.sendgrid.net
SMTP_PORT=587
SMTP_USER=apikey
SMTP_PASSWORD=SG.xxx`}
              </code>
            </div>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-3 text-gray-700">Infrastructure Checklist</h3>
            <div className="space-y-3">
              <div className="p-3 bg-green-50 rounded border-l-4 border-green-500">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-green-600">✓</span>
                  <strong className="text-gray-800">Database Backups</strong>
                </div>
                <p className="text-sm text-gray-600">Automated daily backups with 30-day retention</p>
              </div>

              <div className="p-3 bg-green-50 rounded border-l-4 border-green-500">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-green-600">✓</span>
                  <strong className="text-gray-800">SSL Certificates</strong>
                </div>
                <p className="text-sm text-gray-600">Valid SSL/TLS certificates for all endpoints</p>
              </div>

              <div className="p-3 bg-green-50 rounded border-l-4 border-green-500">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-green-600">✓</span>
                  <strong className="text-gray-800">Monitoring & Alerts</strong>
                </div>
                <p className="text-sm text-gray-600">Uptime monitoring, error tracking, and alerting</p>
              </div>

              <div className="p-3 bg-green-50 rounded border-l-4 border-green-500">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-green-600">✓</span>
                  <strong className="text-gray-800">Log Aggregation</strong>
                </div>
                <p className="text-sm text-gray-600">Centralized logging with retention policies</p>
              </div>

              <div className="p-3 bg-green-50 rounded border-l-4 border-green-500">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-green-600">✓</span>
                  <strong className="text-gray-800">Security Scanning</strong>
                </div>
                <p className="text-sm text-gray-600">Regular vulnerability scans and penetration testing</p>
              </div>

              <div className="p-3 bg-green-50 rounded border-l-4 border-green-500">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-green-600">✓</span>
                  <strong className="text-gray-800">Disaster Recovery</strong>
                </div>
                <p className="text-sm text-gray-600">Multi-region failover and backup restoration plan</p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
