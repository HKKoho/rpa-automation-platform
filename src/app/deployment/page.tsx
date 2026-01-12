import EnvironmentConfig from '@/components/deployment/EnvironmentConfig';
import CICDPipeline from '@/components/deployment/CICDPipeline';

export default function DeploymentPage() {
  return (
    <>
      {/* Page Title */}
      <div className="card-glass mb-8">
        <h1 className="text-4xl font-bold mb-4 text-gray-800">Deployment Guide</h1>
        <p className="text-gray-600 text-lg">
          Production deployment strategies and infrastructure configuration
        </p>
      </div>

      {/* Deployment Options */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Deployment Options</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="feature-card border-t-4 border-orange-500">
            <div className="text-4xl mb-4 text-center">🖥️</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800 text-center">Local Server</h3>
            <p className="text-sm text-gray-600 text-center mb-4">
              On-premise deployment with local infrastructure
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Windows/Linux server deployment</li>
              <li>• Microsoft Dynamic SL integration</li>
              <li>• Standalone Power BI Desktop/Pro</li>
              <li>• Local PostgreSQL database</li>
              <li>• AI Engine Server (GPU optional)</li>
              <li>• Full data sovereignty & control</li>
            </ul>
          </div>

          <div className="feature-card border-t-4 border-blue-500">
            <div className="text-4xl mb-4 text-center">☁️</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800 text-center">Microsoft Azure</h3>
            <p className="text-sm text-gray-600 text-center mb-4">
              Cloud deployment on Microsoft Azure for Dynamics 365 integration
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Azure App Service for RPA platform</li>
              <li>• Native Dynamics 365 connectivity</li>
              <li>• Azure SQL Database / PostgreSQL</li>
              <li>• Azure Cache for Redis</li>
              <li>• Power BI Premium integration</li>
            </ul>
          </div>

          <div className="feature-card border-t-4 border-purple-500">
            <div className="text-4xl mb-4 text-center">▲</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800 text-center">Vercel</h3>
            <p className="text-sm text-gray-600 text-center mb-4">
              Fast deployment for Next.js RPA platform frontend
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Instant deployment from GitHub</li>
              <li>• Global CDN with edge functions</li>
              <li>• Automatic HTTPS and SSL</li>
              <li>• Preview deployments for testing</li>
              <li>• Zero-config Next.js optimization</li>
            </ul>
          </div>

          <div className="feature-card border-t-4 border-green-500">
            <div className="text-4xl mb-4 text-center">🔶</div>
            <h3 className="text-xl font-bold mb-3 text-gray-800 text-center">Oracle Cloud</h3>
            <p className="text-sm text-gray-600 text-center mb-4">
              Alternative deployment for Oracle Finance integration
            </p>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Oracle Cloud Infrastructure (OCI)</li>
              <li>• Native Oracle Finance connectivity</li>
              <li>• Oracle Autonomous Database</li>
              <li>• OCI Container Engine</li>
              <li>• Oracle Analytics Cloud</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Local Server Deployment Details */}
      <div className="card-glass mb-8">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Local Server Deployment Architecture</h2>
        <p className="text-gray-600 mb-6">
          On-premise deployment option for organizations requiring full data sovereignty, air-gapped environments, or integration with existing local infrastructure.
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
          <div className="p-5 bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg border-l-4 border-blue-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Server Components</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• <strong>RPA Application Server</strong> - Node.js runtime</li>
              <li>• <strong>PostgreSQL Database</strong> - Transactional data storage</li>
              <li>• <strong>Redis Server</strong> - Queue management & caching</li>
              <li>• <strong>Microsoft Dynamic SL</strong> - ERP integration</li>
              <li>• <strong>AI Engine Server</strong> - Computer vision & NLP processing</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg border-l-4 border-purple-500">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Analytics & Reporting</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• <strong>Power BI Desktop</strong> - Local report development</li>
              <li>• <strong>Power BI Pro</strong> - Team collaboration (optional)</li>
              <li>• <strong>Direct Query</strong> - Connect to PostgreSQL</li>
              <li>• <strong>Scheduled Refresh</strong> - Automated data updates</li>
              <li>• <strong>Custom Dashboards</strong> - Tailored analytics</li>
            </ul>
          </div>
        </div>

        <div className="p-5 bg-gradient-to-br from-green-50 to-green-100 rounded-lg border-l-4 border-green-500 mb-6">
          <h3 className="text-lg font-bold mb-3 text-gray-800">AI Engine Server Specifications</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <h4 className="font-semibold text-gray-700 mb-2 text-sm">CPU-Based (Budget Option)</h4>
              <ul className="text-sm text-gray-600 space-y-1">
                <li>• Intel Xeon or AMD EPYC processor</li>
                <li>• 16+ CPU cores recommended</li>
                <li>• 32GB+ RAM for TensorFlow.js</li>
                <li>• Suitable for light workloads</li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold text-gray-700 mb-2 text-sm">GPU-Accelerated (Performance)</h4>
              <ul className="text-sm text-gray-600 space-y-1">
                <li>• NVIDIA GPU with CUDA support</li>
                <li>• 8GB+ VRAM (e.g., RTX 3060, A4000)</li>
                <li>• 10x faster AI processing</li>
                <li>• Recommended for production</li>
              </ul>
            </div>
          </div>
        </div>

        <div className="p-5 bg-yellow-50 rounded-lg border-l-4 border-yellow-500">
          <h3 className="text-lg font-bold mb-2 text-gray-800">Minimum Hardware Requirements</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
            <div>
              <h4 className="font-semibold text-gray-700 mb-2">Starter Tier</h4>
              <ul className="text-gray-600 space-y-1">
                <li>• 4 CPU cores</li>
                <li>• 16GB RAM</li>
                <li>• 500GB SSD</li>
                <li>• 1Gbps network</li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold text-gray-700 mb-2">Professional Tier</h4>
              <ul className="text-gray-600 space-y-1">
                <li>• 8 CPU cores</li>
                <li>• 32GB RAM</li>
                <li>• 1TB SSD</li>
                <li>• 10Gbps network</li>
              </ul>
            </div>
            <div>
              <h4 className="font-semibold text-gray-700 mb-2">Enterprise Tier</h4>
              <ul className="text-gray-600 space-y-1">
                <li>• 16+ CPU cores</li>
                <li>• 64GB+ RAM</li>
                <li>• 2TB+ NVMe SSD</li>
                <li>• 10Gbps+ network</li>
              </ul>
            </div>
          </div>
        </div>
      </div>

      {/* Docker Setup - Hidden for production guide */}
      <div className="card-glass mb-8" style={{ display: 'none' }}>
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Docker Deployment</h2>
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-3 text-gray-700">Dockerfile</h3>
          <div className="code-block">
            <code className="text-sm">
              {`# Multi-stage build for optimization
FROM node:18-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source code
COPY . .

# Build Next.js application
RUN npm run build

# Production image
FROM node:18-alpine AS runner

WORKDIR /app

# Create non-root user
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Copy built application
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

# Set ownership
RUN chown -R nextjs:nodejs /app

# Switch to non-root user
USER nextjs

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \\
  CMD node healthcheck.js || exit 1

# Start application
CMD ["node", "server.js"]`}
            </code>
          </div>
        </div>

        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-3 text-gray-700">docker-compose.yml</h3>
          <div className="code-block">
            <code className="text-sm">
              {`version: '3.8'

services:
  # Next.js Application
  app:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      - NODE_ENV=production
      - DATABASE_URL=\${DATABASE_URL}
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - ENCRYPTION_MASTER_KEY=\${ENCRYPTION_MASTER_KEY}
    depends_on:
      - postgres
      - redis
    restart: unless-stopped
    networks:
      - rpa-network

  # WebSocket Server
  websocket:
    build:
      context: .
      dockerfile: Dockerfile.websocket
    ports:
      - "3001:3001"
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
    depends_on:
      - redis
    restart: unless-stopped
    networks:
      - rpa-network

  # Extraction Worker
  worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      - NODE_ENV=production
      - DATABASE_URL=\${DATABASE_URL}
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - ENCRYPTION_MASTER_KEY=\${ENCRYPTION_MASTER_KEY}
    depends_on:
      - postgres
      - redis
    restart: unless-stopped
    deploy:
      replicas: 3
    networks:
      - rpa-network

  # PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    environment:
      - POSTGRES_USER=rpa_user
      - POSTGRES_PASSWORD=\${POSTGRES_PASSWORD}
      - POSTGRES_DB=rpa_platform
    volumes:
      - postgres-data:/var/lib/postgresql/data
    restart: unless-stopped
    networks:
      - rpa-network

  # Redis Cache & Queue
  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes
    volumes:
      - redis-data:/data
    restart: unless-stopped
    networks:
      - rpa-network

volumes:
  postgres-data:
  redis-data:

networks:
  rpa-network:
    driver: bridge`}
            </code>
          </div>
        </div>

        <div>
          <h3 className="text-lg font-semibold mb-3 text-gray-700">Build & Run</h3>
          <div className="code-block">
            <code className="text-sm">
              {`# Build images
docker-compose build

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f app

# Scale workers
docker-compose up -d --scale worker=5

# Stop all services
docker-compose down

# Clean up volumes
docker-compose down -v`}
            </code>
          </div>
        </div>
      </div>

      {/* Kubernetes Deployment - Hidden for production guide */}
      <div className="card-glass mb-8" style={{ display: 'none' }}>
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Kubernetes Deployment</h2>
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-3 text-gray-700">deployment.yaml</h3>
          <div className="code-block">
            <code className="text-sm">
              {`apiVersion: apps/v1
kind: Deployment
metadata:
  name: rpa-platform
  labels:
    app: rpa-platform
spec:
  replicas: 3
  selector:
    matchLabels:
      app: rpa-platform
  template:
    metadata:
      labels:
        app: rpa-platform
    spec:
      containers:
      - name: app
        image: gcr.io/project-id/rpa-platform:latest
        ports:
        - containerPort: 3000
        env:
        - name: NODE_ENV
          value: "production"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: rpa-secrets
              key: database-url
        - name: REDIS_HOST
          value: "redis-service"
        - name: ENCRYPTION_MASTER_KEY
          valueFrom:
            secretKeyRef:
              name: rpa-secrets
              key: encryption-key
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /api/health
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/ready
            port: 3000
          initialDelaySeconds: 5
          periodSeconds: 5

---
apiVersion: v1
kind: Service
metadata:
  name: rpa-platform-service
spec:
  selector:
    app: rpa-platform
  ports:
  - protocol: TCP
    port: 80
    targetPort: 3000
  type: LoadBalancer

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: rpa-platform-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: rpa-platform
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80`}
            </code>
          </div>
        </div>

        <div>
          <h3 className="text-lg font-semibold mb-3 text-gray-700">Deploy to Kubernetes</h3>
          <div className="code-block">
            <code className="text-sm">
              {`# Create namespace
kubectl create namespace rpa-platform

# Create secrets
kubectl create secret generic rpa-secrets \\
  --from-literal=database-url="postgresql://..." \\
  --from-literal=encryption-key="..." \\
  -n rpa-platform

# Apply configurations
kubectl apply -f k8s/deployment.yaml -n rpa-platform
kubectl apply -f k8s/service.yaml -n rpa-platform
kubectl apply -f k8s/ingress.yaml -n rpa-platform

# Check deployment status
kubectl get pods -n rpa-platform
kubectl get services -n rpa-platform

# View logs
kubectl logs -f deployment/rpa-platform -n rpa-platform

# Scale deployment
kubectl scale deployment/rpa-platform --replicas=5 -n rpa-platform

# Rolling update
kubectl set image deployment/rpa-platform \\
  app=gcr.io/project-id/rpa-platform:v2.0.0 \\
  -n rpa-platform

# Rollback if needed
kubectl rollout undo deployment/rpa-platform -n rpa-platform`}
            </code>
          </div>
        </div>
      </div>

      {/* Environment Configuration */}
      <EnvironmentConfig />

      <div style={{ display: 'none' }}>{/* Old Environment section hidden */}
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Environment Configuration OLD</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
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
      </div>

      {/* CI/CD Pipeline */}
      <CICDPipeline />

      <div style={{ display: 'none' }}>{/* Old CI/CD section hidden */}
        <h2 className="text-2xl font-bold mb-6 text-gray-800">CI/CD Pipeline OLD</h2>
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-3 text-gray-700">GitHub Actions Workflow</h3>
          <div className="code-block">
            <code className="text-sm">
              {`name: Deploy to Production

on:
  push:
    branches:
      - main

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: '18'
      - run: npm ci
      - run: npm run type-check
      - run: npm run lint
      - run: npm test

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: docker/setup-buildx-action@v2
      - uses: docker/login-action@v2
        with:
          registry: gcr.io
          username: _json_key
          password: \${{ secrets.GCP_SA_KEY }}
      - uses: docker/build-push-action@v4
        with:
          context: .
          push: true
          tags: gcr.io/\${{ secrets.GCP_PROJECT }}/rpa-platform:\${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max

  deploy:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: google-github-actions/setup-gcloud@v1
        with:
          service_account_key: \${{ secrets.GCP_SA_KEY }}
          project_id: \${{ secrets.GCP_PROJECT }}
      - run: |
          gcloud container clusters get-credentials production-cluster \\
            --zone us-central1-a
      - run: |
          kubectl set image deployment/rpa-platform \\
            app=gcr.io/\${{ secrets.GCP_PROJECT }}/rpa-platform:\${{ github.sha }} \\
            -n rpa-platform
      - run: kubectl rollout status deployment/rpa-platform -n rpa-platform`}
            </code>
          </div>
        </div>
      </div>

      {/* Monitoring & Observability */}
      <div className="card-glass">
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Monitoring & Observability</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="p-5 bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Application Monitoring</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Request/response times</li>
              <li>• Error rates and stack traces</li>
              <li>• API endpoint performance</li>
              <li>• WebSocket connection metrics</li>
              <li>• Job execution statistics</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Infrastructure Metrics</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• CPU and memory utilization</li>
              <li>• Disk I/O and storage usage</li>
              <li>• Network throughput</li>
              <li>• Database connection pools</li>
              <li>• Redis queue depths</li>
            </ul>
          </div>

          <div className="p-5 bg-gradient-to-br from-green-50 to-green-100 rounded-lg">
            <h3 className="text-lg font-bold mb-3 text-gray-800">Business Metrics</h3>
            <ul className="text-sm text-gray-600 space-y-2">
              <li>• Jobs scheduled/completed</li>
              <li>• Data extraction volumes</li>
              <li>• Pipeline success rates</li>
              <li>• SLA compliance tracking</li>
              <li>• Cost per transaction</li>
            </ul>
          </div>
        </div>

        <div className="mt-6 p-5 bg-yellow-50 rounded-lg border-l-4 border-yellow-500">
          <h3 className="text-lg font-bold mb-2 text-gray-800">Recommended Tools</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm text-gray-600">
            <div>• Datadog</div>
            <div>• New Relic</div>
            <div>• Sentry</div>
            <div>• Prometheus</div>
            <div>• Grafana</div>
            <div>• ELK Stack</div>
            <div>• CloudWatch</div>
            <div>• PagerDuty</div>
          </div>
        </div>
      </div>
    </>
  );
}
