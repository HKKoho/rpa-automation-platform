'use client';

import { useState } from 'react';

export default function CICDPipeline() {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div className="card-glass mb-8">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between text-left hover:opacity-80 transition-opacity"
      >
        <h2 className="text-2xl font-bold text-gray-800">CI/CD Pipeline</h2>
        <span className="text-2xl text-gray-600">
          {isExpanded ? '▼' : '▶'}
        </span>
      </button>

      {isExpanded && (
        <div className="mb-6 mt-6">
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
      )}
    </div>
  );
}
