/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  swcMinify: true,

  // Environment variables
  env: {
    APP_NAME: 'RPA Automation Platform',
    APP_VERSION: '1.0.0',
  },

  // Server configuration
  serverRuntimeConfig: {
    // Server-side only
  },

  publicRuntimeConfig: {
    // Available on both server and client
  },

  // Headers for security
  async headers() {
    return [
      {
        source: '/(.*)',
        headers: [
          {
            key: 'X-Frame-Options',
            value: 'DENY',
          },
          {
            key: 'X-Content-Type-Options',
            value: 'nosniff',
          },
          {
            key: 'X-XSS-Protection',
            value: '1; mode=block',
          },
          {
            key: 'Referrer-Policy',
            value: 'strict-origin-when-cross-origin',
          },
        ],
      },
    ];
  },

  // Webpack configuration
  webpack: (config, { isServer }) => {
    // Fixes for Puppeteer and TensorFlow
    if (!isServer) {
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        net: false,
        tls: false,
        crypto: false,
        stream: false,
        path: false,
        os: false,
        'mock-aws-s3': false,
        'aws-sdk': false,
        'nock': false,
      };
    }

    // Ignore problematic dependencies
    config.externals = config.externals || [];
    config.externals.push({
      '@mapbox/node-pre-gyp': 'commonjs @mapbox/node-pre-gyp',
      'mock-aws-s3': 'mock-aws-s3',
      'aws-sdk': 'aws-sdk',
      'nock': 'nock',
    });

    // Handle HTML files in node_modules
    config.module.rules.push({
      test: /\.html$/,
      type: 'asset/resource',
    });

    return config;
  },

  // Image optimization
  images: {
    domains: [],
    formats: ['image/avif', 'image/webp'],
  },

  // Experimental features
  experimental: {
    serverActions: {
      bodySizeLimit: '2mb',
    },
    serverComponentsExternalPackages: [
      'puppeteer',
      'puppeteer-core',
      '@tensorflow/tfjs-node',
      'bullmq',
      'ioredis',
      'bcrypt',
      '@mapbox/node-pre-gyp',
    ],
  },

  // Output configuration for better serverless compatibility
  output: 'standalone',
};

module.exports = nextConfig;
