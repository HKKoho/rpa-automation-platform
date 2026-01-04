// Vercel-compatible stub version
// For full implementation with Puppeteer, see WebAutomation.local.ts

import { AIElementDetector } from '@/services/ai/DynamicElementDetector';
import type { ExtractedData, WebSelectors } from '@/types/rpa.types';

/**
 * Web Automation Stub for Vercel Deployment
 *
 * This is a placeholder that allows the documentation site to build on Vercel.
 * For local development with full Puppeteer functionality, use package.local.json
 * which includes puppeteer dependency.
 *
 * Production deployment requires:
 * - Browserless.io or similar headless browser service
 * - @sparticuz/chromium for AWS Lambda (limited)
 * - Or external scraping service
 */
export class WebAutomation {
  private browser: any = null;
  private aiDetector: AIElementDetector;
  private options: AutomationOptions;

  constructor(options: AutomationOptions = {}) {
    this.options = {
      headless: options.headless !== false,
      timeout: options.timeout || 30000,
      userAgent: options.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      viewport: options.viewport || { width: 1920, height: 1080 },
      ...options,
    };
    this.aiDetector = new AIElementDetector();
    console.warn('WebAutomation stub loaded - browser automation not available on Vercel');
  }

  /**
   * Extract data from banking web interface
   */
  async extract(params: ExtractionParams): Promise<ExtractedData> {
    throw new Error(
      'Web automation not implemented on Vercel. Use Browserless.io, @sparticuz/chromium, or external scraping service.'
    );
  }

  /**
   * Authenticate to banking interface
   */
  private async authenticate(page: any, params: ExtractionParams): Promise<void> {
    // Stub
  }

  /**
   * Extract data using AI-assisted element detection
   */
  private async extractWithAI(page: any, selectors?: WebSelectors): Promise<any[]> {
    return [];
  }

  /**
   * Handle multi-factor authentication
   */
  private async handleMFA(page: any, mfaToken: string): Promise<void> {
    // Stub
  }

  /**
   * Filter requests for security and performance
   */
  private isAllowedRequest(request: any): boolean {
    return true;
  }

  /**
   * Wait for page to stabilize (no network activity)
   */
  private async waitForPageStable(page: any, timeout: number = 5000): Promise<void> {
    // Stub
  }
}

export interface AutomationOptions {
  headless?: boolean;
  timeout?: number;
  userAgent?: string;
  viewport?: { width: number; height: number };
}

export interface ExtractionParams {
  jobId: string;
  url: string;
  credentials?: {
    username: string;
    password: string;
    mfaToken?: string;
  };
  selectors?: WebSelectors;
  options?: any;
}
