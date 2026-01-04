// Vercel-compatible stub version
// For full implementation with TensorFlow.js, see DynamicElementDetector.local.ts

/**
 * AI Element Detector Stub for Vercel Deployment
 *
 * This is a placeholder that allows the documentation site to build on Vercel.
 * For local development with full TensorFlow.js functionality, use package.local.json
 * which includes @tensorflow/tfjs-node dependency.
 *
 * Production deployment requires:
 * - @tensorflow/tfjs (browser version) for client-side AI
 * - External AI service (Google Cloud Vision, AWS Rekognition)
 * - Or heuristic-based fallback
 */
export class AIElementDetector {
  private model: any = null;
  private initialized: boolean = false;

  async initialize(): Promise<void> {
    console.warn('AIElementDetector stub loaded - TensorFlow.js not available on Vercel');
    this.initialized = false;
  }

  /**
   * Detect login form elements using heuristics (AI not available)
   */
  async detectLoginForm(page: any): Promise<LoginElements> {
    // Fallback to simple heuristic detection
    return {
      username: 'input[type="text"], input[type="email"], input[name*="user"], input[name*="email"]',
      password: 'input[type="password"], input[name*="pass"]',
      submitButton: 'button[type="submit"], input[type="submit"], button:contains("Login"), button:contains("Sign in")',
    };
  }

  /**
   * Detect data tables using heuristics (AI not available)
   */
  async detectDataTables(page: any): Promise<any[]> {
    // Fallback to simple table detection
    return [];
  }

  async dispose(): Promise<void> {
    // No-op
  }
}

export interface LoginElements {
  username: string;
  password: string;
  submitButton: string;
}
