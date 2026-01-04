// Core RPA Type Definitions for Banking Network Utility Operations

export interface RPAJob {
  id: string;
  name: string;
  schedule: CronSchedule;
  source: DataSource;
  credentials: CredentialReference;
  extractionMethod: 'web-automation' | 'api' | 'file-download';
  status: 'idle' | 'running' | 'failed' | 'completed';
  retryConfig: RetryConfiguration;
  createdAt: Date;
  updatedAt: Date;
  lastRunAt?: Date;
  description?: string;
  tags?: string[];
}

export interface DataSource {
  type: 'banking' | 'erp' | 'custom';
  url?: string;
  apiEndpoint?: string;
  selectors?: WebSelectors;
  accountIdentifiers: string[];
  bankingNetwork?: BankingNetworkType;
}

export interface BankingNetworkType {
  name: string;
  type: 'clearinghouse' | 'payment-processor' | 'shared-infrastructure';
  protocol: 'REST' | 'SOAP' | 'FIX' | 'ISO20022';
  requiresAuth: boolean;
}

export interface ExtractedData {
  jobId: string;
  timestamp: Date;
  rawData: any;
  metadata: ExtractionMetadata;
  validationStatus: ValidationResult;
  transactionCount?: number;
  dataSize: number;
}

export interface ExtractionMetadata {
  source: string;
  extractionDuration: number;
  recordCount: number;
  fileFormat?: string;
  compressionUsed: boolean;
  checksumHash: string;
}

export interface ValidationResult {
  isValid: boolean;
  errors: ValidationError[];
  warnings: ValidationWarning[];
  validatedAt: Date;
  validationRules: string[];
}

export interface ValidationError {
  field: string;
  message: string;
  severity: 'critical' | 'error' | 'warning';
  code: string;
}

export interface ValidationWarning {
  field: string;
  message: string;
  suggestion?: string;
}

export interface CronSchedule {
  expression: string;
  timezone: string;
  preferredHours: number[]; // off-peak hours
  enabled: boolean;
  nextRun?: Date;
}

export interface CredentialReference {
  vaultId: string;
  type: 'oauth' | 'basic' | 'certificate' | 'api-key';
  encryptedData: string;
  expiresAt?: Date;
  rotationPolicy?: string;
}

export interface RetryConfiguration {
  maxAttempts: number;
  backoffStrategy: 'linear' | 'exponential' | 'constant';
  initialDelay: number;
  maxDelay: number;
  retryableErrors: string[];
}

export interface WebSelectors {
  loginForm?: string;
  usernameField?: string;
  passwordField?: string;
  submitButton?: string;
  dataTable?: string;
  downloadButton?: string;
  navigationElements?: Record<string, string>;
}

export interface BankingTransaction {
  transactionId: string;
  accountNumber: string;
  amount: number;
  currency: string;
  transactionType: 'debit' | 'credit' | 'transfer';
  timestamp: Date;
  status: 'pending' | 'completed' | 'failed' | 'reversed';
  sourceBank?: string;
  destinationBank?: string;
  clearinghouseReference?: string;
}

export interface ETLJob {
  id: string;
  extractionJobId: string;
  stage: 'extract' | 'transform' | 'load' | 'validate';
  status: 'pending' | 'processing' | 'completed' | 'failed';
  startedAt?: Date;
  completedAt?: Date;
  recordsProcessed: number;
  errorCount: number;
}

export interface TransformationRule {
  id: string;
  name: string;
  inputFields: string[];
  outputFields: string[];
  transformationType: 'mapping' | 'calculation' | 'aggregation' | 'enrichment';
  expression: string;
  validationRules?: string[];
}

export interface AuditLogEntry {
  id: string;
  timestamp: Date;
  userId: string;
  action: string;
  resource: string;
  resourceId: string;
  changes?: Record<string, any>;
  ipAddress: string;
  userAgent: string;
  result: 'success' | 'failure';
  errorMessage?: string;
}

export interface SecurityConfig {
  encryptionAlgorithm: string;
  keyDerivation: string;
  sessionTimeout: number;
  mfaRequired: boolean;
  auditEnabled: boolean;
  complianceMode: 'PCI-DSS' | 'GDPR' | 'SOC2' | 'ISO27001';
}

export interface QueueJob<T = any> {
  id: string;
  name: string;
  data: T;
  priority: number;
  attempts: number;
  maxAttempts: number;
  createdAt: Date;
  processedAt?: Date;
  failedReason?: string;
}

export interface WorkerConfig {
  concurrency: number;
  maxJobsPerWorker: number;
  idleTimeout: number;
  stalledInterval: number;
  lockDuration: number;
}
