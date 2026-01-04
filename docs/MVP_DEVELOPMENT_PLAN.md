# RPA Automation Platform - MVP Development Plan

**Version:** 1.0
**Target Timeline:** 4-6 weeks
**Goal:** Build a working proof-of-concept with basic data extraction and storage

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current State](#current-state)
3. [MVP Goals](#mvp-goals)
4. [Technical Architecture](#technical-architecture)
5. [Phase-by-Phase Implementation](#phase-by-phase-implementation)
6. [Detailed Technical Specifications](#detailed-technical-specifications)
7. [What to Skip for MVP](#what-to-skip-for-mvp)
8. [Success Criteria](#success-criteria)
9. [Post-MVP Roadmap](#post-mvp-roadmap)

---

## Executive Summary

### Current State
The platform currently consists of:
- ✅ Next.js 14 frontend with comprehensive documentation
- ✅ TypeScript type definitions
- ✅ UI components and pages
- ❌ **No backend implementation**
- ❌ **No database connections**
- ❌ **No actual data extraction**
- ❌ **No BigQuery integration**

### MVP Objective
Build a **minimal viable product** that can:
1. Extract transaction data from at least ONE banking network/demo site
2. Store data securely in PostgreSQL
3. Encrypt sensitive credentials
4. Optionally sync data to BigQuery for analytics
5. Display real data on the frontend (not mock data)

### Expected Outcome
A working demo that proves the platform can:
- Automate login to a banking portal
- Extract real transaction data
- Store and encrypt credentials
- Process and store transaction data
- Display results in the UI

---

## Current State

### What Exists
```
rpa-automation-platform/
├── src/app/                    # ✅ Frontend pages (Next.js 14)
├── src/components/             # ✅ UI components
├── src/types/                  # ✅ TypeScript types
├── src/core/                   # ⚠️ Type definitions only, NO implementation
│   ├── engine/                 # ❌ RPAEngine not implemented
│   ├── extraction/             # ❌ WebAutomation not implemented
│   ├── security/               # ❌ CredentialVault not implemented
│   └── pipeline/               # ❌ ETLPipeline not implemented
├── server/                     # ⚠️ Directories exist, but empty
│   ├── workers/                # ❌ No workers
│   ├── websocket/              # ❌ No WebSocket server
│   └── database/               # ❌ No database scripts
└── README.md                   # ✅ Comprehensive documentation
```

### Gap Analysis
| Feature | Documentation | Implementation | Gap |
|---------|--------------|----------------|-----|
| Frontend UI | ✅ Complete | ✅ Complete | None |
| API Routes | ✅ Documented | ❌ Not built | **HIGH** |
| Database | ✅ Documented | ❌ Not setup | **HIGH** |
| Credential Vault | ✅ Documented | ❌ Not built | **HIGH** |
| Web Scraper | ✅ Documented | ❌ Not built | **CRITICAL** |
| Job Queue | ✅ Documented | ❌ Not built | Medium |
| BigQuery | ✅ Documented | ❌ Not setup | Medium |
| AI Features | ✅ Documented | ❌ Not built | Low (skip for MVP) |

---

## MVP Goals

### Primary Goals (Must Have)
1. ✅ **Working Data Extraction**
   - Login to at least 1 banking portal or demo site
   - Extract transaction data using Puppeteer
   - Handle basic authentication

2. ✅ **Data Storage**
   - PostgreSQL database with proper schema
   - Store jobs, credentials, and transactions
   - Basic CRUD operations via API

3. ✅ **Credential Security**
   - AES-256-GCM encryption for credentials
   - Secure storage and retrieval
   - Environment-based master key

4. ✅ **Frontend Integration**
   - Display real data from database (not mock data)
   - Create jobs via UI
   - View job status and results

### Secondary Goals (Nice to Have)
5. ⭐ **BigQuery Integration**
   - Stream transactions to BigQuery
   - Basic analytics queries
   - Data warehouse setup

6. ⭐ **Job Scheduling**
   - Simple cron-based scheduling
   - Manual job execution
   - Status tracking

### Out of Scope (Post-MVP)
❌ Queue system (BullMQ/Redis)
❌ Worker processes
❌ WebSocket real-time updates
❌ AI features (TensorFlow.js, LLMs)
❌ Multiple banking network support
❌ Advanced ETL pipeline
❌ Power BI integration
❌ Audit logging system

---

## Technical Architecture

### MVP Architecture (Simplified)

```
┌─────────────────────────────────────────────────────────────┐
│                     FRONTEND (Next.js 14)                    │
│  - Job creation UI                                           │
│  - Job status display                                        │
│  - Transaction list                                          │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│              NEXT.JS API ROUTES (Backend)                    │
│  - POST /api/jobs          (create job)                      │
│  - GET  /api/jobs          (list jobs)                       │
│  - GET  /api/jobs/[id]     (get job details)                 │
│  - POST /api/credentials   (store credential)                │
│  - GET  /api/transactions  (list transactions)               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                  CORE SERVICES (New)                         │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │  Scraper Service │  │ Encryption Utils │                 │
│  │  (Puppeteer)     │  │ (AES-256-GCM)    │                 │
│  └──────────────────┘  └──────────────────┘                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────┐
│                    DATA LAYER                                │
│  ┌──────────────────┐          ┌─────────────────┐          │
│  │   PostgreSQL     │          │    BigQuery     │          │
│  │  (Primary DB)    │   sync   │   (Analytics)   │          │
│  │  - jobs          │  ──────> │ - transactions  │          │
│  │  - credentials   │          │ - jobs_history  │          │
│  │  - transactions  │          └─────────────────┘          │
│  └──────────────────┘                                        │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. User creates job via UI
   ↓
2. POST /api/jobs creates job record in PostgreSQL (status: 'pending')
   ↓
3. Scraper service launches Puppeteer
   ↓
4. Login to banking portal (credentials decrypted from vault)
   ↓
5. Extract transaction data from HTML
   ↓
6. Store transactions in PostgreSQL
   ↓
7. Update job status to 'completed'
   ↓
8. (Optional) Sync transactions to BigQuery
   ↓
9. Frontend polls /api/jobs/[id] for status
   ↓
10. Display results to user
```

---

## Phase-by-Phase Implementation

### **Phase 0: Project Setup** (Week 1, Days 1-2)
**Duration:** 2 days
**Goal:** Set up development environment and dependencies

#### Tasks
- [ ] Install PostgreSQL locally or provision cloud instance (Railway, Supabase, Neon)
- [ ] Install Prisma ORM and initialize schema
- [ ] Set up environment variables
- [ ] Create GCP project for BigQuery (optional)
- [ ] Install Puppeteer and dependencies

#### Dependencies to Install
```bash
# Database
npm install @prisma/client
npm install -D prisma

# Web scraping
npm install puppeteer

# BigQuery (optional)
npm install @google-cloud/bigquery

# Utilities
npm install date-fns zod
```

#### Environment Variables
```bash
# .env.local
DATABASE_URL="postgresql://user:password@localhost:5432/rpa_db"
ENCRYPTION_MASTER_KEY="<32-byte-hex-string>"
NODE_ENV="development"

# Optional - BigQuery
GCP_PROJECT_ID="your-project-id"
GCP_KEY_FILE="./credentials/gcp-service-account.json"
BIGQUERY_DATASET="rpa_warehouse"
```

#### Deliverables
✅ PostgreSQL running locally
✅ Prisma schema initialized
✅ Environment variables configured
✅ Dependencies installed

---

### **Phase 1: Database Layer** (Week 1, Days 3-5)
**Duration:** 3 days
**Goal:** Set up PostgreSQL with Prisma ORM

#### Database Schema

```prisma
// prisma/schema.prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

// Job model - tracks extraction jobs
model Job {
  id          String        @id @default(uuid())
  name        String
  status      JobStatus     @default(PENDING)
  config      Json          // Job configuration (URL, selectors, etc.)
  error       String?       // Error message if failed

  startedAt   DateTime?
  completedAt DateTime?
  createdAt   DateTime      @default(now())
  updatedAt   DateTime      @updatedAt

  transactions Transaction[]

  @@index([status])
  @@index([createdAt])
}

enum JobStatus {
  PENDING
  RUNNING
  COMPLETED
  FAILED
}

// Credential model - encrypted credentials
model Credential {
  id          String   @id @default(uuid())
  name        String   @unique
  encrypted   String   // Encrypted JSON blob
  metadata    Json?    // Non-sensitive metadata

  expiresAt   DateTime?
  createdAt   DateTime @default(now())
  updatedAt   DateTime @updatedAt

  @@index([name])
}

// Transaction model - extracted banking data
model Transaction {
  id          String   @id @default(uuid())
  jobId       String
  job         Job      @relation(fields: [jobId], references: [id], onDelete: Cascade)

  date        DateTime
  description String
  amount      Float
  currency    String   @default("USD")
  category    String?
  rawData     Json?    // Original extracted data

  createdAt   DateTime @default(now())

  @@index([jobId])
  @@index([date])
}
```

#### Setup Commands
```bash
# Initialize Prisma
npx prisma init

# Create migration
npx prisma migrate dev --name init

# Generate Prisma client
npx prisma generate

# Open Prisma Studio (DB GUI)
npx prisma studio
```

#### Create Prisma Client Singleton
```typescript
// src/lib/prisma.ts
import { PrismaClient } from '@prisma/client';

const globalForPrisma = global as unknown as { prisma: PrismaClient };

export const prisma =
  globalForPrisma.prisma ||
  new PrismaClient({
    log: process.env.NODE_ENV === 'development' ? ['query', 'error', 'warn'] : ['error'],
  });

if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma;
```

#### Deliverables
✅ Database schema created
✅ Prisma migrations applied
✅ Prisma client generated
✅ Test data seeded (optional)

---

### **Phase 2: Credential Encryption** (Week 2, Days 1-2)
**Duration:** 2 days
**Goal:** Implement secure credential storage

#### Encryption Utilities

```typescript
// src/lib/encryption.ts
import crypto from 'crypto';

const ALGORITHM = 'aes-256-gcm';
const KEY_LENGTH = 32; // 256 bits
const IV_LENGTH = 16;  // 128 bits
const AUTH_TAG_LENGTH = 16;

// Get encryption key from environment
function getEncryptionKey(): Buffer {
  const key = process.env.ENCRYPTION_MASTER_KEY;

  if (!key) {
    throw new Error('ENCRYPTION_MASTER_KEY is not set in environment variables');
  }

  // Ensure key is 32 bytes (64 hex characters)
  if (key.length !== 64) {
    throw new Error('ENCRYPTION_MASTER_KEY must be 64 hex characters (32 bytes)');
  }

  return Buffer.from(key, 'hex');
}

export interface EncryptedData {
  iv: string;
  encrypted: string;
  authTag: string;
}

/**
 * Encrypt data using AES-256-GCM
 */
export function encrypt(data: any): string {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);

  // Encrypt the data
  const plaintext = JSON.stringify(data);
  let encrypted = cipher.update(plaintext, 'utf8', 'hex');
  encrypted += cipher.final('hex');

  // Get authentication tag
  const authTag = cipher.getAuthTag();

  // Return encrypted data with IV and auth tag
  const result: EncryptedData = {
    iv: iv.toString('hex'),
    encrypted,
    authTag: authTag.toString('hex'),
  };

  return JSON.stringify(result);
}

/**
 * Decrypt data using AES-256-GCM
 */
export function decrypt(encryptedData: string): any {
  const key = getEncryptionKey();
  const { iv, encrypted, authTag }: EncryptedData = JSON.parse(encryptedData);

  const decipher = crypto.createDecipheriv(
    ALGORITHM,
    key,
    Buffer.from(iv, 'hex')
  );

  decipher.setAuthTag(Buffer.from(authTag, 'hex'));

  // Decrypt the data
  let decrypted = decipher.update(encrypted, 'hex', 'utf8');
  decrypted += decipher.final('utf8');

  return JSON.parse(decrypted);
}

/**
 * Generate a random encryption key (for initial setup)
 */
export function generateEncryptionKey(): string {
  return crypto.randomBytes(KEY_LENGTH).toString('hex');
}
```

#### Credential Service

```typescript
// src/lib/services/credentialService.ts
import { prisma } from '@/lib/prisma';
import { encrypt, decrypt } from '@/lib/encryption';

export interface CredentialData {
  username?: string;
  password?: string;
  apiKey?: string;
  apiSecret?: string;
  [key: string]: any;
}

export class CredentialService {
  /**
   * Store encrypted credential
   */
  async store(
    name: string,
    data: CredentialData,
    options?: {
      expiresAt?: Date;
      metadata?: Record<string, any>;
    }
  ) {
    const encrypted = encrypt(data);

    const credential = await prisma.credential.upsert({
      where: { name },
      update: {
        encrypted,
        metadata: options?.metadata || null,
        expiresAt: options?.expiresAt || null,
      },
      create: {
        name,
        encrypted,
        metadata: options?.metadata || null,
        expiresAt: options?.expiresAt || null,
      },
    });

    return credential;
  }

  /**
   * Retrieve and decrypt credential
   */
  async retrieve(name: string): Promise<CredentialData> {
    const credential = await prisma.credential.findUnique({
      where: { name },
    });

    if (!credential) {
      throw new Error(`Credential not found: ${name}`);
    }

    // Check expiration
    if (credential.expiresAt && credential.expiresAt < new Date()) {
      throw new Error(`Credential expired: ${name}`);
    }

    return decrypt(credential.encrypted);
  }

  /**
   * Delete credential
   */
  async delete(name: string) {
    await prisma.credential.delete({
      where: { name },
    });
  }

  /**
   * List all credentials (metadata only, no decryption)
   */
  async list() {
    return prisma.credential.findMany({
      select: {
        id: true,
        name: true,
        metadata: true,
        expiresAt: true,
        createdAt: true,
        updatedAt: true,
      },
    });
  }
}

export const credentialService = new CredentialService();
```

#### Generate Master Key Script

```typescript
// scripts/generate-encryption-key.ts
import { generateEncryptionKey } from '../src/lib/encryption';

console.log('Generated Encryption Master Key:');
console.log(generateEncryptionKey());
console.log('\nAdd this to your .env.local file:');
console.log(`ENCRYPTION_MASTER_KEY="${generateEncryptionKey()}"`);
```

Run with:
```bash
npx tsx scripts/generate-encryption-key.ts
```

#### Deliverables
✅ Encryption utilities implemented
✅ Credential service created
✅ Master key generated
✅ Unit tests for encryption (optional)

---

### **Phase 3: Basic API Routes** (Week 2, Days 3-5)
**Duration:** 3 days
**Goal:** Implement CRUD operations for jobs, credentials, and transactions

#### Jobs API

```typescript
// src/app/api/jobs/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { z } from 'zod';

const createJobSchema = z.object({
  name: z.string().min(1),
  config: z.object({
    url: z.string().url(),
    selectors: z.object({
      username: z.string(),
      password: z.string(),
      loginButton: z.string(),
      transactionTable: z.string().optional(),
    }),
    credentialName: z.string(),
  }),
});

// GET /api/jobs - List all jobs
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '20');
    const status = searchParams.get('status');

    const jobs = await prisma.job.findMany({
      where: status ? { status: status as any } : undefined,
      orderBy: { createdAt: 'desc' },
      take: limit,
      include: {
        _count: {
          select: { transactions: true },
        },
      },
    });

    return NextResponse.json({
      success: true,
      data: jobs,
      timestamp: new Date(),
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'FETCH_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 500 }
    );
  }
}

// POST /api/jobs - Create new job
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const validated = createJobSchema.parse(body);

    const job = await prisma.job.create({
      data: {
        name: validated.name,
        status: 'PENDING',
        config: validated.config,
      },
    });

    return NextResponse.json({
      success: true,
      data: job,
      timestamp: new Date(),
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'CREATE_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 400 }
    );
  }
}
```

```typescript
// src/app/api/jobs/[id]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

// GET /api/jobs/[id] - Get job details
export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const job = await prisma.job.findUnique({
      where: { id: params.id },
      include: {
        transactions: {
          orderBy: { date: 'desc' },
          take: 100,
        },
      },
    });

    if (!job) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: 'Job not found',
          },
          timestamp: new Date(),
        },
        { status: 404 }
      );
    }

    return NextResponse.json({
      success: true,
      data: job,
      timestamp: new Date(),
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'FETCH_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 500 }
    );
  }
}
```

#### Credentials API

```typescript
// src/app/api/credentials/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { credentialService } from '@/lib/services/credentialService';
import { z } from 'zod';

const createCredentialSchema = z.object({
  name: z.string().min(1),
  data: z.object({
    username: z.string().optional(),
    password: z.string().optional(),
    apiKey: z.string().optional(),
  }),
  metadata: z.record(z.any()).optional(),
  expiresAt: z.string().datetime().optional(),
});

// GET /api/credentials - List credentials (metadata only)
export async function GET() {
  try {
    const credentials = await credentialService.list();

    return NextResponse.json({
      success: true,
      data: credentials,
      timestamp: new Date(),
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'FETCH_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 500 }
    );
  }
}

// POST /api/credentials - Store credential
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const validated = createCredentialSchema.parse(body);

    const credential = await credentialService.store(
      validated.name,
      validated.data,
      {
        metadata: validated.metadata,
        expiresAt: validated.expiresAt ? new Date(validated.expiresAt) : undefined,
      }
    );

    return NextResponse.json({
      success: true,
      data: {
        id: credential.id,
        name: credential.name,
        metadata: credential.metadata,
      },
      timestamp: new Date(),
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'CREATE_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 400 }
    );
  }
}
```

#### Transactions API

```typescript
// src/app/api/transactions/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

// GET /api/transactions - List transactions
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const jobId = searchParams.get('jobId');
    const limit = parseInt(searchParams.get('limit') || '100');

    const transactions = await prisma.transaction.findMany({
      where: jobId ? { jobId } : undefined,
      orderBy: { date: 'desc' },
      take: limit,
      include: {
        job: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });

    return NextResponse.json({
      success: true,
      data: transactions,
      timestamp: new Date(),
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'FETCH_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 500 }
    );
  }
}
```

#### Deliverables
✅ Jobs API (GET, POST, GET by ID)
✅ Credentials API (GET, POST)
✅ Transactions API (GET with filters)
✅ Request validation with Zod
✅ Consistent error handling

---

### **Phase 4: Web Scraper Service** (Week 3-4)
**Duration:** 7-10 days
**Goal:** Implement Puppeteer-based web scraping

#### Basic Scraper Service

```typescript
// src/lib/services/scraperService.ts
import puppeteer, { Browser, Page } from 'puppeteer';
import { credentialService } from './credentialService';

export interface ScraperConfig {
  url: string;
  selectors: {
    username: string;
    password: string;
    loginButton: string;
    transactionTable?: string;
    transactionRows?: string;
  };
  credentialName: string;
}

export interface ScrapedTransaction {
  date: string;
  description: string;
  amount: string;
  category?: string;
}

export class ScraperService {
  private browser: Browser | null = null;

  /**
   * Initialize browser
   */
  private async initBrowser() {
    if (!this.browser) {
      this.browser = await puppeteer.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
      });
    }
    return this.browser;
  }

  /**
   * Close browser
   */
  async closeBrowser() {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
  }

  /**
   * Scrape banking portal
   */
  async scrape(config: ScraperConfig): Promise<ScrapedTransaction[]> {
    const browser = await this.initBrowser();
    const page = await browser.newPage();

    try {
      // Set viewport
      await page.setViewport({ width: 1920, height: 1080 });

      // Retrieve credentials
      const credentials = await credentialService.retrieve(config.credentialName);

      if (!credentials.username || !credentials.password) {
        throw new Error('Username and password required in credential');
      }

      // Navigate to login page
      console.log(`Navigating to ${config.url}`);
      await page.goto(config.url, { waitUntil: 'networkidle2' });

      // Take screenshot for debugging
      await page.screenshot({ path: 'debug-login-page.png' });

      // Fill login form
      console.log('Filling login form...');
      await page.waitForSelector(config.selectors.username);
      await page.type(config.selectors.username, credentials.username);
      await page.type(config.selectors.password, credentials.password);

      // Click login button
      console.log('Clicking login button...');
      await page.click(config.selectors.loginButton);

      // Wait for navigation
      await page.waitForNavigation({ waitUntil: 'networkidle2' });

      // Take screenshot after login
      await page.screenshot({ path: 'debug-after-login.png' });

      // Wait for transaction table (if specified)
      if (config.selectors.transactionTable) {
        console.log('Waiting for transaction table...');
        await page.waitForSelector(config.selectors.transactionTable, {
          timeout: 10000,
        });
      }

      // Extract transactions
      console.log('Extracting transactions...');
      const transactions = await this.extractTransactions(page, config);

      console.log(`Extracted ${transactions.length} transactions`);
      return transactions;

    } catch (error) {
      // Take error screenshot
      await page.screenshot({ path: 'debug-error.png' });
      throw error;
    } finally {
      await page.close();
    }
  }

  /**
   * Extract transactions from page
   */
  private async extractTransactions(
    page: Page,
    config: ScraperConfig
  ): Promise<ScrapedTransaction[]> {
    const tableSelector = config.selectors.transactionTable;
    const rowSelector = config.selectors.transactionRows || `${tableSelector} tbody tr`;

    if (!tableSelector) {
      throw new Error('Transaction table selector required');
    }

    const transactions = await page.evaluate((selector) => {
      const rows = document.querySelectorAll(selector);
      const results: any[] = [];

      rows.forEach((row) => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 3) {
          results.push({
            date: cells[0]?.textContent?.trim() || '',
            description: cells[1]?.textContent?.trim() || '',
            amount: cells[2]?.textContent?.trim() || '',
            category: cells[3]?.textContent?.trim() || '',
          });
        }
      });

      return results;
    }, rowSelector);

    return transactions;
  }
}

export const scraperService = new ScraperService();
```

#### Job Execution API

```typescript
// src/app/api/jobs/[id]/execute/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';
import { scraperService } from '@/lib/services/scraperService';
import { parse } from 'date-fns';

// POST /api/jobs/[id]/execute - Execute job
export async function POST(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    // Get job
    const job = await prisma.job.findUnique({
      where: { id: params.id },
    });

    if (!job) {
      return NextResponse.json(
        { success: false, error: { code: 'NOT_FOUND', message: 'Job not found' } },
        { status: 404 }
      );
    }

    // Update status to running
    await prisma.job.update({
      where: { id: params.id },
      data: {
        status: 'RUNNING',
        startedAt: new Date(),
      },
    });

    try {
      // Execute scraper
      const scrapedData = await scraperService.scrape(job.config as any);

      // Parse and store transactions
      const transactions = scrapedData.map((tx) => ({
        jobId: job.id,
        date: parseDate(tx.date),
        description: tx.description,
        amount: parseAmount(tx.amount),
        currency: 'USD',
        category: tx.category || null,
        rawData: tx,
      }));

      // Bulk insert transactions
      await prisma.transaction.createMany({
        data: transactions,
      });

      // Update job status to completed
      await prisma.job.update({
        where: { id: params.id },
        data: {
          status: 'COMPLETED',
          completedAt: new Date(),
        },
      });

      return NextResponse.json({
        success: true,
        data: {
          jobId: job.id,
          transactionsCount: transactions.length,
        },
        timestamp: new Date(),
      });

    } catch (error: any) {
      // Update job status to failed
      await prisma.job.update({
        where: { id: params.id },
        data: {
          status: 'FAILED',
          error: error.message,
          completedAt: new Date(),
        },
      });

      throw error;
    }

  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: {
          code: 'EXECUTION_FAILED',
          message: error.message,
        },
        timestamp: new Date(),
      },
      { status: 500 }
    );
  } finally {
    await scraperService.closeBrowser();
  }
}

// Helper functions
function parseDate(dateStr: string): Date {
  // Try multiple date formats
  const formats = [
    'MM/dd/yyyy',
    'yyyy-MM-dd',
    'dd/MM/yyyy',
    'MMM dd, yyyy',
  ];

  for (const format of formats) {
    try {
      return parse(dateStr, format, new Date());
    } catch {
      continue;
    }
  }

  // Fallback to current date
  return new Date();
}

function parseAmount(amountStr: string): number {
  // Remove currency symbols and commas
  const cleaned = amountStr.replace(/[$,€£]/g, '').trim();
  return parseFloat(cleaned) || 0;
}
```

#### Demo Banking Portal (for Testing)

```html
<!-- public/demo-bank/index.html -->
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Demo Bank - Login</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 400px; margin: 50px auto; }
    input { width: 100%; padding: 10px; margin: 10px 0; }
    button { width: 100%; padding: 10px; background: #007bff; color: white; border: none; cursor: pointer; }
  </style>
</head>
<body>
  <h1>Demo Bank Login</h1>
  <form id="loginForm">
    <input type="text" id="username" placeholder="Username" required>
    <input type="password" id="password" placeholder="Password" required>
    <button type="submit" id="loginButton">Login</button>
  </form>

  <script>
    document.getElementById('loginForm').addEventListener('submit', (e) => {
      e.preventDefault();
      const username = document.getElementById('username').value;
      const password = document.getElementById('password').value;

      // Simple validation (username: demo, password: demo123)
      if (username === 'demo' && password === 'demo123') {
        window.location.href = 'transactions.html';
      } else {
        alert('Invalid credentials. Try username: demo, password: demo123');
      }
    });
  </script>
</body>
</html>
```

```html
<!-- public/demo-bank/transactions.html -->
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Demo Bank - Transactions</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
    th { background: #007bff; color: white; }
  </style>
</head>
<body>
  <h1>Your Transactions</h1>
  <table id="transactionTable">
    <thead>
      <tr>
        <th>Date</th>
        <th>Description</th>
        <th>Amount</th>
        <th>Category</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>01/15/2025</td>
        <td>Amazon.com Purchase</td>
        <td>$125.50</td>
        <td>Shopping</td>
      </tr>
      <tr>
        <td>01/14/2025</td>
        <td>Starbucks Coffee</td>
        <td>$8.75</td>
        <td>Food</td>
      </tr>
      <tr>
        <td>01/13/2025</td>
        <td>Salary Deposit</td>
        <td>$5,000.00</td>
        <td>Income</td>
      </tr>
      <tr>
        <td>01/12/2025</td>
        <td>Electric Bill</td>
        <td>$150.00</td>
        <td>Utilities</td>
      </tr>
      <tr>
        <td>01/11/2025</td>
        <td>Uber Ride</td>
        <td>$25.30</td>
        <td>Transportation</td>
      </tr>
    </tbody>
  </table>
</body>
</html>
```

#### Test Job Configuration

```json
{
  "name": "Demo Bank Daily Extract",
  "config": {
    "url": "http://localhost:3000/demo-bank/index.html",
    "selectors": {
      "username": "#username",
      "password": "#password",
      "loginButton": "#loginButton",
      "transactionTable": "#transactionTable",
      "transactionRows": "#transactionTable tbody tr"
    },
    "credentialName": "demo-bank-credentials"
  }
}
```

#### Deliverables
✅ Scraper service with Puppeteer
✅ Job execution API endpoint
✅ Demo banking portal for testing
✅ Transaction parsing utilities
✅ Error handling and screenshots

---

### **Phase 5: BigQuery Integration (Optional)** (Week 5)
**Duration:** 4-5 days
**Goal:** Set up BigQuery data warehouse and sync

#### BigQuery Setup Script

```typescript
// scripts/setup-bigquery.ts
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery({
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.GCP_KEY_FILE,
});

const datasetId = process.env.BIGQUERY_DATASET || 'rpa_warehouse';

async function setupBigQuery() {
  console.log('Creating BigQuery dataset and tables...');

  // Create dataset
  const [dataset] = await bigquery.dataset(datasetId).get({ autoCreate: true });
  console.log(`Dataset ${dataset.id} ready`);

  // Create transactions table
  const transactionsSchema = [
    { name: 'id', type: 'STRING', mode: 'REQUIRED' },
    { name: 'job_id', type: 'STRING', mode: 'REQUIRED' },
    { name: 'date', type: 'TIMESTAMP', mode: 'REQUIRED' },
    { name: 'description', type: 'STRING', mode: 'NULLABLE' },
    { name: 'amount', type: 'FLOAT', mode: 'REQUIRED' },
    { name: 'currency', type: 'STRING', mode: 'REQUIRED' },
    { name: 'category', type: 'STRING', mode: 'NULLABLE' },
    { name: 'raw_data', type: 'JSON', mode: 'NULLABLE' },
    { name: 'created_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
  ];

  const [transactionsTable] = await dataset.table('fact_banking_transactions').get({
    autoCreate: true,
    schema: transactionsSchema,
  });
  console.log(`Table ${transactionsTable.id} ready`);

  // Create jobs table
  const jobsSchema = [
    { name: 'id', type: 'STRING', mode: 'REQUIRED' },
    { name: 'name', type: 'STRING', mode: 'REQUIRED' },
    { name: 'status', type: 'STRING', mode: 'REQUIRED' },
    { name: 'started_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
    { name: 'completed_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
    { name: 'created_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
  ];

  const [jobsTable] = await dataset.table('fact_job_executions').get({
    autoCreate: true,
    schema: jobsSchema,
  });
  console.log(`Table ${jobsTable.id} ready`);

  console.log('BigQuery setup complete!');
}

setupBigQuery().catch(console.error);
```

Run with:
```bash
npx tsx scripts/setup-bigquery.ts
```

#### BigQuery Service

```typescript
// src/lib/services/bigqueryService.ts
import { BigQuery } from '@google-cloud/bigquery';
import { Transaction, Job } from '@prisma/client';

const bigquery = new BigQuery({
  projectId: process.env.GCP_PROJECT_ID,
  keyFilename: process.env.GCP_KEY_FILE,
});

const dataset = bigquery.dataset(process.env.BIGQUERY_DATASET || 'rpa_warehouse');

export class BigQueryService {
  /**
   * Insert transactions to BigQuery
   */
  async insertTransactions(transactions: Transaction[]) {
    const table = dataset.table('fact_banking_transactions');

    const rows = transactions.map((tx) => ({
      id: tx.id,
      job_id: tx.jobId,
      date: tx.date.toISOString(),
      description: tx.description,
      amount: tx.amount,
      currency: tx.currency,
      category: tx.category,
      raw_data: tx.rawData,
      created_at: tx.createdAt.toISOString(),
    }));

    await table.insert(rows);

    return { success: true, count: rows.length };
  }

  /**
   * Insert job execution record
   */
  async insertJobExecution(job: Job) {
    const table = dataset.table('fact_job_executions');

    const row = {
      id: job.id,
      name: job.name,
      status: job.status,
      started_at: job.startedAt?.toISOString(),
      completed_at: job.completedAt?.toISOString(),
      created_at: job.createdAt.toISOString(),
    };

    await table.insert([row]);

    return { success: true };
  }

  /**
   * Query transactions from BigQuery
   */
  async queryTransactions(filters: {
    startDate: Date;
    endDate: Date;
    limit?: number;
  }) {
    const query = `
      SELECT *
      FROM \`${process.env.GCP_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.fact_banking_transactions\`
      WHERE date >= @startDate
      AND date <= @endDate
      ORDER BY date DESC
      LIMIT @limit
    `;

    const options = {
      query,
      params: {
        startDate: filters.startDate.toISOString(),
        endDate: filters.endDate.toISOString(),
        limit: filters.limit || 1000,
      },
    };

    const [rows] = await bigquery.query(options);
    return rows;
  }
}

export const bigqueryService = new BigQueryService();
```

#### Update Job Execution to Sync with BigQuery

```typescript
// Add to src/app/api/jobs/[id]/execute/route.ts
// After storing transactions in PostgreSQL:

// Sync to BigQuery (optional)
if (process.env.BIGQUERY_DATASET) {
  try {
    await bigqueryService.insertTransactions(
      await prisma.transaction.findMany({
        where: { jobId: job.id },
      })
    );
    await bigqueryService.insertJobExecution(job);
  } catch (error) {
    console.error('BigQuery sync failed:', error);
    // Don't fail the job if BigQuery sync fails
  }
}
```

#### Deliverables
✅ BigQuery dataset and tables created
✅ BigQuery service for data sync
✅ Automatic sync on job completion
✅ Query utilities for analytics

---

## Detailed Technical Specifications

### Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | Next.js 14, React, TailwindCSS | UI and presentation |
| **Backend** | Next.js API Routes | RESTful API |
| **Database** | PostgreSQL + Prisma ORM | Primary data storage |
| **Encryption** | Node.js crypto (AES-256-GCM) | Credential security |
| **Web Scraping** | Puppeteer | Browser automation |
| **Data Warehouse** | Google BigQuery (optional) | Analytics |
| **Validation** | Zod | Request validation |
| **Utilities** | date-fns | Date parsing |

### System Requirements

**Development:**
- Node.js >= 18.0.0
- npm >= 9.0.0
- PostgreSQL >= 14
- 4GB RAM minimum
- Chrome/Chromium (for Puppeteer)

**Production:**
- Node.js >= 18.0.0
- PostgreSQL >= 14
- 8GB RAM recommended
- Google Cloud account (for BigQuery)

### Environment Variables Reference

```bash
# Database
DATABASE_URL="postgresql://user:password@localhost:5432/rpa_db"

# Security
ENCRYPTION_MASTER_KEY="<64-character-hex-string>"

# Node Environment
NODE_ENV="development"

# BigQuery (Optional)
GCP_PROJECT_ID="your-project-id"
GCP_KEY_FILE="./credentials/gcp-service-account.json"
BIGQUERY_DATASET="rpa_warehouse"

# Next.js
NEXT_PUBLIC_API_URL="http://localhost:3000"
```

---

## What to Skip for MVP

### ❌ Not Needed for MVP

1. **Queue System (BullMQ + Redis)**
   - Run jobs synchronously in API routes
   - Add later for production scalability

2. **Worker Processes**
   - Execute scraping in API route
   - Separate workers for production

3. **WebSocket Real-time Updates**
   - Use polling for job status
   - Add WebSockets in v2

4. **AI Features**
   - TensorFlow.js computer vision
   - Generative AI (GPT-4, Gemini, DeepSeek)
   - Add in future versions

5. **Multiple Banking Networks**
   - Focus on 1-2 networks for MVP
   - Use demo portal for testing

6. **Advanced ETL Pipeline**
   - Basic data transformation is enough
   - Full pipeline in production

7. **Power BI Integration**
   - Use BigQuery UI for queries
   - Power BI connector later

8. **Audit Logging System**
   - Use console.log for debugging
   - Proper logging in production

9. **Job Scheduling (Cron)**
   - Manual job execution only
   - Add scheduling in v2

10. **Advanced Error Handling**
    - Basic try/catch is sufficient
    - Retry logic in production

---

## Success Criteria

### MVP is Complete When:

✅ **Data Extraction Works**
- [ ] Can create a job via UI
- [ ] Job connects to demo banking portal
- [ ] Successfully extracts transaction data
- [ ] Stores data in PostgreSQL

✅ **Security Implemented**
- [ ] Credentials are encrypted with AES-256-GCM
- [ ] Master key stored securely in environment
- [ ] No plaintext passwords in database

✅ **Frontend Integration**
- [ ] Can view list of jobs
- [ ] Can see job status (pending/running/completed/failed)
- [ ] Can view extracted transactions
- [ ] Real data displayed (not mock)

✅ **Optional: BigQuery**
- [ ] Transactions synced to BigQuery
- [ ] Can query data from BigQuery
- [ ] Analytics queries work

### Testing Checklist

**End-to-End Test:**
1. Create credential via API
2. Create job with demo bank config
3. Execute job via API
4. Verify job status changes: PENDING → RUNNING → COMPLETED
5. Check transactions stored in PostgreSQL
6. View transactions in frontend UI
7. (Optional) Verify data in BigQuery

---

## Post-MVP Roadmap

### Version 2.0 Features (After MVP)

**Phase 6: Production-Ready (4 weeks)**
- Implement queue system (BullMQ + Redis)
- Separate worker processes
- Add job scheduling (cron expressions)
- Advanced error handling and retry logic
- Comprehensive logging and monitoring

**Phase 7: Scale & Performance (3 weeks)**
- WebSocket real-time updates
- Horizontal scaling support
- Database optimizations (indexes, partitioning)
- Caching layer (Redis)
- Rate limiting

**Phase 8: Advanced Features (6 weeks)**
- AI-powered adaptive selectors (TensorFlow.js)
- Multiple banking network templates
- Advanced ETL pipeline
- Power BI connector
- Audit logging system

**Phase 9: Enterprise Features (8 weeks)**
- Generative AI integration (GPT-4, Gemini, DeepSeek)
- Multi-tenant architecture
- Role-based access control (RBAC)
- Compliance certifications (SOC2, PCI-DSS)
- White-label capabilities

---

## Development Best Practices

### Git Workflow
```bash
# Feature branches
git checkout -b feature/database-setup
git checkout -b feature/scraper-service
git checkout -b feature/bigquery-integration

# Commit often with clear messages
git commit -m "feat: implement credential encryption service"
git commit -m "fix: handle date parsing errors in scraper"
git commit -m "docs: add API endpoint documentation"
```

### Code Organization
```
src/
├── app/                    # Next.js pages and API routes
│   ├── api/                # API endpoints
│   │   ├── jobs/           # Job endpoints
│   │   ├── credentials/    # Credential endpoints
│   │   └── transactions/   # Transaction endpoints
│   └── ...                 # Frontend pages
├── lib/                    # Core utilities
│   ├── prisma.ts           # Prisma client
│   ├── encryption.ts       # Encryption utilities
│   └── services/           # Business logic services
│       ├── credentialService.ts
│       ├── scraperService.ts
│       └── bigqueryService.ts
└── types/                  # TypeScript types (already exists)
```

### Testing Strategy
- **Unit Tests**: Encryption, date parsing, amount parsing
- **Integration Tests**: API endpoints with test database
- **E2E Tests**: Full flow from UI to database
- **Manual Testing**: Demo bank portal scraping

### Documentation
- Update README.md with setup instructions
- Document API endpoints (OpenAPI/Swagger)
- Add code comments for complex logic
- Keep this MVP plan updated

---

## Risk Mitigation

### Potential Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Puppeteer fails on certain sites | High | Start with demo portal, test thoroughly |
| Credential encryption key leaked | Critical | Use strong key, never commit to git |
| Database performance issues | Medium | Add indexes, optimize queries |
| BigQuery costs too high | Medium | Set quotas, monitor usage |
| Web selectors break | High | Build selector recovery mechanism (v2) |
| Job execution timeout | Medium | Add timeout handling, job queues (v2) |

### Mitigation Strategies

1. **Start with Demo Portal**: Test all functionality with controlled demo site
2. **Incremental Rollout**: Add real banking sites one at a time
3. **Error Logging**: Capture screenshots and HTML on errors
4. **Cost Monitoring**: Set up GCP billing alerts
5. **Regular Backups**: Daily PostgreSQL backups
6. **Security Audits**: Review encryption implementation

---

## Resources & References

### Documentation
- [Next.js 14 Docs](https://nextjs.org/docs)
- [Prisma Docs](https://www.prisma.io/docs)
- [Puppeteer Docs](https://pptr.dev/)
- [BigQuery Docs](https://cloud.google.com/bigquery/docs)
- [Node.js Crypto](https://nodejs.org/api/crypto.html)

### Tutorials
- [Prisma with Next.js](https://www.prisma.io/nextjs)
- [Web Scraping with Puppeteer](https://www.scrapingbee.com/blog/web-scraping-with-puppeteer/)
- [AES-256-GCM Encryption in Node.js](https://nodejs.org/en/knowledge/cryptography/how-to-use-crypto-module/)

### Tools
- [Prisma Studio](https://www.prisma.io/studio) - Database GUI
- [Postman](https://www.postman.com/) - API testing
- [Railway](https://railway.app/) - PostgreSQL hosting
- [Neon](https://neon.tech/) - Serverless PostgreSQL

---

## Conclusion

This MVP development plan provides a **realistic, achievable roadmap** to transform the current documentation-only platform into a **working proof-of-concept** in 4-6 weeks.

### Key Takeaways

✅ **Start Simple**: Focus on core functionality first
✅ **One Banking Network**: Master demo portal before adding complexity
✅ **Iterate Quickly**: Get something working, then improve
✅ **Skip Non-Essentials**: No queues, AI, or advanced features for MVP
✅ **Test Thoroughly**: Use demo portal to validate all functionality

### Next Steps

1. **Week 1**: Set up database and encryption
2. **Week 2**: Build API routes and credential service
3. **Week 3-4**: Implement web scraper
4. **Week 5**: (Optional) BigQuery integration
5. **Week 6**: Testing, bug fixes, documentation

**Good luck building your MVP! 🚀**

---

**Document Version:** 1.0
**Last Updated:** January 2025
**Author:** Claude Sonnet 4.5
**Status:** Ready for Implementation
