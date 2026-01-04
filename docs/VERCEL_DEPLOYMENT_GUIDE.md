# Vercel Deployment Guide - RPA Automation Platform

**Platform:** Vercel (Next.js)
**Application Type:** Next.js 14 (App Router)
**Deployment Time:** 5-10 minutes
**Cost:** Free tier available, Pro tier recommended for production

---

## 📋 Table of Contents

1. [Current Deployment Status](#current-deployment-status)
2. [Quick Deployment (Frontend Only)](#quick-deployment-frontend-only)
3. [Full Stack Deployment Strategy](#full-stack-deployment-strategy)
4. [Step-by-Step Deployment](#step-by-step-deployment)
5. [Environment Variables Setup](#environment-variables-setup)
6. [Database Configuration](#database-configuration)
7. [Vercel Limitations & Workarounds](#vercel-limitations--workarounds)
8. [Production Deployment Checklist](#production-deployment-checklist)

---

## Current Deployment Status

### ✅ What's Ready to Deploy

Your current application is a **documentation/presentation layer** that can be deployed to Vercel immediately:

- ✅ Next.js 14 frontend (all pages)
- ✅ Static documentation
- ✅ UI components
- ✅ TypeScript configuration
- ✅ TailwindCSS styling

### ⚠️ What's NOT Implemented (Yet)

- ❌ Backend API routes (no actual logic)
- ❌ Database connection (PostgreSQL)
- ❌ Web scraper (Puppeteer)
- ❌ Credential encryption service
- ❌ BigQuery integration
- ❌ Job execution engine

### Current Deployment = **Documentation Site Only**

When you deploy now, you'll get a beautiful, fast documentation site showcasing your platform's capabilities, but **no actual data extraction functionality**.

---

## Quick Deployment (Frontend Only)

### Option 1: Deploy via Vercel Dashboard (Easiest)

**Step 1: Connect GitHub Repository**
1. Go to [vercel.com](https://vercel.com)
2. Sign up/login with GitHub
3. Click "Add New..." → "Project"
4. Import your `rpa-automation-platform` repository
5. Vercel auto-detects Next.js settings

**Step 2: Configure Project**
```
Framework Preset: Next.js
Build Command: npm run build
Output Directory: .next
Install Command: npm install
```

**Step 3: Deploy**
- Click "Deploy"
- Wait 2-3 minutes
- Get your URL: `https://rpa-automation-platform.vercel.app`

**That's it!** Your documentation site is now live.

---

### Option 2: Deploy via Vercel CLI

```bash
# Install Vercel CLI
npm install -g vercel

# Login to Vercel
vercel login

# Deploy (from project root)
cd /Users/drpanda/RPA/rpa-automation-platform
vercel

# Follow prompts:
# - Set up and deploy? Yes
# - Which scope? <your-username>
# - Link to existing project? No
# - What's your project's name? rpa-automation-platform
# - In which directory is your code located? ./
# - Want to modify settings? No

# Deploy to production
vercel --prod
```

Your site will be live at: `https://rpa-automation-platform-<hash>.vercel.app`

---

## Full Stack Deployment Strategy

### Vercel Architecture Considerations

Vercel is optimized for **serverless functions** but has limitations for RPA use cases:

| Feature | Vercel Support | Limitation | Solution |
|---------|----------------|------------|----------|
| **Next.js Frontend** | ✅ Perfect | None | Deploy directly |
| **API Routes** | ✅ Good | 10s timeout (Hobby), 60s (Pro) | Fine for lightweight APIs |
| **PostgreSQL** | ⚠️ External | No built-in DB | Use Vercel Postgres or Neon |
| **Puppeteer** | ❌ Difficult | Large bundle, memory limits | Use external service or serverless Chrome |
| **Long-running Jobs** | ❌ No | Max 60s execution | Use external workers |
| **BigQuery** | ✅ Good | None | Works via SDK |
| **WebSocket** | ❌ No | Serverless doesn't support | Use Pusher or Ably |
| **Queue System** | ❌ No | Can't run Redis/BullMQ | Use Vercel KV or Upstash |

### Recommended Architecture for Vercel

```
┌─────────────────────────────────────────────────────────────────┐
│                     VERCEL DEPLOYMENT                            │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐     │
│  │          Next.js Frontend + API Routes                 │     │
│  │  - Pages (documentation, UI)                           │     │
│  │  - API routes (lightweight CRUD only)                  │     │
│  │  - Serverless functions (< 60s execution)              │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                  │
└───────────┬──────────────────────────────────────┬──────────────┘
            │                                      │
            ↓                                      ↓
┌───────────────────────┐            ┌──────────────────────────┐
│   Vercel Postgres     │            │  External Worker Service │
│   or Neon Database    │            │  (Railway, Render, Fly)  │
│  - Jobs table         │            │  - Puppeteer scraping    │
│  - Credentials table  │            │  - Long-running jobs     │
│  - Transactions table │            │  - Queue workers         │
└───────────────────────┘            └──────────────────────────┘
            │                                      │
            ↓                                      ↓
┌───────────────────────┐            ┌──────────────────────────┐
│   Google BigQuery     │            │    Browserless.io        │
│  - Analytics          │            │  - Serverless Chrome     │
│  - Data warehouse     │            │  - Puppeteer execution   │
└───────────────────────┘            └──────────────────────────┘
```

### Deployment Strategy

**Phase 1: Documentation Site (Now)**
- Deploy frontend to Vercel
- Static pages only
- No backend functionality

**Phase 2: API Routes (When MVP is built)**
- Add Vercel Postgres
- Implement API routes
- CRUD operations only (< 10s execution)

**Phase 3: Worker Services (For scraping)**
- Deploy workers to Railway/Render
- Workers handle Puppeteer scraping
- API routes trigger workers via HTTP

---

## Step-by-Step Deployment

### Step 1: Prepare Repository

```bash
# Ensure all changes are committed
git status
git add .
git commit -m "Prepare for Vercel deployment"
git push origin master
```

### Step 2: Create Vercel Project

**Via Dashboard:**
1. Go to [vercel.com/new](https://vercel.com/new)
2. Import Git Repository
3. Select `HKKoho/rpa-automation-platform`
4. Configure:
   - **Framework**: Next.js
   - **Root Directory**: `./`
   - **Build Command**: `npm run build`
   - **Output Directory**: `.next`
   - **Install Command**: `npm install`

**Via CLI:**
```bash
vercel
# Follow interactive prompts
```

### Step 3: Configure Build Settings

Vercel auto-detects Next.js, but verify in `vercel.json`:

```json
{
  "buildCommand": "npm run build",
  "devCommand": "npm run dev",
  "installCommand": "npm install",
  "framework": "nextjs",
  "outputDirectory": ".next"
}
```

### Step 4: Set Environment Variables

In Vercel Dashboard → Project Settings → Environment Variables:

**For Current Deployment (Documentation Only):**
```bash
NODE_ENV=production
NEXT_PUBLIC_APP_VERSION=2.0.0
```

**For Future MVP Deployment (With Backend):**
```bash
# Database (Vercel Postgres or Neon)
DATABASE_URL=<vercel-postgres-connection-string>

# Encryption
ENCRYPTION_MASTER_KEY=<64-character-hex>

# BigQuery (Optional)
GCP_PROJECT_ID=<your-project-id>
GCP_KEY_FILE=<base64-encoded-service-account-json>
BIGQUERY_DATASET=rpa_warehouse

# Feature Flags
ENABLE_SCRAPING=false  # Set to true when workers are ready
```

### Step 5: Deploy

```bash
# Deploy to preview (staging)
vercel

# Deploy to production
vercel --prod
```

### Step 6: Verify Deployment

Visit your deployment URL:
```
https://rpa-automation-platform.vercel.app
```

Check:
- ✅ Homepage loads
- ✅ Documentation pages work
- ✅ Features page displays correctly
- ✅ Architecture page shows 4-tier design
- ✅ Examples page shows code samples
- ✅ Pricing page loads
- ✅ All styling works

---

## Environment Variables Setup

### Current Deployment (Documentation Site)

**Minimal Variables:**
```bash
NODE_ENV=production
```

### Future MVP Deployment

#### Option A: Vercel Postgres (Recommended)

**Setup Vercel Postgres:**
1. Go to Vercel Dashboard → Storage
2. Create → Postgres
3. Connect to your project
4. Vercel automatically adds `POSTGRES_URL` environment variable

**Required Variables:**
```bash
# Database (auto-added by Vercel)
POSTGRES_URL=<vercel-postgres-url>
POSTGRES_PRISMA_URL=<prisma-connection-pooling-url>

# Encryption
ENCRYPTION_MASTER_KEY=<generate-with-script>

# Node Environment
NODE_ENV=production
```

#### Option B: External Database (Neon, Railway, Supabase)

**Required Variables:**
```bash
# External Database
DATABASE_URL=postgresql://user:password@host:5432/database

# Encryption
ENCRYPTION_MASTER_KEY=<64-character-hex>

# Node Environment
NODE_ENV=production

# Optional - BigQuery
GCP_PROJECT_ID=your-project-id
GCP_KEY_FILE=<base64-encoded-json>
BIGQUERY_DATASET=rpa_warehouse
```

### How to Add Environment Variables

**Via Vercel Dashboard:**
1. Go to Project Settings
2. Navigate to "Environment Variables"
3. Add each variable:
   - **Key**: `DATABASE_URL`
   - **Value**: `postgresql://...`
   - **Environment**: Production, Preview, Development

**Via Vercel CLI:**
```bash
# Add environment variable
vercel env add DATABASE_URL production

# Pull environment variables locally
vercel env pull .env.local
```

---

## Database Configuration

### Option 1: Vercel Postgres (Easiest)

**Pros:**
- ✅ Fully managed by Vercel
- ✅ Automatic environment variable setup
- ✅ Integrated into Vercel dashboard
- ✅ Connection pooling built-in

**Cons:**
- ❌ Limited to Vercel Pro plan ($20/month)
- ❌ 256MB database limit on Hobby tier
- ❌ Additional costs for larger databases

**Setup:**
```bash
# 1. Create Vercel Postgres via dashboard
# 2. Connect to project
# 3. Install Prisma
npm install @prisma/client prisma

# 4. Initialize Prisma (if not done)
npx prisma init

# 5. Update schema to use Vercel Postgres
# (DATABASE_URL is automatically set)

# 6. Push schema to database
npx prisma db push

# 7. Generate Prisma client
npx prisma generate
```

**Connection String Format:**
```
postgres://user:password@host:5432/database?sslmode=require&pgbouncer=true
```

---

### Option 2: Neon (Recommended for MVP)

**Pros:**
- ✅ Serverless PostgreSQL
- ✅ Free tier: 512MB storage
- ✅ Auto-scaling
- ✅ Vercel integration
- ✅ Connection pooling

**Setup:**
1. Sign up at [neon.tech](https://neon.tech)
2. Create project
3. Get connection string
4. Add to Vercel environment variables:
   ```bash
   DATABASE_URL=postgresql://user:password@host.neon.tech/dbname?sslmode=require
   ```

---

### Option 3: Railway (Good for Workers)

**Pros:**
- ✅ PostgreSQL + Redis + Worker processes
- ✅ $5 free credit per month
- ✅ Deploy workers alongside database
- ✅ Simple CLI

**Setup:**
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Create project
railway init

# Add PostgreSQL
railway add postgresql

# Get connection string
railway variables

# Add to Vercel
vercel env add DATABASE_URL production
```

---

### Option 4: Supabase

**Pros:**
- ✅ PostgreSQL with REST API
- ✅ Free tier: 500MB database
- ✅ Built-in auth (if needed later)
- ✅ Real-time subscriptions

**Setup:**
1. Sign up at [supabase.com](https://supabase.com)
2. Create project
3. Go to Settings → Database
4. Copy connection string (Pooler mode)
5. Add to Vercel environment variables

---

## Vercel Limitations & Workarounds

### 1. Puppeteer Not Supported

**Problem:**
- Puppeteer bundle too large (> 50MB)
- Chrome binary not available in serverless
- Memory limits (1GB max)

**Solution A: Use Browserless.io (Recommended)**

```typescript
// src/lib/services/scraperService.ts
import puppeteer from 'puppeteer-core';

const browser = await puppeteer.connect({
  browserWSEndpoint: `wss://chrome.browserless.io?token=${process.env.BROWSERLESS_TOKEN}`,
});
```

**Pricing:** $15-75/month for Browserless.io

**Solution B: Use @sparticuz/chromium**

```bash
npm install puppeteer-core @sparticuz/chromium
```

```typescript
import puppeteer from 'puppeteer-core';
import chromium from '@sparticuz/chromium';

const browser = await puppeteer.launch({
  args: chromium.args,
  executablePath: await chromium.executablePath(),
  headless: chromium.headless,
});
```

**Limitations:**
- Still hits 50MB limit
- Slower cold starts
- Max 1GB memory

**Solution C: Deploy Workers Externally (Best for Production)**

Deploy scraping workers to Railway, Render, or Fly.io:

```typescript
// Vercel API route triggers external worker
export async function POST(request: Request) {
  const { jobId } = await request.json();

  // Trigger external worker via HTTP
  await fetch('https://your-worker.railway.app/scrape', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${process.env.WORKER_API_KEY}` },
    body: JSON.stringify({ jobId }),
  });

  return NextResponse.json({ success: true });
}
```

---

### 2. Execution Timeout

**Problem:**
- Hobby: 10 seconds max
- Pro: 60 seconds max
- No long-running processes

**Solution:**
- Keep API routes lightweight (< 10s)
- Offload heavy work to external workers
- Use job queues for async processing

```typescript
// Good: Quick API response
export async function POST(request: Request) {
  const job = await prisma.job.create({ data: { ...jobData, status: 'PENDING' } });

  // Queue job for processing (external worker picks it up)
  await queueService.enqueue('scrape-job', { jobId: job.id });

  return NextResponse.json({ success: true, jobId: job.id });
}
```

---

### 3. No WebSocket Support

**Problem:**
- Serverless doesn't support persistent connections
- Can't run WebSocket server

**Solution A: Use Pusher**

```bash
npm install pusher pusher-js
```

```typescript
// Server-side: Trigger events
import Pusher from 'pusher';

const pusher = new Pusher({
  appId: process.env.PUSHER_APP_ID!,
  key: process.env.PUSHER_KEY!,
  secret: process.env.PUSHER_SECRET!,
  cluster: 'us2',
});

await pusher.trigger('jobs', 'job-completed', { jobId });

// Client-side: Listen for events
import Pusher from 'pusher-js';

const pusher = new Pusher(process.env.NEXT_PUBLIC_PUSHER_KEY!, {
  cluster: 'us2',
});

pusher.subscribe('jobs').bind('job-completed', (data) => {
  console.log('Job completed:', data);
});
```

**Pricing:** Free tier: 200k messages/day

**Solution B: Use Ably**

Similar to Pusher, but more generous free tier.

**Solution C: Polling**

```typescript
// Frontend: Poll for job status
useEffect(() => {
  const interval = setInterval(async () => {
    const response = await fetch(`/api/jobs/${jobId}`);
    const job = await response.json();

    if (job.status === 'COMPLETED' || job.status === 'FAILED') {
      clearInterval(interval);
      setJobStatus(job.status);
    }
  }, 3000); // Poll every 3 seconds

  return () => clearInterval(interval);
}, [jobId]);
```

---

### 4. No Queue System (BullMQ/Redis)

**Problem:**
- Can't run Redis on Vercel
- Can't run persistent queue workers

**Solution A: Vercel KV (Redis)**

```bash
npm install @vercel/kv
```

```typescript
import { kv } from '@vercel/kv';

// Queue job
await kv.lpush('job-queue', JSON.stringify({ jobId }));

// External worker dequeues jobs
const job = await kv.rpop('job-queue');
```

**Pricing:** Free tier: 30k requests/month

**Solution B: Upstash Redis**

Works with Vercel, similar API to Vercel KV.

**Solution C: External Queue Service**

- Use Railway/Render for Redis + workers
- Vercel API routes enqueue jobs
- External workers process jobs

---

## Production Deployment Checklist

### Pre-Deployment

- [ ] All code committed and pushed to GitHub
- [ ] Environment variables documented
- [ ] Database schema finalized
- [ ] Build succeeds locally (`npm run build`)
- [ ] TypeScript checks pass (`npm run type-check`)
- [ ] Linting passes (`npm run lint`)

### Vercel Setup

- [ ] GitHub repository connected to Vercel
- [ ] Framework preset set to Next.js
- [ ] Environment variables added
- [ ] Custom domain configured (optional)
- [ ] SSL certificate active (automatic)

### Database Setup

- [ ] Database provisioned (Vercel Postgres, Neon, etc.)
- [ ] Connection string added to environment variables
- [ ] Prisma schema pushed to database
- [ ] Test data seeded (optional)

### Security

- [ ] Encryption master key generated and secured
- [ ] No secrets committed to repository
- [ ] API routes have proper authentication (future)
- [ ] CORS configured correctly (if needed)

### Monitoring

- [ ] Vercel Analytics enabled
- [ ] Error tracking configured (Sentry, optional)
- [ ] Uptime monitoring (optional)

### Testing

- [ ] Test deployment in preview environment
- [ ] Verify all pages load
- [ ] Check API routes (when implemented)
- [ ] Test database connection
- [ ] Verify environment variables work

### Go Live

- [ ] Deploy to production (`vercel --prod`)
- [ ] Verify production URL
- [ ] Test critical user flows
- [ ] Monitor for errors in first hour
- [ ] Update DNS (if using custom domain)

---

## Custom Domain Setup

### Add Custom Domain to Vercel

**Step 1: Add Domain in Vercel**
1. Go to Project Settings → Domains
2. Enter your domain: `rpa.yourdomain.com`
3. Click "Add"

**Step 2: Configure DNS**

Add these records in your DNS provider:

**For subdomain (rpa.yourdomain.com):**
```
Type: CNAME
Name: rpa
Value: cname.vercel-dns.com
```

**For root domain (yourdomain.com):**
```
Type: A
Name: @
Value: 76.76.21.21

Type: AAAA (IPv6)
Name: @
Value: 2606:4700:10::6814:1515
```

**Step 3: Verify**
- Vercel automatically provisions SSL certificate
- Domain should be active within 1-2 hours

---

## Cost Estimate

### Hobby Tier (Free)
- ✅ Next.js deployment
- ✅ 100GB bandwidth
- ✅ Serverless function executions
- ❌ No Vercel Postgres
- ❌ 10s function timeout

**Cost:** $0/month

### Pro Tier (Recommended for MVP)
- ✅ Everything in Hobby
- ✅ Vercel Postgres (256MB)
- ✅ 60s function timeout
- ✅ Custom domains
- ✅ Team collaboration

**Cost:** $20/month + usage

### Additional Services
- **Neon Database:** Free (512MB) or $19/month (Pro)
- **Browserless.io:** $15-75/month
- **Pusher:** Free (200k messages/day) or $49/month
- **BigQuery:** ~$10-50/month (depends on usage)

**Total MVP Cost:** $20-100/month

---

## Troubleshooting

### Build Fails

**Error:** `Module not found`
```bash
# Solution: Ensure all dependencies are in package.json
npm install
git add package.json package-lock.json
git commit -m "Update dependencies"
git push
```

**Error:** `TypeScript errors`
```bash
# Solution: Fix TypeScript errors locally
npm run type-check
# Fix errors, then redeploy
```

### Environment Variables Not Working

```typescript
// Server-side: Use process.env
const dbUrl = process.env.DATABASE_URL;

// Client-side: Must prefix with NEXT_PUBLIC_
const apiUrl = process.env.NEXT_PUBLIC_API_URL;
```

### Database Connection Issues

```bash
# Check connection string format
# PostgreSQL format:
postgresql://user:password@host:5432/database?sslmode=require

# Test connection locally
npx prisma db push
```

### Puppeteer Bundle Too Large

```bash
# Solution: Use puppeteer-core + @sparticuz/chromium
npm install puppeteer-core @sparticuz/chromium
npm uninstall puppeteer
```

---

## Next Steps After Deployment

1. **Monitor Deployment**
   - Check Vercel dashboard for errors
   - Review function logs
   - Monitor performance metrics

2. **Implement MVP Backend**
   - Follow `MVP_DEVELOPMENT_PLAN.md`
   - Add database integration
   - Implement API routes

3. **Add External Workers**
   - Deploy scraping workers to Railway
   - Configure job queue
   - Connect workers to Vercel API

4. **Enable Analytics**
   - Add Vercel Analytics
   - Set up error tracking
   - Monitor user behavior

5. **Scale Gradually**
   - Start with demo banking portal
   - Add real banking networks one by one
   - Monitor costs and performance

---

## Resources

- [Vercel Documentation](https://vercel.com/docs)
- [Next.js Deployment](https://nextjs.org/docs/deployment)
- [Vercel Postgres](https://vercel.com/docs/storage/vercel-postgres)
- [Neon Database](https://neon.tech/docs)
- [Railway](https://docs.railway.app/)
- [Browserless.io](https://www.browserless.io/docs)

---

## Conclusion

Your RPA Automation Platform is **ready to deploy to Vercel right now** as a documentation site. For full functionality with backend services, you'll need to:

1. Build the MVP following `MVP_DEVELOPMENT_PLAN.md`
2. Add database (Vercel Postgres or Neon)
3. Deploy workers externally (Railway/Render) for Puppeteer
4. Use managed services for WebSocket and queues

**Quick Start:**
```bash
vercel --prod
```

Your documentation site will be live in minutes! 🚀

---

**Document Version:** 1.0
**Last Updated:** January 2025
**Deployment Status:** ✅ Ready for Vercel
