# RPA Software Integration Guide

This document outlines how to integrate existing RPA software solutions into the Banking Network Utility RPA platform.

---

## Recommended Solutions

### **Option 1: UiPath (Enterprise-Grade)**
**License:** Commercial (Enterprise ~$420/bot/month)
**Best For:** Large banks, clearinghouses, compliance-heavy environments

### **Option 2: Robocorp (Cloud-Native)**
**License:** Open-source + Commercial cloud orchestration
**Best For:** Startups, mid-market banks, developer-friendly teams

---

## Architecture Integration Patterns

### **Hybrid Architecture Pattern**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Your Platform (Orchestrator)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Next.js UI   │  │ RPAEngine    │  │ Job Scheduler│          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         │                  │                  │                   │
│         └──────────────────┴──────────────────┘                   │
│                            │                                       │
│                    ┌───────┴────────┐                            │
│                    │  Queue Manager  │                            │
│                    │   (BullMQ)      │                            │
│                    └───────┬────────┘                            │
└────────────────────────────┼──────────────────────────────────────┘
                             │
                ┌────────────┴────────────┐
                │                         │
        ┌───────▼──────┐         ┌───────▼──────┐
        │   UiPath     │         │  Robocorp    │
        │ Orchestrator │         │    Cloud     │
        └───────┬──────┘         └───────┬──────┘
                │                         │
        ┌───────▼──────┐         ┌───────▼──────┐
        │  UiPath      │         │  Robocorp    │
        │  Robots      │         │   Workers    │
        └───────┬──────┘         └───────┬──────┘
                │                         │
                └────────────┬────────────┘
                             │
                    ┌────────▼────────┐
                    │ Banking Network │
                    │   (ACH, SWIFT,  │
                    │  Banks, etc.)   │
                    └─────────────────┘
```

---

## Option 1: UiPath Integration

### **Why UiPath?**

**Strengths:**
- ✅ Industry leader in enterprise RPA (41% market share)
- ✅ **Banking-specific connectors** (SWIFT, ACH, FedWire, SAP)
- ✅ **Orchestrator** for centralized bot management (similar to your RPAEngine)
- ✅ **Strong security** (PCI-DSS, SOC2 certified, encryption at rest/transit)
- ✅ **Audit trails** built-in (compliance requirement for banking)
- ✅ **Document Understanding** (AI/ML for invoice, statement parsing)
- ✅ **Enterprise support** (24/7 support for production incidents)

**Weaknesses:**
- ❌ Expensive licensing ($420/robot/month + Orchestrator fees)
- ❌ Windows-centric (Linux support limited)
- ❌ Proprietary stack (vendor lock-in risk)

### **Integration Architecture**

#### **Pattern: Orchestrator-to-Orchestrator**

```typescript
// src/integrations/uipath/UiPathConnector.ts

import axios from 'axios';
import { CredentialVault } from '@/core/security/CredentialVault';
import { AuditLogger } from '@/core/security/AuditLogger';

/**
 * UiPath Orchestrator API Integration
 *
 * Routes complex web automation jobs to UiPath robots
 * while keeping job scheduling, credentials, and analytics in our platform
 */
export class UiPathConnector {
  private orchestratorUrl: string;
  private tenantName: string;
  private vault: CredentialVault;
  private auditLogger: AuditLogger;
  private accessToken?: string;
  private tokenExpiry?: Date;

  constructor() {
    this.orchestratorUrl = process.env.UIPATH_ORCHESTRATOR_URL || '';
    this.tenantName = process.env.UIPATH_TENANT_NAME || 'Default';
    this.vault = new CredentialVault();
    this.auditLogger = new AuditLogger();
  }

  /**
   * Authenticate with UiPath Orchestrator
   */
  async authenticate(): Promise<void> {
    if (this.accessToken && this.tokenExpiry && this.tokenExpiry > new Date()) {
      return; // Token still valid
    }

    try {
      // Retrieve UiPath credentials from vault
      const creds = await this.vault.retrieve('uipath-orchestrator');

      const response = await axios.post(
        `${this.orchestratorUrl}/api/Account/Authenticate`,
        {
          tenancyName: this.tenantName,
          usernameOrEmailAddress: creds.username,
          password: creds.password,
        }
      );

      this.accessToken = response.data.result;
      this.tokenExpiry = new Date(Date.now() + 60 * 60 * 1000); // 1 hour

      await this.auditLogger.log({
        action: 'uipath.authenticated',
        resource: 'uipath-orchestrator',
        result: 'success',
      });
    } catch (error) {
      await this.auditLogger.log({
        action: 'uipath.authentication.failed',
        resource: 'uipath-orchestrator',
        result: 'failure',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  /**
   * Start a UiPath process (robot job)
   *
   * @param processName - Name of the UiPath process to start
   * @param inputArguments - Input parameters for the process
   */
  async startJob(
    processName: string,
    inputArguments: Record<string, any>
  ): Promise<string> {
    await this.authenticate();

    try {
      const response = await axios.post(
        `${this.orchestratorUrl}/odata/Jobs/UiPath.Server.Configuration.OData.StartJobs`,
        {
          startInfo: {
            ReleaseKey: await this.getReleaseKey(processName),
            Strategy: 'Specific', // Run on specific robots
            RobotIds: [], // Auto-select available robot
            JobsCount: 1,
            InputArguments: JSON.stringify(inputArguments),
          },
        },
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
            'Content-Type': 'application/json',
            'X-UIPATH-TenantName': this.tenantName,
          },
        }
      );

      const jobId = response.data.value[0].Key;

      await this.auditLogger.log({
        action: 'uipath.job.started',
        resource: 'uipath-job',
        resourceId: jobId,
        changes: { processName, inputArguments },
      });

      return jobId;
    } catch (error) {
      throw new Error(`Failed to start UiPath job: ${error}`);
    }
  }

  /**
   * Get job status from UiPath Orchestrator
   */
  async getJobStatus(jobId: string): Promise<UiPathJobStatus> {
    await this.authenticate();

    const response = await axios.get(
      `${this.orchestratorUrl}/odata/Jobs(${jobId})`,
      {
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          'X-UIPATH-TenantName': this.tenantName,
        },
      }
    );

    const job = response.data;

    return {
      id: job.Key,
      state: job.State, // 'Pending', 'Running', 'Successful', 'Failed'
      outputArguments: job.OutputArguments ? JSON.parse(job.OutputArguments) : {},
      startTime: new Date(job.StartTime),
      endTime: job.EndTime ? new Date(job.EndTime) : undefined,
      errorMessage: job.Info,
    };
  }

  /**
   * Get release key for a process (required to start jobs)
   */
  private async getReleaseKey(processName: string): Promise<string> {
    await this.authenticate();

    const response = await axios.get(
      `${this.orchestratorUrl}/odata/Releases`,
      {
        params: {
          $filter: `ProcessKey eq '${processName}'`,
        },
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          'X-UIPATH-TenantName': this.tenantName,
        },
      }
    );

    if (response.data.value.length === 0) {
      throw new Error(`Process '${processName}' not found in UiPath Orchestrator`);
    }

    return response.data.value[0].Key;
  }

  /**
   * Cancel a running job
   */
  async cancelJob(jobId: string): Promise<void> {
    await this.authenticate();

    await axios.post(
      `${this.orchestratorUrl}/odata/Jobs(${jobId})/UiPath.Server.Configuration.OData.StopJob`,
      { strategy: 'Kill' },
      {
        headers: {
          Authorization: `Bearer ${this.accessToken}`,
          'X-UIPATH-TenantName': this.tenantName,
        },
      }
    );

    await this.auditLogger.log({
      action: 'uipath.job.cancelled',
      resource: 'uipath-job',
      resourceId: jobId,
    });
  }
}

export interface UiPathJobStatus {
  id: string;
  state: 'Pending' | 'Running' | 'Successful' | 'Failed' | 'Stopped';
  outputArguments: Record<string, any>;
  startTime: Date;
  endTime?: Date;
  errorMessage?: string;
}
```

#### **Integration Points**

**1. RPAEngine Delegation Logic**

```typescript
// src/core/engine/RPAEngine.ts

import { UiPathConnector } from '@/integrations/uipath/UiPathConnector';

export class RPAEngine extends EventEmitter {
  private uiPathConnector: UiPathConnector;

  constructor() {
    super();
    this.uiPathConnector = new UiPathConnector();
    // ... existing code
  }

  /**
   * Decide whether to use internal automation or delegate to UiPath
   */
  async extractData(params: ExtractionParams): Promise<ExtractedData> {
    // Decision logic: delegate complex web automation to UiPath
    if (this.shouldDelegateToUiPath(params)) {
      return this.extractViaUiPath(params);
    }

    // Use internal automation for API/simple cases
    return this.extractInternally(params);
  }

  /**
   * Delegation decision logic
   */
  private shouldDelegateToUiPath(params: ExtractionParams): boolean {
    // Delegate if:
    // 1. Complex web automation with multiple pages
    // 2. Dynamic UI elements requiring AI detection
    // 3. CAPTCHA solving needed
    // 4. Multi-step authentication flows
    // 5. PDF/document processing required

    const complexConditions = [
      params.source.selectors && Object.keys(params.source.selectors).length > 10,
      params.source.requiresCaptcha === true,
      params.source.requiresMFA === true,
      params.source.documentProcessing === true,
    ];

    return complexConditions.some(condition => condition === true);
  }

  /**
   * Extract data via UiPath robot
   */
  private async extractViaUiPath(
    params: ExtractionParams
  ): Promise<ExtractedData> {
    const startTime = Date.now();

    try {
      // Start UiPath job
      const jobId = await this.uiPathConnector.startJob('BankingDataExtraction', {
        sourceUrl: params.source.url,
        accountNumbers: params.source.accountIdentifiers,
        credentialsId: params.credentials.id,
        extractionType: params.extractionMethod,
      });

      this.emit('extraction:delegated', {
        jobId: params.jobId,
        provider: 'uipath',
        uiPathJobId: jobId,
      });

      // Poll for job completion
      let status = await this.uiPathConnector.getJobStatus(jobId);
      while (status.state === 'Pending' || status.state === 'Running') {
        await this.sleep(5000); // Poll every 5 seconds
        status = await this.uiPathConnector.getJobStatus(jobId);
      }

      if (status.state !== 'Successful') {
        throw new Error(`UiPath job failed: ${status.errorMessage}`);
      }

      // Extract results from output arguments
      const extractedData: ExtractedData = {
        jobId: params.jobId,
        timestamp: new Date(),
        rawData: status.outputArguments.ExtractedData,
        metadata: {
          source: 'uipath',
          extractionDuration: Date.now() - startTime,
          recordCount: status.outputArguments.RecordCount || 0,
          checksumHash: status.outputArguments.ChecksumHash || '',
          compressionUsed: false,
        },
        validationStatus: { isValid: true, errors: [], warnings: [] },
        dataSize: JSON.stringify(status.outputArguments.ExtractedData).length,
      };

      this.emit('extraction:completed', { jobId: params.jobId, data: extractedData });
      return extractedData;
    } catch (error) {
      this.emit('extraction:failed', { jobId: params.jobId, error });
      throw error;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
```

**2. Environment Configuration**

```env
# UiPath Orchestrator Configuration
UIPATH_ORCHESTRATOR_URL=https://cloud.uipath.com/{orgName}/{tenantName}
UIPATH_TENANT_NAME=Default
UIPATH_API_KEY=your-api-key-here

# Store credentials in CredentialVault with ID: 'uipath-orchestrator'
```

**3. UiPath Process Development**

Create a UiPath process in UiPath Studio:

**Process Name:** `BankingDataExtraction`

**Input Arguments:**
- `sourceUrl` (String) - Bank portal URL
- `accountNumbers` (String[]) - Account numbers to extract
- `credentialsId` (String) - Reference to credentials in your vault
- `extractionType` (String) - Type of extraction

**Output Arguments:**
- `ExtractedData` (Object) - JSON data extracted from bank portal
- `RecordCount` (Int32) - Number of transactions extracted
- `ChecksumHash` (String) - Data integrity checksum

**Workflow Steps:**
1. Retrieve credentials from your API (`GET /api/credentials/{credentialsId}`)
2. Navigate to bank portal
3. Login with credentials (handle MFA if needed)
4. Extract transaction data using selectors
5. Parse data into standardized JSON format
6. Return data via output arguments

---

## Option 2: Robocorp Integration

### **Why Robocorp?**

**Strengths:**
- ✅ **Open-source** (Python Robot Framework + proprietary cloud orchestration)
- ✅ **Developer-friendly** (Python-based, Git-native, CI/CD friendly)
- ✅ **Cloud-native** (serverless workers, Kubernetes-ready)
- ✅ **Cost-effective** (Free tier: 5,000 executions/month, paid from $99/month)
- ✅ **Modern API** (REST API, webhooks, native integrations)
- ✅ **Container-based** (Docker workers, easy scaling)
- ✅ **Good for AI/ML** (Python ecosystem for TensorFlow, scikit-learn)

**Weaknesses:**
- ❌ Smaller ecosystem than UiPath
- ❌ Less enterprise support
- ❌ Fewer pre-built connectors for banking systems

### **Integration Architecture**

#### **Pattern: Microservices with Robocorp Workers**

```typescript
// src/integrations/robocorp/RobocorpConnector.ts

import axios from 'axios';
import { CredentialVault } from '@/core/security/CredentialVault';

/**
 * Robocorp Cloud API Integration
 *
 * Manages robot execution in Robocorp Cloud
 */
export class RobocorpConnector {
  private apiUrl = 'https://api.eu1.robocorp.com';
  private workspaceId: string;
  private vault: CredentialVault;
  private apiKey?: string;

  constructor() {
    this.workspaceId = process.env.ROBOCORP_WORKSPACE_ID || '';
    this.vault = new CredentialVault();
  }

  /**
   * Initialize API key from vault
   */
  private async getApiKey(): Promise<string> {
    if (this.apiKey) return this.apiKey;

    const creds = await this.vault.retrieve('robocorp-api-key');
    this.apiKey = creds.key || '';
    return this.apiKey;
  }

  /**
   * Start a robot run
   *
   * @param processId - Robocorp process ID (from Control Room)
   * @param inputData - Input work items for the robot
   */
  async startRun(
    processId: string,
    inputData: Record<string, any>
  ): Promise<string> {
    const apiKey = await this.getApiKey();

    try {
      // Create work item with input data
      const workItemResponse = await axios.post(
        `${this.apiUrl}/workspaces/${this.workspaceId}/work-items`,
        {
          payload: inputData,
        },
        {
          headers: {
            Authorization: `RC-WSKEY ${apiKey}`,
            'Content-Type': 'application/json',
          },
        }
      );

      const workItemId = workItemResponse.data.id;

      // Trigger process run with work item
      const runResponse = await axios.post(
        `${this.apiUrl}/workspaces/${this.workspaceId}/processes/${processId}/runs`,
        {
          workItemIds: [workItemId],
        },
        {
          headers: {
            Authorization: `RC-WSKEY ${apiKey}`,
            'Content-Type': 'application/json',
          },
        }
      );

      return runResponse.data.id;
    } catch (error) {
      throw new Error(`Failed to start Robocorp run: ${error}`);
    }
  }

  /**
   * Get run status
   */
  async getRunStatus(runId: string): Promise<RobocorpRunStatus> {
    const apiKey = await this.getApiKey();

    const response = await axios.get(
      `${this.apiUrl}/workspaces/${this.workspaceId}/runs/${runId}`,
      {
        headers: {
          Authorization: `RC-WSKEY ${apiKey}`,
        },
      }
    );

    const run = response.data;

    // Get output from work items
    let outputData = {};
    if (run.state === 'COMPLETED') {
      const workItemsResponse = await axios.get(
        `${this.apiUrl}/workspaces/${this.workspaceId}/runs/${runId}/work-items`,
        {
          headers: { Authorization: `RC-WSKEY ${apiKey}` },
        }
      );

      const outputWorkItem = workItemsResponse.data.find(
        (item: any) => item.type === 'output'
      );
      outputData = outputWorkItem?.payload || {};
    }

    return {
      id: run.id,
      state: run.state, // 'PENDING', 'RUNNING', 'COMPLETED', 'FAILED'
      startedAt: new Date(run.startedAt),
      completedAt: run.completedAt ? new Date(run.completedAt) : undefined,
      outputData,
      errorMessage: run.errorMessage,
    };
  }

  /**
   * Set up webhook for real-time notifications
   * Call this during platform initialization
   */
  async setupWebhook(callbackUrl: string): Promise<void> {
    const apiKey = await this.getApiKey();

    await axios.post(
      `${this.apiUrl}/workspaces/${this.workspaceId}/webhooks`,
      {
        url: callbackUrl,
        events: ['run.completed', 'run.failed'],
      },
      {
        headers: {
          Authorization: `RC-WSKEY ${apiKey}`,
          'Content-Type': 'application/json',
        },
      }
    );
  }
}

export interface RobocorpRunStatus {
  id: string;
  state: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';
  startedAt: Date;
  completedAt?: Date;
  outputData: Record<string, any>;
  errorMessage?: string;
}
```

**Example Robocorp Robot (Python)**

```python
# banking_extractor/tasks.robot

*** Settings ***
Library    RPA.Browser.Selenium
Library    RPA.HTTP
Library    RPA.JSON
Library    Collections

*** Tasks ***
Extract Banking Transactions
    [Documentation]    Extract transactions from bank portal
    ${input}=    Get Work Item Variables

    # Retrieve credentials from your platform API
    ${credentials}=    Get Credentials    ${input}[credentialsId]

    # Open bank portal
    Open Available Browser    ${input}[sourceUrl]

    # Login
    Input Text    id:username    ${credentials}[username]
    Input Text    id:password    ${credentials}[password]
    Click Button    id:login-button

    # Handle MFA if needed
    ${mfa_code}=    Handle MFA    ${credentials}[mfaSecret]
    Input Text    id:mfa-code    ${mfa_code}
    Click Button    id:verify-button

    # Navigate to transactions page
    Click Link    Transactions

    # Extract transaction data
    ${transactions}=    Extract Transaction Table

    # Create output work item
    Create Output Work Item
    Set Work Item Variable    ExtractedData    ${transactions}
    Set Work Item Variable    RecordCount    ${transactions.__len__()}
    Save Work Item

*** Keywords ***
Get Credentials
    [Arguments]    ${credentials_id}
    ${response}=    GET    https://your-platform.com/api/credentials/${credentials_id}
    ...    headers={"Authorization": "Bearer ${API_TOKEN}"}
    ${creds}=    Set Variable    ${response.json()}[data]
    [Return]    ${creds}

Extract Transaction Table
    ${rows}=    Get WebElements    xpath://table[@id='transactions']//tr
    ${transactions}=    Create List
    FOR    ${row}    IN    @{rows}
        ${cells}=    Get WebElements    xpath:${row}/td
        ${transaction}=    Create Dictionary
        ...    date=${cells[0].text}
        ...    description=${cells[1].text}
        ...    amount=${cells[2].text}
        ...    status=${cells[3].text}
        Append To List    ${transactions}    ${transaction}
    END
    [Return]    ${transactions}
```

**Webhook Handler for Real-Time Updates**

```typescript
// src/app/api/webhooks/robocorp/route.ts

import { NextRequest, NextResponse } from 'next/server';
import { RPAEngine } from '@/core/engine/RPAEngine';

export async function POST(request: NextRequest): Promise<NextResponse> {
  const webhook = await request.json();

  // Webhook payload from Robocorp
  const { event, runId, workspaceId, state, errorMessage } = webhook;

  // Map Robocorp run to your internal job
  const jobId = await getJobIdFromRunId(runId);

  if (event === 'run.completed' && state === 'COMPLETED') {
    // Fetch output data and process
    const connector = new RobocorpConnector();
    const status = await connector.getRunStatus(runId);

    // Update job status in your system
    // Trigger ETL pipeline with extracted data
  } else if (event === 'run.failed') {
    // Handle failure
    console.error(`Robocorp run ${runId} failed: ${errorMessage}`);
  }

  return NextResponse.json({ received: true });
}
```

---

## Comparison Matrix

| Feature | Your Platform (Current) | UiPath Integration | Robocorp Integration |
|---------|-------------------------|-------------------|---------------------|
| **Cost** | Infrastructure only | $420+/bot/month | Free tier / $99+/month |
| **Deployment** | Self-hosted Node.js | Hybrid (cloud/on-prem) | Cloud-native |
| **Web Automation** | Puppeteer (basic) | Advanced (AI selectors) | Medium (Selenium) |
| **API Integration** | Strong (custom) | Medium (pre-built) | Strong (Python) |
| **Document Processing** | Manual | AI-powered (strong) | Medium (libraries) |
| **Banking Connectors** | Custom-built | Pre-built (SWIFT, SAP) | Custom-built |
| **Compliance** | DIY audit logs | PCI-DSS certified | DIY audit logs |
| **Scalability** | Horizontal (workers) | Orchestrator limits | Kubernetes-native |
| **Development** | TypeScript | Visual (Studio) | Python (code-first) |
| **AI/ML** | TensorFlow.js | Document Understanding | Python ecosystem |

---

## Recommended Architecture: Hybrid Approach

**Best of all worlds:**

```
┌──────────────────────────────────────────────────────────────┐
│              Your Platform (Central Orchestrator)             │
│                                                               │
│  • Job Scheduling (cron-based)                               │
│  • Credential Management (CredentialVault)                    │
│  • Analytics & Reporting (BigQuery + Power BI)               │
│  • Audit Logging (compliance)                                │
│  • Queue Management (BullMQ)                                 │
└────────────┬──────────────┬──────────────┬──────────────────┘
             │              │              │
     ┌───────┴────┐  ┌──────┴─────┐  ┌────┴──────┐
     │  Internal  │  │  UiPath    │  │ Robocorp  │
     │ Automation │  │  Robots    │  │  Workers  │
     └─────┬──────┘  └──────┬─────┘  └────┬──────┘
           │                │              │
           └────────────────┴──────────────┘
                           │
                 ┌─────────┴─────────┐
                 │  Banking Networks  │
                 │  (ACH, Banks, etc) │
                 └───────────────────┘

**Routing Logic:**

┌─────────────────────┬──────────────────────┬───────────────────┐
│     Job Type        │    Route To          │     Reason        │
├─────────────────────┼──────────────────────┼───────────────────┤
│ Simple API calls    │ Internal (APIExtr)   │ Fast, cheap       │
│ REST/SOAP/FIX       │ Internal             │ Native support    │
│ File downloads      │ Internal             │ Node.js native    │
│ Basic web scraping  │ Internal (Puppeteer) │ Cost-effective    │
│ Complex web (CAPTCHA)│ UiPath              │ AI capabilities   │
│ Document processing │ UiPath              │ Doc Understanding │
│ Multi-page workflows│ UiPath              │ Enterprise-grade  │
│ Python-heavy tasks  │ Robocorp            │ Python ecosystem  │
│ Prototype/Testing   │ Robocorp            │ Free tier         │
└─────────────────────┴──────────────────────┴───────────────────┘
```

**Cost Optimization:**
- **80% of jobs** → Internal automation (API, simple web) → **$0 bot cost**
- **15% of jobs** → Robocorp (medium complexity) → **~$200/month**
- **5% of jobs** → UiPath (complex, compliance-critical) → **~$500/month**

**Total RPA software cost:** ~$700/month (vs $10,000+/month for UiPath-only)

---

## Implementation Roadmap

### **Phase 1: Internal Automation (Current)**
- ✅ API extraction (REST, SOAP, FIX, ISO20022)
- ✅ Basic web automation (Puppeteer)
- ✅ Credential management
- ✅ Job scheduling
- ✅ BigQuery analytics

### **Phase 2: Robocorp Integration** (Recommended first)
**Week 1-2:**
1. Sign up for Robocorp Cloud (free tier)
2. Create RobocorpConnector integration
3. Build first robot (simple transaction extraction)
4. Test webhook integration
5. Deploy to production

**Cost:** $0 (free tier covers 5,000 runs/month)

### **Phase 3: UiPath Integration** (If needed)
**Week 3-6:**
1. Evaluate UiPath trial license
2. Deploy UiPath Orchestrator (cloud or on-prem)
3. Build UiPathConnector integration
4. Migrate complex workflows from Robocorp
5. Train team on UiPath Studio

**Cost:** ~$5,000 setup + $420/month/bot

---

## Conclusion

**My Recommendation:**

1. **Start with Robocorp** for:
   - Cost-effectiveness (free tier)
   - Modern architecture (fits your Node.js stack)
   - Quick wins (Python-friendly for banking APIs)

2. **Add UiPath later** if you need:
   - Enterprise banking connectors (SWIFT, SAP)
   - AI document processing
   - Compliance certifications (PCI-DSS)

3. **Keep your platform** as the:
   - Central orchestrator
   - Credential vault
   - Analytics layer (BigQuery + Power BI)
   - Audit system

This hybrid approach gives you **flexibility, cost control, and best-in-class capabilities** for each use case.

---

## Next Steps

Would you like me to:
1. ✅ Implement the RobocorpConnector integration?
2. ✅ Create sample Robocorp robots for banking extraction?
3. ✅ Set up the webhook handler for real-time updates?
4. ✅ Build the routing logic in RPAEngine?

Let me know which integration path you'd like to pursue!
