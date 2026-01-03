import { NextRequest, NextResponse } from 'next/server';
import { CredentialVault } from '@/core/security/CredentialVault';
import type {
  APIResponse,
  CredentialCreateRequest,
  CredentialResponse,
  CredentialListResponse,
} from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize Credential Vault (singleton pattern)
let credentialVault: CredentialVault;
function getCredentialVault(): CredentialVault {
  if (!credentialVault) {
    credentialVault = new CredentialVault();
  }
  return credentialVault;
}

/**
 * GET /api/credentials
 * List all credentials (metadata only, no sensitive data)
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const vault = getCredentialVault();
    const allCredentials = await vault.list();

    // Map to response format with calculated fields
    const credentials: CredentialResponse[] = allCredentials.map((cred) => {
      const daysUntilExpiration = cred.expiresAt
        ? Math.ceil((new Date(cred.expiresAt).getTime() - Date.now()) / (1000 * 60 * 60 * 24))
        : undefined;

      return {
        id: cred.id,
        type: cred.type as any,
        metadata: {
          description: cred.description,
          tags: cred.tags,
        },
        createdAt: cred.createdAt,
        expiresAt: cred.expiresAt,
        rotationPolicy: cred.rotationPolicy,
        lastRotated: cred.lastRotated,
        daysUntilExpiration,
        needsRotation: daysUntilExpiration !== undefined && daysUntilExpiration <= 7,
      };
    });

    const response: APIResponse<CredentialListResponse> = {
      success: true,
      data: {
        credentials,
        total: credentials.length,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error('[API] GET /api/credentials error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'CREDENTIAL_LIST_ERROR',
        message: error.message || 'Failed to retrieve credentials',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * POST /api/credentials
 * Create and store a new credential
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const body: CredentialCreateRequest = await request.json();

    // Validate required fields
    if (!body.type || !body.data) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'VALIDATION_ERROR',
            message: 'Missing required fields: type, data',
          },
          timestamp: new Date(),
        },
        { status: 400 }
      );
    }

    const vault = getCredentialVault();

    // Generate credential ID
    const credentialId = `cred-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;

    // Merge type with data to match CredentialData interface
    const credentialData = {
      type: body.type,
      ...body.data,
    };

    // Store credential
    await vault.store(credentialId, credentialData as any, {
      expiresAt: body.expiresAt,
      rotationPolicy: body.rotationPolicy || 'manual',
      description: body.metadata?.description || '',
      tags: body.metadata?.tags || [],
    });

    const response: APIResponse<{ credentialId: string }> = {
      success: true,
      data: {
        credentialId,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 201 });
  } catch (error: any) {
    console.error('[API] POST /api/credentials error:', error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'CREDENTIAL_CREATE_ERROR',
        message: error.message || 'Failed to create credential',
        details: error,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
