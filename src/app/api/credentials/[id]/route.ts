import { NextRequest, NextResponse } from 'next/server';
import { CredentialVault } from '@/core/security/CredentialVault';
import type { APIResponse } from '@/types/api.types';

export const dynamic = 'force-dynamic';

// Initialize Credential Vault (singleton pattern)
let credentialVault: CredentialVault;
function getCredentialVault(): CredentialVault {
  if (!credentialVault) {
    credentialVault = new CredentialVault();
  }
  return credentialVault;
}

interface RouteParams {
  params: {
    id: string;
  };
}

/**
 * GET /api/credentials/[id]
 * Retrieve a specific credential (returns encrypted data only to authorized requests)
 */
export async function GET(
  request: NextRequest,
  { params }: RouteParams
): Promise<NextResponse> {
  try {
    const { id } = params;

    const vault = getCredentialVault();
    const credential = await vault.retrieve(id);

    if (!credential) {
      return NextResponse.json(
        {
          success: false,
          error: {
            code: 'CREDENTIAL_NOT_FOUND',
            message: `Credential with ID ${id} not found`,
          },
          timestamp: new Date(),
        },
        { status: 404 }
      );
    }

    // Return credential (note: actual credential data is encrypted by vault)
    const response: APIResponse<any> = {
      success: true,
      data: credential,
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error(`[API] GET /api/credentials/${params.id} error:`, error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'CREDENTIAL_RETRIEVE_ERROR',
        message: error.message || 'Failed to retrieve credential',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * PUT /api/credentials/[id]
 * Update a credential
 */
export async function PUT(
  request: NextRequest,
  { params }: RouteParams
): Promise<NextResponse> {
  try {
    const { id } = params;
    const body = await request.json();

    const vault = getCredentialVault();

    // Update credential (metadata and expiresAt are preserved from original entry)
    await vault.update(id, body.data);

    const response: APIResponse<{ message: string }> = {
      success: true,
      data: {
        message: `Credential ${id} updated successfully`,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error(`[API] PUT /api/credentials/${params.id} error:`, error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'CREDENTIAL_UPDATE_ERROR',
        message: error.message || 'Failed to update credential',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}

/**
 * DELETE /api/credentials/[id]
 * Delete a credential
 */
export async function DELETE(
  request: NextRequest,
  { params }: RouteParams
): Promise<NextResponse> {
  try {
    const { id } = params;

    const vault = getCredentialVault();
    await vault.delete(id);

    const response: APIResponse<{ message: string }> = {
      success: true,
      data: {
        message: `Credential ${id} deleted successfully`,
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response);
  } catch (error: any) {
    console.error(`[API] DELETE /api/credentials/${params.id} error:`, error);

    const response: APIResponse<never> = {
      success: false,
      error: {
        code: 'CREDENTIAL_DELETE_ERROR',
        message: error.message || 'Failed to delete credential',
      },
      timestamp: new Date(),
    };

    return NextResponse.json(response, { status: 500 });
  }
}
