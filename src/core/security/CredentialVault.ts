import { Encryption } from './Encryption';
import { AuditLogger } from './AuditLogger';

/**
 * Secure Credential Vault for Banking Authentication
 * Implements encrypted storage for sensitive credentials with rotation policies
 */
export class CredentialVault {
  private encryption: Encryption;
  private auditLogger: AuditLogger;
  private storage: Map<string, EncryptedCredential>;

  constructor() {
    this.encryption = new Encryption();
    this.auditLogger = new AuditLogger();
    this.storage = new Map();
  }

  /**
   * Store credentials securely in vault
   */
  async store(
    id: string,
    credentials: CredentialData,
    options?: StorageOptions
  ): Promise<void> {
    try {
      // Encrypt credential data
      const encrypted = await this.encryption.encrypt(JSON.stringify(credentials));

      // Create vault entry
      const vaultEntry: EncryptedCredential = {
        id,
        type: credentials.type,
        encryptedData: encrypted,
        createdAt: new Date(),
        expiresAt: options?.expiresAt,
        rotationPolicy: options?.rotationPolicy || 'manual',
        lastRotated: new Date(),
        metadata: {
          description: options?.description || '',
          tags: options?.tags || [],
        },
      };

      // Store in vault
      this.storage.set(id, vaultEntry);

      // Audit log
      await this.auditLogger.log({
        action: 'credential.stored',
        resource: 'credential-vault',
        resourceId: id,
        changes: {
          type: credentials.type,
          rotationPolicy: vaultEntry.rotationPolicy,
        },
      });
    } catch (error) {
      await this.auditLogger.log({
        action: 'credential.store.failed',
        resource: 'credential-vault',
        resourceId: id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  /**
   * Retrieve and decrypt credentials from vault
   */
  async retrieve(id: string): Promise<CredentialData> {
    try {
      const vaultEntry = this.storage.get(id);

      if (!vaultEntry) {
        throw new Error(`Credential ${id} not found in vault`);
      }

      // Check expiration
      if (vaultEntry.expiresAt && vaultEntry.expiresAt < new Date()) {
        throw new Error(`Credential ${id} has expired`);
      }

      // Decrypt credential data
      const decrypted = await this.encryption.decrypt(vaultEntry.encryptedData);
      const credentials = JSON.parse(decrypted);

      // Audit log
      await this.auditLogger.log({
        action: 'credential.retrieved',
        resource: 'credential-vault',
        resourceId: id,
      });

      // Check if rotation is needed
      await this.checkRotationPolicy(id, vaultEntry);

      return credentials;
    } catch (error) {
      await this.auditLogger.log({
        action: 'credential.retrieve.failed',
        resource: 'credential-vault',
        resourceId: id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  /**
   * Update existing credentials
   */
  async update(id: string, credentials: CredentialData): Promise<void> {
    try {
      const existing = this.storage.get(id);

      if (!existing) {
        throw new Error(`Credential ${id} not found`);
      }

      // Encrypt new data
      const encrypted = await this.encryption.encrypt(JSON.stringify(credentials));

      // Update entry
      const updated: EncryptedCredential = {
        ...existing,
        encryptedData: encrypted,
        lastRotated: new Date(),
      };

      this.storage.set(id, updated);

      await this.auditLogger.log({
        action: 'credential.updated',
        resource: 'credential-vault',
        resourceId: id,
      });
    } catch (error) {
      await this.auditLogger.log({
        action: 'credential.update.failed',
        resource: 'credential-vault',
        resourceId: id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  /**
   * Rotate credentials based on policy
   */
  async rotate(id: string, newCredentials: CredentialData): Promise<void> {
    await this.update(id, newCredentials);

    await this.auditLogger.log({
      action: 'credential.rotated',
      resource: 'credential-vault',
      resourceId: id,
    });
  }

  /**
   * Delete credentials from vault
   */
  async delete(id: string): Promise<void> {
    try {
      const deleted = this.storage.delete(id);

      if (!deleted) {
        throw new Error(`Credential ${id} not found`);
      }

      await this.auditLogger.log({
        action: 'credential.deleted',
        resource: 'credential-vault',
        resourceId: id,
      });
    } catch (error) {
      await this.auditLogger.log({
        action: 'credential.delete.failed',
        resource: 'credential-vault',
        resourceId: id,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }

  /**
   * List all credential IDs (not the credentials themselves)
   */
  async list(): Promise<CredentialMetadata[]> {
    const metadata: CredentialMetadata[] = [];

    for (const [id, entry] of this.storage) {
      metadata.push({
        id,
        type: entry.type,
        createdAt: entry.createdAt,
        expiresAt: entry.expiresAt,
        lastRotated: entry.lastRotated,
        rotationPolicy: entry.rotationPolicy,
        description: entry.metadata.description,
        tags: entry.metadata.tags,
      });
    }

    return metadata;
  }

  /**
   * Check if credential rotation is needed
   */
  private async checkRotationPolicy(
    id: string,
    entry: EncryptedCredential
  ): Promise<void> {
    const now = new Date();
    const daysSinceRotation = Math.floor(
      (now.getTime() - entry.lastRotated.getTime()) / (1000 * 60 * 60 * 24)
    );

    let shouldRotate = false;

    switch (entry.rotationPolicy) {
      case 'daily':
        shouldRotate = daysSinceRotation >= 1;
        break;
      case 'weekly':
        shouldRotate = daysSinceRotation >= 7;
        break;
      case 'monthly':
        shouldRotate = daysSinceRotation >= 30;
        break;
      case 'quarterly':
        shouldRotate = daysSinceRotation >= 90;
        break;
      default:
        shouldRotate = false;
    }

    if (shouldRotate) {
      await this.auditLogger.log({
        action: 'credential.rotation.needed',
        resource: 'credential-vault',
        resourceId: id,
        changes: {
          policy: entry.rotationPolicy,
          daysSinceRotation,
        },
      });
    }
  }

  /**
   * Get credential expiration status
   */
  async getExpirationStatus(id: string): Promise<ExpirationStatus> {
    const entry = this.storage.get(id);

    if (!entry) {
      throw new Error(`Credential ${id} not found`);
    }

    if (!entry.expiresAt) {
      return { isExpired: false, daysUntilExpiration: null };
    }

    const now = new Date();
    const isExpired = entry.expiresAt < now;
    const daysUntilExpiration = Math.floor(
      (entry.expiresAt.getTime() - now.getTime()) / (1000 * 60 * 60 * 24)
    );

    return { isExpired, daysUntilExpiration };
  }
}

// Types
export interface CredentialData {
  type: 'oauth' | 'basic' | 'certificate' | 'api-key';
  username?: string;
  password?: string;
  token?: string;
  key?: string;
  certificate?: string;
  privateKey?: string;
  headerName?: string;
}

interface EncryptedCredential {
  id: string;
  type: string;
  encryptedData: string;
  createdAt: Date;
  expiresAt?: Date;
  rotationPolicy: RotationPolicy;
  lastRotated: Date;
  metadata: {
    description: string;
    tags: string[];
  };
}

interface StorageOptions {
  expiresAt?: Date;
  rotationPolicy?: RotationPolicy;
  description?: string;
  tags?: string[];
}

type RotationPolicy = 'manual' | 'daily' | 'weekly' | 'monthly' | 'quarterly';

interface CredentialMetadata {
  id: string;
  type: string;
  createdAt: Date;
  expiresAt?: Date;
  lastRotated: Date;
  rotationPolicy: RotationPolicy;
  description: string;
  tags: string[];
}

interface ExpirationStatus {
  isExpired: boolean;
  daysUntilExpiration: number | null;
}
