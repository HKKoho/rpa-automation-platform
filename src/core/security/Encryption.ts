import crypto from 'crypto';

/**
 * Enterprise-Grade Encryption Service
 * Implements AES-256-GCM encryption for banking-grade security
 */
export class Encryption {
  private algorithm: string = 'aes-256-gcm';
  private keyDerivation: string = 'pbkdf2';
  private iterations: number = 100000;
  private saltLength: number = 32;
  private ivLength: number = 16;
  private tagLength: number = 16;
  private masterKey: Buffer;

  constructor() {
    // Get master key from environment or generate
    const masterKeyHex = process.env.ENCRYPTION_MASTER_KEY;

    if (!masterKeyHex) {
      console.warn(
        'WARNING: ENCRYPTION_MASTER_KEY not set. Using temporary key. DO NOT USE IN PRODUCTION!'
      );
      this.masterKey = crypto.randomBytes(32);
    } else {
      this.masterKey = Buffer.from(masterKeyHex, 'hex');
    }
  }

  /**
   * Encrypt data using AES-256-GCM
   */
  async encrypt(plaintext: string): Promise<string> {
    try {
      // Generate random salt
      const salt = crypto.randomBytes(this.saltLength);

      // Derive encryption key from master key
      const key = crypto.pbkdf2Sync(
        this.masterKey,
        salt,
        this.iterations,
        32,
        'sha256'
      );

      // Generate random IV
      const iv = crypto.randomBytes(this.ivLength);

      // Create cipher
      const cipher = crypto.createCipheriv(this.algorithm, key, iv) as crypto.CipherGCM;

      // Encrypt data
      const encrypted = Buffer.concat([
        cipher.update(plaintext, 'utf8'),
        cipher.final(),
      ]);

      // Get authentication tag
      const tag = cipher.getAuthTag();

      // Combine: salt + iv + tag + encrypted data
      const combined = Buffer.concat([salt, iv, tag, encrypted]);

      // Return base64-encoded result
      return combined.toString('base64');
    } catch (error) {
      throw new Error(
        `Encryption failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Decrypt data encrypted with AES-256-GCM
   */
  async decrypt(ciphertext: string): Promise<string> {
    try {
      // Decode base64
      const combined = Buffer.from(ciphertext, 'base64');

      // Extract components
      const salt = combined.slice(0, this.saltLength);
      const iv = combined.slice(this.saltLength, this.saltLength + this.ivLength);
      const tag = combined.slice(
        this.saltLength + this.ivLength,
        this.saltLength + this.ivLength + this.tagLength
      );
      const encrypted = combined.slice(
        this.saltLength + this.ivLength + this.tagLength
      );

      // Derive decryption key
      const key = crypto.pbkdf2Sync(
        this.masterKey,
        salt,
        this.iterations,
        32,
        'sha256'
      );

      // Create decipher
      const decipher = crypto.createDecipheriv(this.algorithm, key, iv) as crypto.DecipherGCM;
      decipher.setAuthTag(tag);

      // Decrypt data
      const decrypted = Buffer.concat([
        decipher.update(encrypted),
        decipher.final(),
      ]);

      return decrypted.toString('utf8');
    } catch (error) {
      throw new Error(
        `Decryption failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Hash data using SHA-256
   */
  hash(data: string): string {
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  /**
   * Generate HMAC for message authentication
   */
  hmac(data: string, key?: string): string {
    const hmacKey = key ? Buffer.from(key, 'utf8') : this.masterKey;
    return crypto.createHmac('sha256', hmacKey).update(data).digest('hex');
  }

  /**
   * Generate cryptographically secure random token
   */
  generateToken(length: number = 32): string {
    return crypto.randomBytes(length).toString('hex');
  }

  /**
   * Generate random encryption key
   */
  generateKey(): string {
    return crypto.randomBytes(32).toString('hex');
  }

  /**
   * Encrypt data with additional authenticated data (AAD)
   */
  async encryptWithAAD(
    plaintext: string,
    additionalData: string
  ): Promise<string> {
    try {
      const salt = crypto.randomBytes(this.saltLength);
      const key = crypto.pbkdf2Sync(
        this.masterKey,
        salt,
        this.iterations,
        32,
        'sha256'
      );
      const iv = crypto.randomBytes(this.ivLength);

      const cipher = crypto.createCipheriv(this.algorithm, key, iv) as crypto.CipherGCM;

      // Set additional authenticated data
      cipher.setAAD(Buffer.from(additionalData, 'utf8'));

      const encrypted = Buffer.concat([
        cipher.update(plaintext, 'utf8'),
        cipher.final(),
      ]);

      const tag = cipher.getAuthTag();

      // Store AAD length for decryption
      const aadLength = Buffer.alloc(4);
      aadLength.writeUInt32BE(Buffer.from(additionalData, 'utf8').length, 0);

      const combined = Buffer.concat([
        salt,
        iv,
        tag,
        aadLength,
        Buffer.from(additionalData, 'utf8'),
        encrypted,
      ]);

      return combined.toString('base64');
    } catch (error) {
      throw new Error(
        `Encryption with AAD failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Decrypt data with additional authenticated data
   */
  async decryptWithAAD(ciphertext: string): Promise<{ data: string; aad: string }> {
    try {
      const combined = Buffer.from(ciphertext, 'base64');

      const salt = combined.slice(0, this.saltLength);
      const iv = combined.slice(this.saltLength, this.saltLength + this.ivLength);
      const tag = combined.slice(
        this.saltLength + this.ivLength,
        this.saltLength + this.ivLength + this.tagLength
      );

      const aadLengthOffset = this.saltLength + this.ivLength + this.tagLength;
      const aadLength = combined.readUInt32BE(aadLengthOffset);

      const aad = combined.slice(
        aadLengthOffset + 4,
        aadLengthOffset + 4 + aadLength
      );

      const encrypted = combined.slice(aadLengthOffset + 4 + aadLength);

      const key = crypto.pbkdf2Sync(
        this.masterKey,
        salt,
        this.iterations,
        32,
        'sha256'
      );

      const decipher = crypto.createDecipheriv(this.algorithm, key, iv) as crypto.DecipherGCM;
      decipher.setAuthTag(tag);
      decipher.setAAD(aad);

      const decrypted = Buffer.concat([
        decipher.update(encrypted),
        decipher.final(),
      ]);

      return {
        data: decrypted.toString('utf8'),
        aad: aad.toString('utf8'),
      };
    } catch (error) {
      throw new Error(
        `Decryption with AAD failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  /**
   * Verify data integrity using HMAC
   */
  verify(data: string, signature: string, key?: string): boolean {
    const expectedSignature = this.hmac(data, key);
    return crypto.timingSafeEqual(
      Buffer.from(signature),
      Buffer.from(expectedSignature)
    );
  }
}
