/**
 * Banking Network Sources Configuration
 * Defines all supported banking network data sources for the RPA platform
 *
 * NOTE: The following networks are reserved for future technical development:
 * - ACH_NACHA (ach-nacha) - Automated Clearing House (NACHA)
 * - SWIFT (swift) - Society for Worldwide Interbank Financial Telecommunication
 * - FedWire (fedwire) - Federal Reserve Wire Network
 * - CHIPS (chips) - Clearing House Interbank Payments System
 *
 * These clearinghouse networks will be activated upon completion of technical integration.
 */

export interface BankingNetworkSource {
  id: string;
  name: string;
  type: 'clearinghouse' | 'payment-processor' | 'shared-infrastructure' | 'direct-bank';
  category: 'ACH' | 'SWIFT' | 'FedWire' | 'CHIPS' | 'Card Networks' | 'Payment Gateway' | 'Banking Platform' | 'Other';
  authMethods: ('oauth' | 'username-password-mfa' | 'api-key' | 'certificate')[];
  protocols: ('REST' | 'SOAP' | 'FIX' | 'ISO20022' | 'web-automation')[];
  url?: string;
  apiEndpoint?: string;
  documentationUrl?: string;
  capabilities: {
    realTime: boolean;
    batch: boolean;
    webhooks: boolean;
  };
}

export interface AuthMethodDetails {
  name: string;
  flows?: string[];
  mfaTypes?: string[];
  headerTypes?: string[];
  certTypes?: string[];
  securityLevel: 'low' | 'medium' | 'high' | 'very-high';
  mfaSupport: boolean;
}

/**
 * Clearing Houses - Payment clearing and settlement networks
 */
export const CLEARING_HOUSES: BankingNetworkSource[] = [
  {
    id: 'ach-nacha',
    name: 'NACHA (ACH Network)',
    type: 'clearinghouse',
    category: 'ACH',
    authMethods: ['oauth', 'certificate'],
    protocols: ['REST', 'SOAP'],
    url: 'https://www.nacha.org',
    apiEndpoint: 'https://api.nacha.org/v1',
    documentationUrl: 'https://www.nacha.org/content/api-documentation',
    capabilities: {
      realTime: false,
      batch: true,
      webhooks: true,
    },
  },
  {
    id: 'swift',
    name: 'SWIFT Network',
    type: 'clearinghouse',
    category: 'SWIFT',
    authMethods: ['certificate'],
    protocols: ['ISO20022', 'FIX'],
    url: 'https://www.swift.com',
    apiEndpoint: 'https://api.swift.com/v1',
    documentationUrl: 'https://developer.swift.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: false,
    },
  },
  {
    id: 'fedwire',
    name: 'Fedwire Funds Service',
    type: 'clearinghouse',
    category: 'FedWire',
    authMethods: ['certificate'],
    protocols: ['ISO20022'],
    url: 'https://www.frbservices.org/financial-services/fedwire',
    documentationUrl: 'https://www.frbservices.org/resources/financial-services/wires',
    capabilities: {
      realTime: true,
      batch: false,
      webhooks: false,
    },
  },
  {
    id: 'chips',
    name: 'CHIPS (Clearing House Interbank Payments)',
    type: 'clearinghouse',
    category: 'CHIPS',
    authMethods: ['certificate'],
    protocols: ['ISO20022'],
    url: 'https://www.theclearinghouse.org/payment-systems/chips',
    documentationUrl: 'https://www.theclearinghouse.org/payment-systems/chips/technical-specifications',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: false,
    },
  },
];

/**
 * Payment Processors - Card networks and payment gateways
 * NOTE: Visa & Mastercard are hidden (infrastructure layer, not merchant-facing)
 * INCLUDED: PayPal, Stripe, Square (merchant-facing payment processors)
 */
export const PAYMENT_PROCESSORS: BankingNetworkSource[] = [
  // HIDDEN: Visa & Mastercard (Global card payment networks) - Not applicable for construction company
  // {
  //   id: 'visa-visanet',
  //   name: 'Visa (VisaNet)',
  //   type: 'payment-processor',
  //   category: 'Card Networks',
  //   authMethods: ['api-key', 'oauth'],
  //   protocols: ['REST', 'SOAP'],
  //   url: 'https://www.visa.com',
  //   apiEndpoint: 'https://api.visa.com',
  //   documentationUrl: 'https://developer.visa.com',
  //   capabilities: {
  //     realTime: true,
  //     batch: true,
  //     webhooks: true,
  //   },
  // },
  // {
  //   id: 'mastercard-network',
  //   name: 'Mastercard Network',
  //   type: 'payment-processor',
  //   category: 'Card Networks',
  //   authMethods: ['api-key', 'oauth'],
  //   protocols: ['REST'],
  //   url: 'https://www.mastercard.com',
  //   apiEndpoint: 'https://api.mastercard.com',
  //   documentationUrl: 'https://developer.mastercard.com',
  //   capabilities: {
  //     realTime: true,
  //     batch: true,
  //     webhooks: true,
  //   },
  // },
  {
    id: 'paypal',
    name: 'PayPal',
    type: 'payment-processor',
    category: 'Payment Gateway',
    authMethods: ['oauth', 'api-key'],
    protocols: ['REST'],
    url: 'https://www.paypal.com',
    apiEndpoint: 'https://api.paypal.com',
    documentationUrl: 'https://developer.paypal.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
  {
    id: 'stripe',
    name: 'Stripe',
    type: 'payment-processor',
    category: 'Payment Gateway',
    authMethods: ['api-key'],
    protocols: ['REST'],
    url: 'https://stripe.com',
    apiEndpoint: 'https://api.stripe.com',
    documentationUrl: 'https://stripe.com/docs/api',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
  {
    id: 'square',
    name: 'Square',
    type: 'payment-processor',
    category: 'Payment Gateway',
    authMethods: ['oauth', 'api-key'],
    protocols: ['REST'],
    url: 'https://squareup.com',
    apiEndpoint: 'https://connect.squareup.com',
    documentationUrl: 'https://developer.squareup.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
];

/**
 * Shared Banking Infrastructure - Multi-institution service platforms
 */
export const SHARED_INFRASTRUCTURE: BankingNetworkSource[] = [
  {
    id: 'fis-global',
    name: 'FIS Global',
    type: 'shared-infrastructure',
    category: 'Banking Platform',
    authMethods: ['oauth', 'username-password-mfa'],
    protocols: ['REST', 'SOAP'],
    url: 'https://www.fisglobal.com',
    apiEndpoint: 'https://api.fisglobal.com',
    documentationUrl: 'https://developer.fisglobal.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
  {
    id: 'fiserv',
    name: 'Fiserv',
    type: 'shared-infrastructure',
    category: 'Banking Platform',
    authMethods: ['oauth', 'api-key'],
    protocols: ['REST', 'SOAP'],
    url: 'https://www.fiserv.com',
    apiEndpoint: 'https://api.fiserv.com',
    documentationUrl: 'https://developer.fiserv.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
  {
    id: 'jack-henry',
    name: 'Jack Henry & Associates',
    type: 'shared-infrastructure',
    category: 'Banking Platform',
    authMethods: ['oauth', 'api-key'],
    protocols: ['REST'],
    url: 'https://www.jackhenry.com',
    apiEndpoint: 'https://api.jackhenry.com',
    documentationUrl: 'https://developer.jackhenry.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
  {
    id: 'temenos',
    name: 'Temenos',
    type: 'shared-infrastructure',
    category: 'Banking Platform',
    authMethods: ['oauth', 'api-key'],
    protocols: ['REST', 'SOAP'],
    url: 'https://www.temenos.com',
    apiEndpoint: 'https://api.temenos.com',
    documentationUrl: 'https://developer.temenos.com',
    capabilities: {
      realTime: true,
      batch: true,
      webhooks: true,
    },
  },
];

/**
 * Direct Bank Websites - Web automation templates for bank portals
 * Includes email-authenticated file downloads and manual document scanning
 */
export const DIRECT_BANKS: BankingNetworkSource[] = [
  {
    id: 'bank-template',
    name: 'Generic Bank Portal',
    type: 'direct-bank',
    category: 'Other',
    authMethods: ['username-password-mfa'],
    protocols: ['web-automation'],
    capabilities: {
      realTime: false,
      batch: true,
      webhooks: false,
    },
  },
  {
    id: 'email-authenticated-download',
    name: 'Email/SMS Authenticated Bank File Platform Download',
    type: 'direct-bank',
    category: 'Other',
    authMethods: ['username-password-mfa'],
    protocols: ['web-automation'],
    url: 'https://example-bank.com/file-download',
    documentationUrl: 'https://example-bank.com/help/email-sms-authentication',
    capabilities: {
      realTime: false,
      batch: true,
      webhooks: false,
    },
  },
  {
    id: 'manual-hardcopy-scan',
    name: 'Manual Scan Bank Hardcopy Statements',
    type: 'direct-bank',
    category: 'Other',
    authMethods: ['username-password-mfa'],
    protocols: ['web-automation'],
    documentationUrl: 'https://example-bank.com/help/statement-download',
    capabilities: {
      realTime: false,
      batch: true,
      webhooks: false,
    },
  },
];

/**
 * Authentication Method Details
 */
export const AUTH_METHOD_DETAILS: Record<string, AuthMethodDetails> = {
  oauth: {
    name: 'OAuth 2.0',
    flows: ['authorization_code', 'client_credentials', 'implicit', 'password'],
    securityLevel: 'high',
    mfaSupport: true,
  },
  'username-password-mfa': {
    name: 'Username/Password + MFA',
    mfaTypes: ['totp', 'sms', 'email', 'hardware-token', 'push-notification'],
    securityLevel: 'medium',
    mfaSupport: true,
  },
  'api-key': {
    name: 'API Key',
    headerTypes: ['X-API-Key', 'Authorization: Bearer', 'X-Auth-Token'],
    securityLevel: 'medium',
    mfaSupport: false,
  },
  certificate: {
    name: 'Certificate-Based',
    certTypes: ['x509', 'mutual-tls', 'client-certificate'],
    securityLevel: 'very-high',
    mfaSupport: false,
  },
};

/**
 * All Banking Networks Combined
 */
export const BANKING_NETWORKS = {
  clearinghouses: CLEARING_HOUSES,
  paymentProcessors: PAYMENT_PROCESSORS,
  sharedInfrastructure: SHARED_INFRASTRUCTURE,
  directBanks: DIRECT_BANKS,
};

/**
 * Helper Functions
 */

// Networks reserved for future technical development
const FUTURE_NETWORKS = ['ach-nacha', 'swift', 'fedwire', 'chips'];

export function getAllBankingNetworks(): BankingNetworkSource[] {
  const allNetworks = [
    ...CLEARING_HOUSES,
    ...PAYMENT_PROCESSORS,
    ...SHARED_INFRASTRUCTURE,
    ...DIRECT_BANKS,
  ];

  // Filter out future networks that are not yet technically integrated
  return allNetworks.filter(network => !FUTURE_NETWORKS.includes(network.id));
}

export function getBankingNetworkById(id: string): BankingNetworkSource | undefined {
  return getAllBankingNetworks().find((network) => network.id === id);
}

export function getBankingNetworksByType(
  type: 'clearinghouse' | 'payment-processor' | 'shared-infrastructure' | 'direct-bank'
): BankingNetworkSource[] {
  return getAllBankingNetworks().filter((network) => network.type === type);
}

export function getBankingNetworksByAuthMethod(
  authMethod: 'oauth' | 'username-password-mfa' | 'api-key' | 'certificate'
): BankingNetworkSource[] {
  return getAllBankingNetworks().filter((network) =>
    network.authMethods.includes(authMethod)
  );
}

export function getBankingNetworksByProtocol(
  protocol: 'REST' | 'SOAP' | 'FIX' | 'ISO20022' | 'web-automation'
): BankingNetworkSource[] {
  return getAllBankingNetworks().filter((network) =>
    network.protocols.includes(protocol)
  );
}
