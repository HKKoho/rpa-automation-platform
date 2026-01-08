/**
 * Pricing & Budget Options Page
 * All pricing tiers in USD
 */

export const metadata = {
  title: 'Pricing & Budget Options | RPA Platform',
  description: 'Complete pricing guide with 3 deployment tiers (all prices in USD)',
};

export default function PricingPage() {
  const tiers = [
    {
      name: 'Starter',
      price: '$105',
      setup: '$12',
      best: 'Small banks, credit unions',
      networks: '1-3',
      txPerDay: '10K',
      users: '5',
      support: 'Community',
      features: [
        'Internal automation only',
        'AI: TensorFlow.js vision only (local)',
        'PostgreSQL analytics',
        'Basic dashboards',
        '99% uptime SLA',
        '90-day data retention',
      ],
      roi: '1,209% annual ROI',
      payback: '1 month',
    },
    {
      name: 'Professional',
      price: '$1,167',
      setup: '$10,100',
      best: 'Mid-market banks, medium size construction companies',
      networks: '5-10',
      txPerDay: '100K',
      users: '10-25',
      support: '8x5 Email',
      popular: true,
      features: [
        'Robocorp Cloud (10K runs)',
        'AI: Data normalization (1K calls/mo, DeepSeek)',
        'Dynamics 365 Business Central (10 Premium users)',
        'Dynamics 365 built-in reports (no Power BI needed)',
        '99.5% uptime SLA',
        '1-year data retention',
        'SOC 2 compliance tracking',
      ],
      roi: '890% annual ROI (with D365: $2,167/mo total)',
      payback: '1.3 months',
    },
    {
      name: 'Enterprise',
      price: '$12,950',
      setup: '$125,000',
      best: 'Large construction companies',
      networks: '20+',
      txPerDay: '1M+',
      users: '500',
      support: 'Daily Operations Support',
      features: [
        'UiPath (5 robots) + Robocorp (50K)',
        'AI: Full suite (unlimited, GPT-4/Gemini/DeepSeek)',
        'AI: Adaptive selectors, NL interface, doc understanding',
        'Dynamics 365 Business Central (100 Premium + 400 Team)',
        'Power BI Premium',
        '99.9% uptime SLA',
        'Unlimited data retention',
        'SOC 2 + PCI-DSS + ISO 27001',
        'Multi-region deployment',
        'Disaster recovery',
      ],
      roi: 'ROI via margin gains (with D365: $26,150/mo total)',
      payback: '3-4 months',
    },
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-purple-50 to-slate-100 p-8">
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold text-green-600 mb-4">
            Pricing & Budget Options
          </h1>
          <p className="text-gray-700 text-xl mb-2">
            3 deployment tiers to fit your needs and budget
          </p>
          <p className="text-blue-600 font-semibold text-lg">
            💵 All prices in USD (United States Dollars)
          </p>
        </div>

        {/* Pricing Cards */}
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6 mb-12">
          {tiers.map((tier) => (
            <div
              key={tier.name}
              className={`bg-blue-50 shadow-lg rounded-lg p-6 relative border-2 ${
                tier.popular ? 'border-blue-500 scale-105' : 'border-blue-200'
              }`}
            >
              {tier.popular && (
                <div className="absolute -top-3 left-1/2 transform -translate-x-1/2">
                  <span className="badge-primary px-4 py-1">Most Popular</span>
                </div>
              )}
              <h3 className="text-2xl font-bold text-green-600 mb-2">{tier.name}</h3>
              <div className="mb-4">
                <div className="text-4xl font-bold text-green-600">{tier.price}</div>
                <div className="text-gray-700 text-sm">per month (USD)</div>
                <div className="text-blue-600 text-xs mt-1">Setup: {tier.setup}</div>
              </div>

              <div className="space-y-3 mb-6">
                <div>
                  <div className="text-blue-600 text-xs">Best For</div>
                  <div className="text-gray-800 text-sm font-semibold">{tier.best}</div>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-blue-600">Networks</div>
                    <div className="text-gray-800 font-semibold">{tier.networks}</div>
                  </div>
                  <div>
                    <div className="text-blue-600">TX/Day</div>
                    <div className="text-gray-800 font-semibold">{tier.txPerDay}</div>
                  </div>
                  <div>
                    <div className="text-blue-600">Users</div>
                    <div className="text-gray-800 font-semibold">{tier.users}</div>
                  </div>
                  <div>
                    <div className="text-blue-600">Support</div>
                    <div className="text-gray-800 font-semibold">{tier.support}</div>
                  </div>
                </div>
              </div>

              <ul className="space-y-2 mb-6">
                {tier.features.map((feature) => (
                  <li key={feature} className="text-gray-700 text-xs flex items-start">
                    <span className="text-green-600 mr-2">✓</span>
                    <span>{feature}</span>
                  </li>
                ))}
              </ul>

              <div className="border-t border-gray-300 pt-4">
                <div className="text-green-600 font-semibold text-sm">{tier.roi}</div>
                <div className="text-blue-600 text-xs">Payback: {tier.payback}</div>
              </div>
            </div>
          ))}
        </div>

        {/* Comparison Table */}
        <section className="mb-12">
          <h2 className="text-3xl font-bold text-green-600 mb-6">Feature Comparison</h2>
          <div className="bg-blue-50 shadow-lg rounded-lg overflow-x-auto border-2 border-blue-200">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-300">
                  <th className="text-left p-4 text-green-600">Feature</th>
                  <th className="p-4 text-green-600">Starter</th>
                  <th className="p-4 text-green-600">Professional</th>
                  <th className="p-4 text-green-600">Enterprise</th>
                </tr>
              </thead>
              <tbody className="text-gray-700">
                <tr className="border-b border-gray-300 bg-blue-100">
                  <td className="p-4 font-semibold text-blue-600">RPA Platform Cost (USD/mo)</td>
                  <td className="p-4 text-center font-bold">$105</td>
                  <td className="p-4 text-center font-bold">$1,167</td>
                  <td className="p-4 text-center font-bold">$12,950</td>
                </tr>
                <tr className="border-b border-gray-300 bg-blue-100">
                  <td className="p-4 font-semibold text-blue-600">Dynamics 365 Add-On (USD/mo)</td>
                  <td className="p-4 text-center text-red-600">Not Available</td>
                  <td className="p-4 text-center font-bold">+$1,000</td>
                  <td className="p-4 text-center font-bold">+$13,200</td>
                </tr>
                <tr className="border-b border-gray-300 bg-purple-50">
                  <td className="p-4 font-semibold text-purple-600">Total Monthly Cost</td>
                  <td className="p-4 text-center font-bold text-purple-600">$105</td>
                  <td className="p-4 text-center font-bold text-purple-600">$2,167</td>
                  <td className="p-4 text-center font-bold text-purple-600">$26,150</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Setup Fee (One-time)</td>
                  <td className="p-4 text-center">$12</td>
                  <td className="p-4 text-center">$10,100</td>
                  <td className="p-4 text-center">$125,000</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Banking Networks</td>
                  <td className="p-4 text-center">1-3</td>
                  <td className="p-4 text-center">5-10</td>
                  <td className="p-4 text-center">20+</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Users Supported</td>
                  <td className="p-4 text-center">5</td>
                  <td className="p-4 text-center">25</td>
                  <td className="p-4 text-center">500</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">UiPath Robots</td>
                  <td className="p-4 text-center text-red-600">✗</td>
                  <td className="p-4 text-center text-red-600">✗</td>
                  <td className="p-4 text-center text-green-600">5 robots</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Robocorp Runs</td>
                  <td className="p-4 text-center text-red-600">✗</td>
                  <td className="p-4 text-center text-green-600">10K/month</td>
                  <td className="p-4 text-center text-green-600">50K/month</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Dynamics 365 Business Central</td>
                  <td className="p-4 text-center text-red-600">✗</td>
                  <td className="p-4 text-center text-green-600">10 Premium users</td>
                  <td className="p-4 text-center text-green-600">100 Premium + 400 Team</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Power BI / Reporting</td>
                  <td className="p-4 text-center text-red-600">✗</td>
                  <td className="p-4 text-center text-green-600">D365 built-in reports</td>
                  <td className="p-4 text-center text-green-600">Premium P1</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">AI Credits (DeepSeek/GPT-4)</td>
                  <td className="p-4 text-center">Local only</td>
                  <td className="p-4 text-center">1K calls/mo</td>
                  <td className="p-4 text-center">Unlimited</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Data Retention</td>
                  <td className="p-4 text-center">90 days</td>
                  <td className="p-4 text-center">1 year</td>
                  <td className="p-4 text-center">Unlimited</td>
                </tr>
                <tr className="border-b border-gray-300">
                  <td className="p-4 font-semibold text-blue-600">Uptime SLA</td>
                  <td className="p-4 text-center">99%</td>
                  <td className="p-4 text-center">99.5%</td>
                  <td className="p-4 text-center">99.9%</td>
                </tr>
                <tr>
                  <td className="p-4 font-semibold text-blue-600">Support</td>
                  <td className="p-4 text-center">Community</td>
                  <td className="p-4 text-center">8x5 Email</td>
                  <td className="p-4 text-center">Daily Operations Support</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        {/* ROI Examples */}
        <section className="mb-12">
          <h2 className="text-3xl font-bold text-green-600 mb-6">ROI Examples (USD)</h2>
          <div className="grid md:grid-cols-3 gap-6">
            <div className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200">
              <h3 className="text-xl font-bold text-green-600 mb-4">Starter Tier</h3>
              <p className="text-gray-700 text-sm mb-4">Small construction company (5-10 employees)</p>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-blue-600">Manual Tasks:</span>
                  <span className="text-gray-800">Bank data entry, reconciliation</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Time Saved:</span>
                  <span className="text-gray-800">2.5 hours/day</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Labor Cost:</span>
                  <span className="text-gray-800">$25/hour bookkeeper</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Monthly Savings:</span>
                  <span className="text-green-600 font-semibold">$1,375</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Platform Cost:</span>
                  <span className="text-gray-800">-$105</span>
                </div>
                <div className="border-t border-gray-300 pt-2 flex justify-between">
                  <span className="text-gray-700 font-semibold">Net Savings:</span>
                  <span className="text-green-600 font-bold">$1,270/month</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Annual ROI:</span>
                  <span className="text-green-600 font-bold">1,209%</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-blue-600">Payback Period:</span>
                  <span className="text-gray-700">1 month</span>
                </div>
              </div>
            </div>

            <div className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200">
              <h3 className="text-xl font-bold text-green-600 mb-4">Professional Tier</h3>
              <p className="text-gray-700 text-sm mb-4">Medium construction company (30-50 employees, 10 office staff)</p>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-blue-600">Benefits:</span>
                  <span className="text-gray-800">RPA + Dynamics 365 (10 users)</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Time Saved:</span>
                  <span className="text-gray-800">Month-end 10→5 days, invoices 48→4hrs</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Staff Efficiency:</span>
                  <span className="text-gray-800">2 FTE saved + margin improvement</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Labor Savings:</span>
                  <span className="text-green-600 font-semibold">$9,000/month</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Margin Improvement:</span>
                  <span className="text-green-600 font-semibold">+3% on $350K revenue = $10,500</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Error Reduction:</span>
                  <span className="text-green-600 font-semibold">$2,000/month</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Total Value:</span>
                  <span className="text-green-600 font-semibold">$21,500/month</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Total Cost:</span>
                  <span className="text-gray-800">-$2,167 (RPA + D365)</span>
                </div>
                <div className="border-t border-gray-300 pt-2 flex justify-between">
                  <span className="text-gray-700 font-semibold">Net Benefit:</span>
                  <span className="text-green-600 font-bold">$19,333/month</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Annual ROI:</span>
                  <span className="text-green-600 font-bold">890%</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-blue-600">Payback Period:</span>
                  <span className="text-gray-700">1.3 months</span>
                </div>
              </div>
            </div>

            <div className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200">
              <h3 className="text-xl font-bold text-green-600 mb-4">Enterprise Tier</h3>
              <p className="text-gray-700 text-sm mb-4">Large construction company (100-500 employees)</p>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-blue-600">Benefits:</span>
                  <span className="text-gray-800">Full automation + ERP modernization</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Efficiency Gains:</span>
                  <span className="text-gray-800">Multi-project tracking, real-time reporting</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Staff Savings:</span>
                  <span className="text-gray-800">3 FTE accounting + error reduction</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Monthly Savings:</span>
                  <span className="text-green-600 font-semibold">$20,000</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Total Cost:</span>
                  <span className="text-gray-800">-$26,150 (RPA + D365)</span>
                </div>
                <div className="border-t border-gray-300 pt-2 flex justify-between">
                  <span className="text-gray-700 font-semibold">Net Loss:</span>
                  <span className="text-red-600 font-bold">-$6,150/month</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">BUT: Gross Margin Gain:</span>
                  <span className="text-green-600 font-semibold">+3-5% on $2M/mo revenue</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-blue-600">Total Value:</span>
                  <span className="text-green-600 font-bold">+$60,000-$100,000/mo</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-blue-600">Payback Period:</span>
                  <span className="text-gray-700">3-4 months (w/ margin gains)</span>
                </div>
              </div>
            </div>
          </div>

          <div className="mt-6 bg-yellow-50 border-2 border-yellow-400 rounded-lg p-6">
            <h3 className="text-lg font-bold text-gray-800 mb-3">💡 Understanding ROI for Construction Companies</h3>
            <div className="grid md:grid-cols-2 gap-4 text-sm text-gray-700">
              <div>
                <p className="font-semibold text-blue-600 mb-2">Direct Labor Savings:</p>
                <ul className="space-y-1 ml-4">
                  <li>• Reduced manual data entry time</li>
                  <li>• Faster month-end close (10 days → 5 days)</li>
                  <li>• Automated bank reconciliation</li>
                  <li>• Streamlined invoice processing</li>
                </ul>
              </div>
              <div>
                <p className="font-semibold text-blue-600 mb-2">Indirect Value (Hard to Quantify):</p>
                <ul className="space-y-1 ml-4">
                  <li>• Better project cost visibility → improved bidding</li>
                  <li>• Real-time cash flow tracking → avoid overdrafts</li>
                  <li>• Fewer accounting errors → reduced write-offs</li>
                  <li>• Improved gross margins (3-5% typical gain)</li>
                </ul>
              </div>
            </div>
            <p className="text-xs text-gray-600 mt-4">
              <strong>Note:</strong> ROI calculations based on 2026 market rates for construction industry bookkeepers ($25/hr),
              accountants ($35/hr), and typical ERP efficiency gains reported by Dynamics 365 customers.
              Actual results vary by company size, processes, and implementation quality.
            </p>
          </div>
        </section>

        {/* Financing Options */}
        <section className="mb-12">
          <h2 className="text-3xl font-bold text-green-600 mb-6">Financing Options</h2>
          <div className="grid md:grid-cols-3 gap-6">
            <div className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200">
              <h3 className="text-green-600 font-bold mb-3">Annual Prepayment</h3>
              <div className="text-3xl font-bold text-green-600 mb-2">10% OFF</div>
              <p className="text-gray-700 text-sm">Pay annually and save 10% on monthly fees</p>
            </div>
            <div className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200">
              <h3 className="text-green-600 font-bold mb-3">2-Year Contract</h3>
              <div className="text-3xl font-bold text-green-600 mb-2">18% OFF</div>
              <p className="text-gray-700 text-sm">Commit for 2 years with monthly billing option</p>
            </div>
            <div className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200">
              <h3 className="text-green-600 font-bold mb-3">3-Year Contract</h3>
              <div className="text-3xl font-bold text-green-600 mb-2">25% OFF</div>
              <p className="text-gray-700 text-sm">Maximum savings with 3-year commitment</p>
            </div>
          </div>
        </section>

        {/* Links */}
        <section className="mb-12">
          <h2 className="text-3xl font-bold text-green-600 mb-6">Additional Resources</h2>
          <div className="grid md:grid-cols-3 gap-4">
            <a href="/docs/networks" className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200 hover:border-blue-400 transition-colors">
              <h3 className="text-green-600 font-bold mb-2">🌐 Banking Networks</h3>
              <p className="text-gray-700 text-sm">See supported networks and availability</p>
            </a>
            <a href="/docs/setup" className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200 hover:border-blue-400 transition-colors">
              <h3 className="text-green-600 font-bold mb-2">🔧 Setup Guide</h3>
              <p className="text-gray-700 text-sm">Dynamics 365 & Power BI integration</p>
            </a>
            <a href="/docs/integrations" className="bg-blue-50 shadow-lg rounded-lg p-6 border-2 border-blue-200 hover:border-blue-400 transition-colors">
              <h3 className="text-green-600 font-bold mb-2">🤖 RPA Options</h3>
              <p className="text-gray-700 text-sm">UiPath and Robocorp integration</p>
            </a>
          </div>
        </section>

        {/* CTA */}
        <div className="text-center bg-blue-50 shadow-lg rounded-lg p-8 border-2 border-blue-200">
          <h2 className="text-2xl font-bold text-green-600 mb-4">Ready to Get Started?</h2>
          <p className="text-gray-700 mb-6">
            Start with a 30-day free trial of the Starter tier (no credit card required)
          </p>
          <div className="flex gap-4 justify-center">
            <a href="/" className="btn-primary px-8 py-3">Start Free Trial</a>
            <a href="mailto:sales@rpa-platform.com" className="btn-secondary px-8 py-3">
              Contact Sales
            </a>
          </div>
        </div>

        {/* Back Button */}
        <div className="text-center mt-8">
          <a href="/" className="text-blue-600 hover:text-blue-700">
            ← Back to Dashboard
          </a>
        </div>
      </div>
    </div>
  );
}
