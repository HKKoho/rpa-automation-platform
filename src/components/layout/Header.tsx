import { Navigation } from './Navigation';

export interface HeaderProps {
  title?: string;
  subtitle?: string;
}

/**
 * Header Component
 * Application header with title and navigation
 */
export function Header({
  title = '🤖 RPA Automation Platform',
  subtitle = 'Enterprise-Grade Robotic Process Automation with AI-Powered Data Extraction',
}: HeaderProps) {
  return (
    <header className="card-glass mb-8 text-center">
      <h1 className="text-5xl font-bold mb-4 bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">{title}</h1>
      <p className="text-xl text-gray-700 mb-2">{subtitle}</p>
      <p className="text-gray-600 text-sm mb-6">Banking Network Utility Operations</p>
      <Navigation />
    </header>
  );
}
