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
    <header className="card-glass mb-8">
      <h1 className="text-4xl font-bold mb-2">{title}</h1>
      <p className="subtitle text-lg mb-6">{subtitle}</p>
      <Navigation />
    </header>
  );
}
