'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { cn } from '@/lib/utils';

const navItems = [
  { name: 'Overview', href: '/' },
  { name: 'Architecture', href: '/architecture' },
  { name: 'Features', href: '/features' },
  { name: 'Implementation', href: '/implementation' },
  { name: 'Security', href: '/security' },
  { name: 'Pipeline', href: '/pipeline' },
  { name: 'Examples', href: '/examples' },
  { name: 'Deployment', href: '/deployment' },
];

/**
 * Navigation Component
 * Main navigation bar with active state
 */
export function Navigation() {
  const pathname = usePathname();

  return (
    <nav className="flex gap-2 flex-wrap">
      {navItems.map((item) => {
        const isActive = pathname === item.href;

        return (
          <Link
            key={item.href}
            href={item.href}
            className={cn(
              'nav-btn',
              isActive && 'active'
            )}
          >
            {item.name}
          </Link>
        );
      })}
    </nav>
  );
}
