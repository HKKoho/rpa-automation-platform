import React from 'react';
import { cn } from '@/lib/utils';

export interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: 'success' | 'warning' | 'danger' | 'info';
}

/**
 * Badge Component
 * Status and category badges
 */
export function Badge({
  variant = 'info',
  className,
  children,
  ...props
}: BadgeProps) {
  const variantClasses = {
    success: 'badge-success',
    warning: 'badge-warning',
    danger: 'badge-danger',
    info: 'badge-info',
  };

  return (
    <span
      className={cn(variantClasses[variant], className)}
      {...props}
    >
      {children}
    </span>
  );
}
