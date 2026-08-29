import type { ButtonHTMLAttributes } from 'react'
import { cn } from './cn'

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger'
type Size = 'sm' | 'md'

const variantClasses: Record<Variant, string> = {
  primary: 'bg-plaita-500 hover:bg-plaita-600 text-on-accent shadow-card',
  secondary: 'bg-elevated border border-line text-ink-primary hover:bg-dark-700',
  ghost: 'text-ink-secondary hover:text-ink-primary hover:bg-elevated',
  danger: 'text-status-error border border-transparent hover:bg-status-error-dim hover:border-status-error/30',
}

const sizeClasses: Record<Size, string> = {
  sm: 'h-7 px-2.5 text-caption rounded-md gap-1.5',
  md: 'h-8 px-3 text-body rounded-lg gap-2',
}

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant
  size?: Size
}

/** 按钮（DESIGN.md §5）：按压 scale(0.98)，时长 150ms；主色随主题取对比字色 */
export function Button({ variant = 'secondary', size = 'md', className, type = 'button', ...rest }: ButtonProps) {
  return (
    <button
      type={type}
      className={cn(
        'inline-flex items-center justify-center gap-0 whitespace-nowrap font-medium transition-[color,background-color,border-color,transform] duration-150',
        'active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed disabled:active:scale-100',
        variantClasses[variant],
        sizeClasses[size],
        className,
      )}
      {...rest}
    />
  )
}
