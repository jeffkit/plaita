import type { ReactNode } from 'react'
import type { HTMLAttributes } from 'react'
import { cn } from './cn'

/** 页面容器：统一 24px 内边距与纵向节奏 */
export function Page({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn('p-6 space-y-5', className)} {...rest}>
      {children}
    </div>
  )
}

export interface PageHeaderProps {
  title: ReactNode
  subtitle?: ReactNode
  actions?: ReactNode
}

/** 页头：page-title 上限 + 副标题 + 右侧动作区（DESIGN.md §5） */
export function PageHeader({ title, subtitle, actions }: PageHeaderProps) {
  return (
    <header className="flex items-end justify-between gap-4">
      <div className="min-w-0">
        <h1 className="text-page-title text-ink-primary">{title}</h1>
        {subtitle && <p className="mt-0.5 text-caption text-ink-muted">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
    </header>
  )
}
