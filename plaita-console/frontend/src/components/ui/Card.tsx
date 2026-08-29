import type { HTMLAttributes, ReactNode } from 'react'
import { cn } from './cn'

/** 卡片（DESIGN.md §2.1/§3：surface + line + shadow-card，禁三件套自由发挥） */
export function Card({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn('bg-surface border border-line rounded-xl shadow-card', className)} {...rest}>
      {children}
    </div>
  )
}

export interface StatCardProps {
  icon?: ReactNode
  title: string
  value: ReactNode
  total?: ReactNode
}

/** 统计卡：micro 大写标签 + 等宽大数值，图标退后（DESIGN.md §5） */
export function StatCard({ icon, title, value, total }: StatCardProps) {
  return (
    <Card className="p-4">
      <div className="flex items-center justify-between">
        <p className="text-micro uppercase text-ink-muted">{title}</p>
        {icon}
      </div>
      <p className="mt-2.5 font-mono text-2xl font-semibold text-ink-primary tabular-nums">
        {value}
        {total !== undefined && (
          <span className="ml-1 text-base font-normal text-ink-muted">/ {total}</span>
        )}
      </p>
    </Card>
  )
}
