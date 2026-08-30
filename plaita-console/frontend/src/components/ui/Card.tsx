import type { HTMLAttributes, ReactNode } from 'react'
import { Link } from 'react-router-dom'
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
  /** 传入路由即整卡可点击下钻（仪表盘卡片是入口，不是终点） */
  to?: string
}

/** 统计卡：micro 大写标签 + 等宽大数值，图标退后（DESIGN.md §5） */
export function StatCard({ icon, title, value, total, to }: StatCardProps) {
  const body = (
    <>
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
    </>
  )
  if (to) {
    return (
      <Link
        to={to}
        className="block bg-surface border border-line rounded-xl shadow-card p-4 transition-colors hover:border-line-strong hover:bg-elevated"
      >
        {body}
      </Link>
    )
  }
  return <Card className="p-4">{body}</Card>
}
