import type { ReactNode } from 'react'

export interface EmptyStateProps {
  icon?: ReactNode
  message: string
  hint?: string
  action?: ReactNode
}

/** 空态：禁止白板（DESIGN.md §6-6），图标 + 一句话 + 可选动作 */
export function EmptyState({ icon, message, hint, action }: EmptyStateProps) {
  return (
    <div className="py-10 text-center">
      {icon && <div className="flex justify-center text-ink-faint">{icon}</div>}
      <p className="mt-2 text-caption text-ink-muted">{message}</p>
      {hint && <p className="mt-1 text-caption text-ink-faint">{hint}</p>}
      {action && <div className="mt-3 flex justify-center">{action}</div>}
    </div>
  )
}
