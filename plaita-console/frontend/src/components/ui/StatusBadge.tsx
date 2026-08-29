import { cn } from './cn'

/**
 * 状态徽章：只收语义名，不收颜色（DESIGN.md §2.5）。
 * running 独享呼吸点；running/success 同绿系，靠动效与静态区分。
 */
const STATUS_STYLES: Record<string, { badge: string; dot: string; breathing?: boolean }> = {
  running:   { badge: 'text-status-running bg-status-running-dim',     dot: 'bg-status-running',     breathing: true },
  current:   { badge: 'text-status-running bg-status-running-dim',     dot: 'bg-status-running',     breathing: true },
  completed: { badge: 'text-status-success bg-status-success-dim',     dot: 'bg-status-success' },
  success:   { badge: 'text-status-success bg-status-success-dim',     dot: 'bg-status-success' },
  published: { badge: 'text-status-success bg-status-success-dim',     dot: 'bg-status-success' },
  suspended: { badge: 'text-status-warning bg-status-warning-dim',     dot: 'bg-status-warning' },
  waiting:   { badge: 'text-status-warning bg-status-warning-dim',     dot: 'bg-status-warning' },
  error:     { badge: 'text-status-error bg-status-error-dim',         dot: 'bg-status-error' },
  pending:   { badge: 'text-status-pending bg-status-pending-dim',     dot: 'bg-status-pending' },
  queued:    { badge: 'text-status-pending bg-status-pending-dim',     dot: 'bg-status-pending' },
  draft:     { badge: 'text-status-pending bg-status-pending-dim',     dot: 'bg-status-pending' },
  stopped:   { badge: 'text-status-cancelled bg-status-cancelled-dim', dot: 'bg-status-cancelled' },
  cancelled: { badge: 'text-status-cancelled bg-status-cancelled-dim', dot: 'bg-status-cancelled' },
  skipped:   { badge: 'text-status-cancelled bg-status-cancelled-dim', dot: 'bg-status-cancelled' },
}

// 已知状态的中文标签；未知状态回退展示原值
const STATUS_LABELS: Record<string, string> = {
  running: '运行中',
  current: '执行中',
  completed: '已完成',
  success: '成功',
  published: '已发布',
  suspended: '已暂停',
  waiting: '等待中',
  error: '错误',
  pending: '等待',
  queued: '排队中',
  draft: '草稿',
  stopped: '已停止',
  cancelled: '已取消',
  skipped: '已跳过',
}

export function StatusBadge({ status, className }: { status: string; className?: string }) {
  const style = STATUS_STYLES[status] ?? STATUS_STYLES.stopped
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md border border-line text-caption',
        style.badge,
        className,
      )}
    >
      <span className={cn('w-1.5 h-1.5 rounded-full shrink-0', style.dot, style.breathing && 'animate-breathe')} />
      {STATUS_LABELS[status] ?? status}
    </span>
  )
}
