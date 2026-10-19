import { AlertCircle } from 'lucide-react'
import { ApiError, errorUserMessage } from '../../services/transport'
import { Button } from './Button'

export interface ErrorStateProps {
  error: unknown
  title?: string
  onRetry?: () => void
  compact?: boolean
}

/** 读面错误态：禁止把请求失败折算成 []/0/「暂无数据」。 */
export function ErrorState({ error, title = '加载失败', onRetry, compact = false }: ErrorStateProps) {
  const details =
    error instanceof ApiError
      ? [error.status ? `HTTP ${error.status}` : '', error.url].filter(Boolean).join(' · ')
      : ''
  return (
    <div
      data-testid="error-state"
      className={
        compact
          ? 'flex items-center gap-2 px-3 py-2 rounded-md bg-status-error-dim text-status-error text-caption'
          : 'flex flex-col items-center justify-center py-10 text-center'
      }
    >
      <AlertCircle size={compact ? 14 : 40} className={compact ? 'shrink-0' : 'mb-3 text-status-error'} />
      <div className={compact ? 'min-w-0 flex-1' : 'max-w-[70ch]'}>
        <p className={compact ? 'text-caption' : 'text-body font-medium text-status-error'}>{title}</p>
        <p className="mt-1 break-words text-caption text-ink-muted">{errorUserMessage(error)}</p>
        {details && (
          <p className="mt-1 break-all font-mono text-micro text-ink-faint">{details}</p>
        )}
      </div>
      {onRetry && (
        <Button variant="secondary" size="sm" onClick={onRetry} className={compact ? 'shrink-0' : 'mt-4'}>
          重试
        </Button>
      )}
    </div>
  )
}

export interface ErrorBannerProps {
  error: unknown
  title?: string
  onRetry?: () => void
}

/** 页面局部错误条（单行，用于一屏多查询的页面）。 */
export function ErrorBanner({ error, title = '数据加载失败', onRetry }: ErrorBannerProps) {
  return <ErrorState error={error} title={title} onRetry={onRetry} compact />
}
