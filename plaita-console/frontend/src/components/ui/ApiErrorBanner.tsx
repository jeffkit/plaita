import { useEffect, useState } from 'react'
import { AlertCircle, X } from 'lucide-react'
import { subscribeApiErrors } from './apiErrorBus'

/**
 * 写操作失败兜底横幅：任何未自行展示错误的 mutation 都不会静默。
 * 固定右下角，可手动关闭；相同文案去重。
 */
export function ApiErrorBanner() {
  const [messages, setMessages] = useState<string[]>([])

  useEffect(() => {
    return subscribeApiErrors((message) => {
      setMessages((prev) => (prev.includes(message) ? prev : [...prev, message]))
    })
  }, [])

  if (messages.length === 0) return null

  return (
    <div
      data-testid="api-error-banner"
      className="fixed right-4 bottom-4 z-[60] w-[min(420px,calc(100vw-2rem))] space-y-2"
    >
      {messages.map((message) => (
        <div
          key={message}
          className="flex items-start gap-2 rounded-lg border border-status-error/40 bg-status-error-dim px-3 py-2.5 shadow-pop"
        >
          <AlertCircle size={15} className="mt-0.5 shrink-0 text-status-error" />
          <p className="min-w-0 flex-1 whitespace-pre-wrap break-words text-caption text-status-error">
            {message}
          </p>
          <button
            type="button"
            aria-label="关闭错误提示"
            title="关闭"
            onClick={() => setMessages((prev) => prev.filter((m) => m !== message))}
            className="shrink-0 text-status-error/70 hover:text-status-error"
          >
            <X size={14} />
          </button>
        </div>
      ))}
    </div>
  )
}
