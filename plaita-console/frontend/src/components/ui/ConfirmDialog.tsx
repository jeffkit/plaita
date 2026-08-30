import { useEffect, useRef } from 'react'
import { Loader2 } from 'lucide-react'
import { cn } from './cn'
import { Button } from './Button'

export interface ConfirmDialogProps {
  open: boolean
  title: string
  /** 正文说明；复杂内容（如变更摘要）可直接传节点 */
  children?: React.ReactNode
  confirmLabel?: string
  cancelLabel?: string
  /** danger = 红色确认按钮（破坏性操作）；primary = 常规主操作 */
  variant?: 'primary' | 'danger'
  /** 大宽度布局（放表格/摘要时开启） */
  wide?: boolean
  busy?: boolean
  onConfirm: () => void
  onCancel: () => void
}

/**
 * 确认对话框基元（DESIGN.md §5 浮层）：elevated 底 + line-strong 描边。
 * 自带 Esc 关闭、role="dialog"、焦点落在取消按钮上——替代散落的 window.confirm。
 */
export function ConfirmDialog({
  open,
  title,
  children,
  confirmLabel = '确认',
  cancelLabel = '取消',
  variant = 'primary',
  wide = false,
  busy = false,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  const cancelAnchorRef = useRef<HTMLSpanElement>(null)

  useEffect(() => {
    if (!open) return
    // 焦点落在取消按钮上（Button 透传 autoFocus），防误触回车直接确认
    const t = window.setTimeout(() => {
      cancelAnchorRef.current?.querySelector('button')?.focus()
    }, 0)
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !busy) onCancel()
    }
    window.addEventListener('keydown', onKey)
    return () => {
      window.clearTimeout(t)
      window.removeEventListener('keydown', onKey)
    }
  }, [open, busy, onCancel])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center animate-fade">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={busy ? undefined : onCancel} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className={cn(
          'relative bg-elevated border border-line-strong rounded-xl shadow-pop animate-pop',
          wide ? 'w-full max-w-xl' : 'w-full max-w-sm'
        )}
      >
        <div className="px-5 py-3.5 border-b border-line">
          <h2 className="text-section text-ink-primary">{title}</h2>
        </div>
        {children && <div className="px-5 py-4 space-y-3 text-body text-ink-secondary">{children}</div>}
        <div className="flex items-center justify-end gap-2 px-5 py-3.5 border-t border-line">
          <span ref={cancelAnchorRef}>
            <Button variant="secondary" size="sm" onClick={onCancel} disabled={busy}>
              {cancelLabel}
            </Button>
          </span>
          <Button variant={variant} size="sm" onClick={onConfirm} disabled={busy}>
            {busy && <Loader2 size={13} className="animate-spin" />}
            {confirmLabel}
          </Button>
        </div>
      </div>
    </div>
  )
}
