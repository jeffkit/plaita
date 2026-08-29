import { useRef, useState } from 'react'
import { cn } from '../../ui/cn'

export interface VarItem {
  expr: string
  desc?: string
}
export interface VarGroup {
  label: string
  items: VarItem[]
}

/**
 * 表达式输入框：文本输入 + 右侧「$」变量菜单。
 * 菜单变量由调用方构建（$INPUT 流程入参 / $NODE 上游结果 / $GLOBAL 全局上下文），
 * 点击插入到光标位置；引擎字符串字段均支持表达式与 {% expr %} 模板插值。
 */
export default function ExpressionInput({
  value,
  onChange,
  groups,
  placeholder,
}: {
  value: string
  onChange: (v: string) => void
  groups: VarGroup[]
  placeholder?: string
}) {
  const [open, setOpen] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  const insert = (expr: string) => {
    const el = inputRef.current
    const cur = value ?? ''
    if (!el) {
      onChange(cur + expr)
      setOpen(false)
      return
    }
    const start = el.selectionStart ?? cur.length
    const end = el.selectionEnd ?? cur.length
    const next = cur.slice(0, start) + expr + cur.slice(end)
    onChange(next)
    setOpen(false)
    requestAnimationFrame(() => {
      el.focus()
      const pos = start + expr.length
      el.setSelectionRange(pos, pos)
    })
  }

  return (
    <div className="relative">
      <input
        ref={inputRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="input w-full pr-8 font-mono text-[12px]"
      />
      <button
        type="button"
        title="插入变量 / 表达式"
        onClick={() => setOpen((v) => !v)}
        className={cn(
          'absolute right-1.5 top-1/2 -translate-y-1/2 w-5 h-5 flex items-center justify-center rounded font-mono text-[11px] transition-colors',
          open
            ? 'bg-plaita-500/20 text-plaita-400'
            : 'text-ink-faint hover:text-ink-primary hover:bg-elevated'
        )}
      >
        $
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 w-80 max-h-72 overflow-y-auto bg-elevated border border-line rounded-lg shadow-pop p-1">
          {groups.length === 0 && (
            <p className="px-2 py-2 text-[11px] text-ink-faint">
              当前无可插入变量（需上游节点或流程入参声明）
            </p>
          )}
          {groups.map((g) => (
            <div key={g.label} className="mb-1 last:mb-0">
              <p className="px-2 pt-1.5 pb-1 text-[10px] uppercase tracking-wide text-ink-faint">
                {g.label}
              </p>
              {g.items.map((it) => (
                <button
                  key={it.expr}
                  type="button"
                  onMouseDown={(e) => {
                    e.preventDefault() // 保持输入框焦点
                    insert(it.expr)
                  }}
                  className="w-full flex items-center gap-2 px-2 py-1.5 rounded-md hover:bg-dark-700 text-left"
                >
                  <span className="font-mono text-[11px] text-plaita-400 shrink-0">
                    {it.expr}
                  </span>
                  {it.desc && (
                    <span className="truncate text-[11px] text-ink-faint">{it.desc}</span>
                  )}
                </button>
              ))}
            </div>
          ))}
          <p className="px-2 pt-1.5 pb-1 border-t border-line text-[10px] leading-4 text-ink-faint">
            支持 $INPUT/$NODE/$GLOBAL/$ENV 取值、$F.函数() 调用、{'{% expr %}'} 模板插值
          </p>
        </div>
      )}
    </div>
  )
}
