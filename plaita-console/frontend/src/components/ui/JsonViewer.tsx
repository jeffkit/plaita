import { useMemo, useState, type ReactNode } from 'react'
import { Check, ChevronDown, ChevronRight, Copy, Search, Rows3, TreePine } from 'lucide-react'
import { cn } from './cn'
import { entriesOf, kindOf, summaryOf, type JsonKind } from './jsonValue'

/**
 * JSON 查看器（执行上下文 / 流程输出 / 节点返回值共用）。
 *
 * 解决的痛点：执行上下文动辄几万字符，用 <pre> 直接铺开就是「一大块文本」。
 * 这里给两种视图：
 * - 树：默认折叠到指定深度，容器节点显示「键数/项数」，可逐级展开、可搜索过滤；
 * - 原始：带语法高亮的 JSON 文本，保留整体观感与行数。
 * 两种视图都可一键复制。
 *
 * 配色只用语义 token（随 data-theme 翻转），不写死颜色（DESIGN.md §2）。
 */

export interface JsonViewerProps {
  value: unknown
  /** 根名称（如 "context"）。仅作显示，不影响数据 */
  rootName?: string
  /** 初始展开深度：0 = 全部折叠，1 = 展开第一层（默认） */
  defaultExpandDepth?: number
  /** 是否显示工具条（搜索 / 展开收起 / 复制 / 视图切换） */
  toolbar?: boolean
  /** 内容区最大高度类（默认 max-h-[30rem]） */
  maxHeightClass?: string
  className?: string
  emptyText?: string
}

const KIND_CLASS: Record<JsonKind, string> = {
  object: 'text-ink-faint',
  array: 'text-ink-faint',
  string: 'text-status-success',
  number: 'text-status-warning',
  boolean: 'text-status-running',
  null: 'text-ink-faint',
}

function matchesFilter(key: string | null, value: unknown, q: string): boolean {
  if (key && key.toLowerCase().includes(q)) return true
  if (value && typeof value === 'object') {
    return entriesOf(value).some(([k, v]) => matchesFilter(k, v, q))
  }
  if (value === null || value === undefined) return 'null'.includes(q)
  return String(value).toLowerCase().includes(q)
}

/** 长字符串折叠：上下文里的提示词动辄上千字，默认只显示前 160 字符 */
function StringValue({ text }: { text: string }) {
  const [expanded, setExpanded] = useState(false)
  const long = text.length > 160
  const shown = expanded || !long ? text : `${text.slice(0, 160)}…`
  return (
    <span className="text-status-success break-all whitespace-pre-wrap">
      &quot;{shown}&quot;
      {long && (
        <button
          onClick={() => setExpanded((v) => !v)}
          className="ml-1.5 text-caption text-plaita-400 hover:text-plaita-300"
        >
          {expanded ? '收起' : `展开 ${text.length} 字符`}
        </button>
      )}
    </span>
  )
}

function ScalarValue({ value }: { value: unknown }) {
  const kind = kindOf(value)
  if (kind === 'string') return <StringValue text={value as string} />
  if (kind === 'null') return <span className="text-ink-faint italic">null</span>
  return <span className={KIND_CLASS[kind]}>{String(value)}</span>
}

function JsonNode({
  name,
  value,
  depth,
  defaultExpandDepth,
  filter,
  isLast,
}: {
  name: string | null
  value: unknown
  depth: number
  defaultExpandDepth: number
  filter: string
  isLast: boolean
}) {
  const kind = kindOf(value)
  const container = kind === 'object' || kind === 'array'
  const [open, setOpen] = useState(depth < defaultExpandDepth)
  const searching = filter.length > 0
  const isOpen = searching ? true : open

  if (searching && !matchesFilter(name, value, filter)) return null

  const entries = container ? entriesOf(value) : []
  const comma = isLast ? '' : ','

  return (
    <div className="leading-5">
      <div
        className={cn('flex items-start gap-1 rounded', container && 'cursor-pointer hover:bg-elevated/50')}
        onClick={container ? () => setOpen((v) => !v) : undefined}
      >
        {container ? (
          <span className="mt-0.5 text-ink-faint shrink-0">
            {isOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          </span>
        ) : (
          <span className="w-3 shrink-0" />
        )}
        {name !== null && (
          <span className="text-ink-primary shrink-0">
            <span className="text-ink-muted">{kind === 'array' ? '' : '"'}</span>
            {name}
            <span className="text-ink-muted">{kind === 'array' ? '' : '"'}</span>
            <span className="text-ink-faint">:</span>
          </span>
        )}
        {container ? (
          <>
            <span className="text-ink-muted">{kind === 'array' ? '[' : '{'}</span>
            {!isOpen && (
              <>
                <button
                  onClick={(e) => {
                    e.stopPropagation()
                    setOpen(true)
                  }}
                  className="text-caption text-ink-faint hover:text-plaita-400 px-1"
                >
                  … {summaryOf(value)}
                </button>
                <span className="text-ink-muted">{kind === 'array' ? ']' : '}'}</span>
                <span className="text-ink-faint">{comma}</span>
              </>
            )}
            {isOpen && entries.length === 0 && (
              <>
                <span className="text-ink-muted">{kind === 'array' ? ']' : '}'}</span>
                <span className="text-ink-faint">{comma}</span>
              </>
            )}
          </>
        ) : (
          <>
            <ScalarValue value={value} />
            <span className="text-ink-faint">{comma}</span>
          </>
        )}
      </div>
      {container && isOpen && entries.length > 0 && (
        <>
          <div className="ml-4 border-l border-line pl-2">
            {entries.map(([k, v], i) => (
              <JsonNode
                key={`${k}-${i}`}
                name={k}
                value={v}
                depth={depth + 1}
                defaultExpandDepth={defaultExpandDepth}
                filter={filter}
                isLast={i === entries.length - 1}
              />
            ))}
          </div>
          <div className="text-ink-muted">
            <span className="w-3 inline-block" />
            {kind === 'array' ? ']' : '}'}
            <span className="text-ink-faint">{comma}</span>
          </div>
        </>
      )}
    </div>
  )
}

const TOKEN_RE =
  /("(?:\\.|[^"\\])*")(\s*:)?|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|\b(true|false)\b|\b(null)\b/g

/** 原始视图的极简 JSON 高亮（不引入 CodeMirror，主题无关、随 token 翻转） */
function highlightJson(text: string): ReactNode[] {
  const out: ReactNode[] = []
  let last = 0
  let key = 0
  let m: RegExpExecArray | null
  TOKEN_RE.lastIndex = 0
  while ((m = TOKEN_RE.exec(text)) !== null) {
    if (m.index > last) {
      out.push(<span key={`p${key++}`} className="text-ink-faint">{text.slice(last, m.index)}</span>)
    }
    if (m[1] !== undefined) {
      if (m[2] !== undefined) {
        out.push(<span key={`k${key++}`} className="text-ink-primary">{m[1]}</span>)
        out.push(<span key={`c${key++}`} className="text-ink-faint">{m[2]}</span>)
      } else {
        out.push(<span key={`s${key++}`} className="text-status-success">{m[1]}</span>)
      }
    } else if (m[3] !== undefined) {
      out.push(<span key={`n${key++}`} className="text-status-warning">{m[3]}</span>)
    } else if (m[4] !== undefined) {
      out.push(<span key={`b${key++}`} className="text-status-running">{m[4]}</span>)
    } else if (m[5] !== undefined) {
      out.push(<span key={`z${key++}`} className="text-ink-faint italic">{m[5]}</span>)
    }
    last = m.index + m[0].length
  }
  if (last < text.length) {
    out.push(<span key={`p${key++}`} className="text-ink-faint">{text.slice(last)}</span>)
  }
  return out
}

function copyText(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) return navigator.clipboard.writeText(text)
  const ta = document.createElement('textarea')
  ta.value = text
  ta.style.position = 'fixed'
  ta.style.opacity = '0'
  document.body.appendChild(ta)
  ta.select()
  document.execCommand('copy')
  document.body.removeChild(ta)
  return Promise.resolve()
}

export function JsonViewer({
  value,
  rootName,
  defaultExpandDepth = 1,
  toolbar = true,
  maxHeightClass = 'max-h-[30rem]',
  className,
  emptyText = '无数据',
}: JsonViewerProps) {
  const [view, setView] = useState<'tree' | 'raw'>('tree')
  const [filter, setFilter] = useState('')
  const [copied, setCopied] = useState(false)
  const [expandMode, setExpandMode] = useState<'default' | 'all' | 'collapsed'>('default')
  const [treeKey, setTreeKey] = useState(0)

  const text = useMemo(() => {
    if (value === undefined) return ''
    try {
      return typeof value === 'string' ? value : JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }, [value])

  const isEmpty = value === undefined || value === null || text === ''
  const lines = useMemo(() => (text ? text.split('\n').length : 0), [text])
  const matchCount = useMemo(() => {
    if (!filter) return 0
    const q = filter.toLowerCase()
    let n = 0
    const walk = (v: unknown) => {
      if (v && typeof v === 'object') {
        for (const [k, child] of entriesOf(v)) {
          if (k.toLowerCase().includes(q)) n += 1
          else if (child === null || typeof child !== 'object') {
            if (String(child).toLowerCase().includes(q)) n += 1
          }
          walk(child)
        }
      }
    }
    walk(value)
    return n
  }, [filter, value])

  if (isEmpty) {
    return <p className="text-data-sm text-ink-muted">{emptyText}</p>
  }

  return (
    <div className={cn('flex flex-col min-h-0', className)}>
      {toolbar && (
        <div className="flex items-center gap-2 flex-wrap mb-2">
          <div className="relative flex-1 min-w-[180px]">
            <Search size={13} className="absolute left-2 top-1/2 -translate-y-1/2 text-ink-faint" />
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="搜索键或值…"
              className="input w-full pl-7 py-1.5 text-data-sm"
            />
          </div>
          <div className="flex items-center rounded-md border border-line overflow-hidden">
            <button
              onClick={() => setView('tree')}
              className={cn(
                'flex items-center gap-1 px-2 py-1.5 text-caption transition-colors',
                view === 'tree' ? 'bg-plaita-500/10 text-plaita-400' : 'text-ink-muted hover:text-ink-primary'
              )}
              title="树形视图（可折叠）"
            >
              <TreePine size={12} /> 树
            </button>
            <button
              onClick={() => setView('raw')}
              className={cn(
                'flex items-center gap-1 px-2 py-1.5 text-caption transition-colors border-l border-line',
                view === 'raw' ? 'bg-plaita-500/10 text-plaita-400' : 'text-ink-muted hover:text-ink-primary'
              )}
              title="原始 JSON（语法高亮）"
            >
              <Rows3 size={12} /> 原始
            </button>
          </div>
          {view === 'tree' && (
            <>
              <button
                onClick={() => {
                  setFilter('')
                  setExpandMode('collapsed')
                  setTreeKey((k) => k + 1)
                }}
                className="text-caption text-ink-muted hover:text-plaita-400 px-1.5 py-1"
                title="折叠到第一层"
              >
                收起全部
              </button>
              <button
                onClick={() => {
                  setFilter('')
                  setExpandMode('all')
                  setTreeKey((k) => k + 1)
                }}
                className="text-caption text-ink-muted hover:text-plaita-400 px-1.5 py-1"
                title="展开全部层级"
              >
                展开全部
              </button>
            </>
          )}
          <button
            onClick={() => {
              void copyText(text).then(() => {
                setCopied(true)
                window.setTimeout(() => setCopied(false), 1500)
              })
            }}
            className="flex items-center gap-1 text-caption text-ink-muted hover:text-plaita-400 px-1.5 py-1"
            title="复制 JSON"
          >
            {copied ? <Check size={12} className="text-status-success" /> : <Copy size={12} />}
            {copied ? '已复制' : '复制'}
          </button>
          <span className="text-caption text-ink-faint tabular-nums">
            {filter ? `命中 ${matchCount} 处` : `${lines} 行`}
          </span>
        </div>
      )}

      <div className={cn('bg-inset rounded-lg p-3 overflow-auto font-mono text-data-sm', maxHeightClass)}>
        {view === 'tree' ? (
          <JsonNode
            key={`${treeKey}-${filter}`}
            name={rootName ?? null}
            value={value}
            depth={0}
            defaultExpandDepth={
              filter || expandMode === 'all' ? 99 : expandMode === 'collapsed' ? 1 : defaultExpandDepth
            }
            filter={filter.toLowerCase()}
            isLast
          />
        ) : (
          <pre className="whitespace-pre text-data-sm">{highlightJson(text)}</pre>
        )}
      </div>
    </div>
  )
}
