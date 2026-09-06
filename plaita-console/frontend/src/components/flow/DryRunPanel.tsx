import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, Pin, PinOff, Play } from 'lucide-react'
import { api, type DryRunNodeResult } from '../../services/api'

interface DryRunPanelProps {
  flowJson: string
  onClose: () => void
}

// 试跑面板：输入 JSON → 调 /api/flows/dry-run → 节点级结果时间线。
// 调试能力：节点输出可「固定」（pin），后续试跑跳过真实执行；
// 每个节点可「仅运行此节点」（上游取 pin 值，下游 mock 无副作用）。
export default function DryRunPanel({ flowJson, onClose, nodesByType, onErrorNodeId }: DryRunPanelProps & {
  nodesByType?: Record<string, string[]>
  onErrorNodeId?: (id: string) => void
}) {
  const [inputJson, setInputJson] = useState('{\n  "name": "plaita"\n}')
  const [inputError, setInputError] = useState<string | null>(null)
  const [pins, setPins] = useState<Record<string, unknown>>({})
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [onlyNode, setOnlyNode] = useState<string | null>(null)

  const mut = useMutation({
    mutationFn: async () => {
      let input: Record<string, unknown> = {}
      try {
        input = inputJson.trim() ? JSON.parse(inputJson) : {}
        setInputError(null)
      } catch (e) {
        throw new Error(`输入 JSON 非法: ${(e as Error).message}`)
      }
      return api.dryRun({
        flowJson,
        input,
        pinned: Object.keys(pins).length ? pins : undefined,
        onlyNode: onlyNode ?? undefined,
      })
    },
  })

  const result = mut.data
  const nodes: DryRunNodeResult[] = result?.nodes || []

  const pinNode = (n: DryRunNodeResult) => {
    if (n.id == null) return
    setPins((p) => ({ ...p, [n.id as string]: n.output ?? null }))
  }
  const unpinNode = (id: string) => {
    setPins((p) => {
      const next = { ...p }
      delete next[id]
      return next
    })
  }
  const runOnly = (id: string) => {
    setOnlyNode(id)
    mut.mutate()
  }
  const runFull = () => {
    setOnlyNode(null)
    mut.mutate()
  }

  return (
    <div className="w-96 bg-dark-900/95 border-l border-dark-700 p-4 overflow-y-auto text-sm flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-dark-100">试跑</h3>
        <button onClick={onClose} className="text-dark-400 hover:text-dark-100">✕</button>
      </div>

      <label className="text-xs text-dark-400 mb-1">输入参数（JSON）</label>
      <textarea
        value={inputJson}
        onChange={(e) => setInputJson(e.target.value)}
        rows={5}
        className="input w-full font-mono text-xs mb-2"
      />

      <div className="flex gap-2 mb-3">
        <button
          onClick={runFull}
          disabled={mut.isPending}
          className="flex-1 bg-plaita-600 hover:bg-plaita-500 disabled:opacity-50 text-white py-1.5 rounded text-xs"
        >
          {mut.isPending ? '执行中…' : onlyNode ? '完整试跑' : '开始试跑'}
        </button>
        {onlyNode && (
          <button
            onClick={() => setOnlyNode(null)}
            className="px-2 text-xs text-dark-400 hover:text-dark-100 border border-dark-700 rounded"
            title="取消「仅运行此节点」"
          >
            取消单节点
          </button>
        )}
      </div>

      {inputError && <p className="text-xs text-red-400 mb-2">{inputError}</p>}
      {mut.isError && (() => {
        const msg = (mut.error as Error).message
        // "Flow 校验失败: 1 validation error for Assignment upstream_output ..."
        // → 解析出错节点类型，列出画布上的同名节点并支持一键打开配置
        // （2026-09 UI 旅程评审：裸 pydantic 文案让用户无从定位）
        const typeMatch = msg.match(/for ([A-Z]\w+)/)
        const nodeType = typeMatch
          ? typeMatch[1].replace(/([A-Z])/g, (c) => '_' + c.toLowerCase()).toLowerCase()
          : null
        const ids = (nodeType && nodesByType?.[nodeType]) || []
        return (
          <div className="mb-2 p-2 rounded border border-red-500/40 bg-red-500/10">
            <p className="text-xs text-red-400">{msg}</p>
            {nodeType && ids.length > 0 && (
              <div className="mt-1.5 text-[11px] text-ink-secondary">
                出错节点类型：<span className="font-mono">{nodeType}</span>
                {ids.length > 0 && <>，画布上对应的节点：
                  {ids.map((id) => (
                    <button
                      key={id}
                      className="mx-0.5 px-1 py-0.5 rounded border border-line bg-dark-800 text-dark-100 hover:border-plaita-500 font-mono"
                      onClick={() => onErrorNodeId?.(id)}
                      title="点击打开该节点的配置"
                    >
                      {id}
                    </button>
                  ))}
                </>}
              </div>
            )}
          </div>
        )
      })()}
      {result?.error && <p className="text-xs text-red-400 mb-2">{result.error}</p>}

      {Object.keys(pins).length > 0 && (
        <div className="mb-3 p-2 rounded bg-plaita-600/10 border border-plaita-600/40">
          <div className="text-xs text-plaita-600 dark:text-plaita-600 dark:text-plaita-300 mb-1">
            已固定 {Object.keys(pins).length} 个节点输出（试跑时跳过真实执行）
          </div>
          <div className="flex flex-wrap gap-1">
            {Object.keys(pins).map((id) => (
              <button
                key={id}
                onClick={() => unpinNode(id)}
                className="flex items-center gap-1 px-1.5 py-0.5 rounded bg-dark-800 border border-dark-700 text-[11px] text-dark-200 hover:border-plaita-500"
                title="取消固定"
              >
                <PinOff size={10} />
                {id}
              </button>
            ))}
          </div>
        </div>
      )}

      {result && !result.error && (
        <NodeIOTree title="最终结果" value={result.result} tone="green" defaultOpen />
      )}

      <div className="text-xs text-dark-400 mt-3 mb-1">节点执行时间线</div>
      <div className="space-y-2 flex-1">
        {nodes.map((n, i) => {
          const key = n.id || `n${i}`
          const open = !!expanded[key]
          return (
            <div
              key={key}
              className={`p-2 rounded border text-xs ${
                n.status === 'error'
                  ? 'bg-red-600/15 border-red-600/50'
                  : n.type === 'mock'
                    ? 'bg-dark-800 border-plaita-600/40'
                    : 'bg-dark-800 border-dark-700'
              }`}
            >
              <div className="flex items-center justify-between gap-1">
                <button
                  className="flex items-center gap-1 min-w-0"
                  onClick={() => setExpanded((s) => ({ ...s, [key]: !open }))}
                  title={open ? '收起输入/输出' : '展开输入/输出'}
                >
                  {open ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
                  <span className="font-mono text-dark-100 truncate">{n.name || n.id}</span>
                  {n.type === 'mock' && (
                    <span className="text-[10px] text-plaita-600 dark:text-plaita-300 border border-plaita-600/50 rounded px-1">
                      mock
                    </span>
                  )}
                </button>
                <div className="flex items-center gap-1 shrink-0">
                  <span className="text-dark-400">{n.type}</span>
                  {n.output !== undefined && n.output !== null && n.type !== 'mock' && (
                    <button
                      onClick={() => pinNode(n)}
                      className="text-dark-400 hover:text-plaita-300"
                      title="固定此输出：后续试跑跳过该节点真实执行"
                    >
                      <Pin size={11} />
                    </button>
                  )}
                  {n.type !== 'mock' && n.type !== 'start' && (
                    <button
                      onClick={() => runOnly(n.id as string)}
                      className="text-dark-400 hover:text-plaita-300"
                      title="仅运行此节点（上游取固定值，下游 mock 无副作用）"
                    >
                      <Play size={11} />
                    </button>
                  )}
                </div>
              </div>
              {open ? (
                <div className="mt-1 space-y-1">
                  <NodeIOTree title="input" value={n.input} />
                  <NodeIOTree title="output" value={n.output} tone="green" />
                </div>
              ) : (
                <>
                  {n.input !== undefined && n.input !== null && (
                    <div className="mt-1 text-dark-400">in: <span className="text-dark-200">{short(n.input)}</span></div>
                  )}
                  {n.output !== undefined && n.output !== null && (
                    <div className="mt-0.5 text-dark-400">out: <span className="text-green-300">{short(n.output)}</span></div>
                  )}
                </>
              )}
              {n.error && <div className="mt-1 text-red-300">{n.error}</div>}
            </div>
          )
        })}
        {nodes.length === 0 && <p className="text-dark-400">无节点结果</p>}
      </div>
    </div>
  )
}

/** 可展开的输入/输出检视器：完整 JSON 缩进展示，不再截断 */
function NodeIOTree({
  title,
  value,
  tone = 'default',
  defaultOpen = false,
}: {
  title: string
  value: unknown
  tone?: 'default' | 'green'
  defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  if (value === undefined || value === null) return null
  const text = JSON.stringify(value, null, 2)
  // 亮色主题下 green-300 过淡不可读：dark 用亮绿、light 用深绿
  const toneCls = tone === 'green' ? 'text-green-700 dark:text-green-300' : 'text-dark-200'
  return (
    <div>
      <button className="text-dark-400 flex items-center gap-0.5" onClick={() => setOpen((v) => !v)}>
        {open ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
        {title}
      </button>
      {open ? (
        <pre className={`mt-0.5 text-[11px] leading-4 whitespace-pre-wrap break-all bg-dark-900/60 rounded p-1.5 border border-dark-700 ${toneCls}`}>
          {text}
        </pre>
      ) : (
        <span className={`ml-1 ${toneCls}`}>{short(value)}</span>
      )}
    </div>
  )
}

function short(v: unknown): string {
  const s = typeof v === 'string' ? v : JSON.stringify(v)
  return s.length > 80 ? s.slice(0, 80) + '…' : s
}
