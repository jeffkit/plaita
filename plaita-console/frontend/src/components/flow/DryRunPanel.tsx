import { useRef, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { ChevronDown, ChevronRight, Pin, PinOff, Play } from 'lucide-react'
import { api, type DryRunNodeResult } from '../../services/api'
import { Button } from '../ui'
import SchemaInput from './schemaForm/SchemaInput'
import { buildTimelineRows } from './timeline'

interface DryRunPanelProps {
  flowJson: string
  /** 流程入参类型（引擎 Property 结构），驱动试跑输入的表单态 */
  inputType: unknown
  onClose: () => void
  nodesByType?: Record<string, string[]>
  onErrorNodeId?: (id: string) => void
  /** 试跑状态回写：开始时回调 []（清除上轮标红），结束后回调出错节点 id 列表（画布标红） */
  onRunStatus?: (erroredIds: string[]) => void
}

// 面板宽度约束与持久化（C5：拖拽调宽）
const PANEL_MIN = 320
const PANEL_MAX_RATIO = 0.6
const PANEL_DEFAULT = 384
const PANEL_WIDTH_KEY = 'plaita-dryrun-width'

// 试跑面板（2026-09 C 线重构）：
// - 输入：SchemaInput 表单 ⇄ JSON 双态（schema 来自流程 inputType）
// - 时间线：按 depth/flow_path 渲染子流程分组与缩进，组可折叠；
//   嵌套节点的固定/单跑禁用（调试变换仅作用于顶层，dryrun.py）
// - 宽度：左缘拖拽，持久化 localStorage
export default function DryRunPanel({
  flowJson,
  inputType,
  onClose,
  nodesByType,
  onErrorNodeId,
  onRunStatus,
}: DryRunPanelProps) {
  const [inputJson, setInputJson] = useState('{\n  "name": "plaita"\n}')
  const [inputError, setInputError] = useState<string | null>(null)
  const [pins, setPins] = useState<Record<string, unknown>>({})
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [onlyNode, setOnlyNode] = useState<string | null>(null)
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set())
  const [width, setWidth] = useState(() => {
    const saved = Number(localStorage.getItem(PANEL_WIDTH_KEY))
    return Number.isFinite(saved) && saved >= PANEL_MIN ? saved : PANEL_DEFAULT
  })
  const resizing = useRef(false)

  const mut = useMutation({
    mutationFn: async () => {
      let input: Record<string, unknown> = {}
      try {
        input = inputJson.trim() ? JSON.parse(inputJson) : {}
        setInputError(null)
      } catch (e) {
        throw new Error(`输入 JSON 非法: ${(e as Error).message}`)
      }
      onRunStatus?.([]) // 新一轮试跑开始：清除上轮画布错误标记
      const out = await api.dryRun({
        flowJson,
        input,
        pinned: Object.keys(pins).length ? pins : undefined,
        onlyNode: onlyNode ?? undefined,
      })
      // 出错节点回写画布标红（节点级 status=error 由采集回调给出）
      const erroredIds = (out.nodes ?? [])
        .filter((n) => n.status === 'error' && n.id != null)
        .map((n) => n.id as string)
      onRunStatus?.(erroredIds)
      return out
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
  const toggleGroup = (key: string) => {
    setCollapsedGroups((s) => {
      const next = new Set(s)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  // 拖拽调宽：pointer capture 跟手，松开持久化
  const onResizeStart = (e: React.PointerEvent) => {
    resizing.current = true
    e.currentTarget.setPointerCapture(e.pointerId)
  }
  const onResizeMove = (e: React.PointerEvent) => {
    if (!resizing.current) return
    const w = Math.min(
      Math.max(window.innerWidth - e.clientX, PANEL_MIN),
      Math.floor(window.innerWidth * PANEL_MAX_RATIO)
    )
    setWidth(w)
  }
  const onResizeEnd = () => {
    if (!resizing.current) return
    resizing.current = false
    localStorage.setItem(PANEL_WIDTH_KEY, String(width))
  }

  return (
    <div
      className="relative shrink-0 bg-surface border-l border-line p-4 overflow-y-auto text-body flex flex-col"
      style={{ width }}
      data-testid="dry-run-panel"
    >
      {/* 左缘拖拽热区 */}
      <div
        onPointerDown={onResizeStart}
        onPointerMove={onResizeMove}
        onPointerUp={onResizeEnd}
        className="absolute left-0 top-0 h-full w-1.5 cursor-col-resize hover:bg-plaita-400/40 transition-colors"
        title="拖拽调整宽度"
      />
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-section text-ink-primary">试跑</h3>
        <button onClick={onClose} className="text-ink-muted hover:text-ink-primary">✕</button>
      </div>

      <label className="text-caption text-ink-muted mb-1">输入参数</label>
      <div className="mb-2">
        <SchemaInput inputType={inputType} text={inputJson} onTextChange={setInputJson} />
      </div>

      <div className="flex gap-2 mb-3">
        <Button variant="primary" className="flex-1" onClick={runFull} disabled={mut.isPending}>
          {mut.isPending ? '执行中…' : onlyNode ? '完整试跑' : '开始试跑'}
        </Button>
        {onlyNode && (
          <Button variant="secondary" onClick={() => setOnlyNode(null)} title="取消「仅运行此节点」">
            取消单节点
          </Button>
        )}
      </div>

      {inputError && <p className="text-caption text-status-error mb-2">{inputError}</p>}
      {mut.isError && (() => {
        const msg = (mut.error as Error).message
        // "Flow 校验失败: 1 validation error for Assignment upstream_output ..."
        // → 解析出错节点类型，列出画布上的同名节点并支持一键打开配置
        const typeMatch = msg.match(/for ([A-Z]\w+)/)
        const nodeType = typeMatch
          ? typeMatch[1].replace(/([A-Z])/g, (c) => '_' + c.toLowerCase()).toLowerCase()
          : null
        const ids = (nodeType && nodesByType?.[nodeType]) || []
        return (
          <div className="mb-2 p-2 rounded-md border border-status-error/40 bg-status-error/10">
            <p className="text-caption text-status-error">{msg}</p>
            {nodeType && ids.length > 0 && (
              <div className="mt-1.5 text-caption text-ink-secondary">
                出错节点类型：<span className="font-mono text-data-sm">{nodeType}</span>
                ，画布上对应的节点：
                {ids.map((id) => (
                  <button
                    key={id}
                    className="mx-0.5 px-1 py-0.5 rounded-md border border-line bg-inset text-ink-primary hover:border-plaita-500 font-mono text-data-sm"
                    onClick={() => onErrorNodeId?.(id)}
                    title="点击打开该节点的配置"
                  >
                    {id}
                  </button>
                ))}
              </div>
            )}
          </div>
        )
      })()}
      {result?.error && (() => {
        // 执行级错误（如 "执行节点fetch出错了: ..."）除面板提示外，给出错节点
        // 提供画布定位入口；节点级错误已通过 onRunStatus 标红画布
        const msgMatch = result.error.match(/执行节点\s*(\S+?)\s*出错了/)
        const timelineErrIds = nodes
          .filter((n) => n.status === 'error' && n.id != null)
          .map((n) => n.id as string)
        const errIds = Array.from(new Set([...(msgMatch ? [msgMatch[1]] : []), ...timelineErrIds]))
        return (
          <div className="mb-2">
            <p className="text-caption text-status-error mb-1">{result.error}</p>
            {errIds.length > 0 && (
              <div className="text-caption text-ink-secondary">
                出错节点：
                {errIds.map((id) => (
                  <button
                    key={id}
                    className="mx-0.5 px-1 py-0.5 rounded-md border border-status-error/50 bg-status-error/10 text-status-error font-mono text-data-sm hover:border-status-error"
                    onClick={() => onErrorNodeId?.(id)}
                    title="点击定位到画布上的出错节点"
                  >
                    {id}
                  </button>
                ))}
                （画布上已标红）
              </div>
            )}
          </div>
        )
      })()}

      {Object.keys(pins).length > 0 && (
        <div className="mb-3 p-2 rounded-md bg-plaita-500/10 border border-plaita-500/40">
          <div className="text-caption text-plaita-400 mb-1">
            已固定 {Object.keys(pins).length} 个节点输出（试跑时跳过真实执行）
          </div>
          <div className="flex flex-wrap gap-1">
            {Object.keys(pins).map((id) => (
              <button
                key={id}
                onClick={() => unpinNode(id)}
                className="flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-inset border border-line text-[11px] text-ink-secondary hover:border-plaita-500"
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

      <div className="text-caption text-ink-muted mt-3 mb-1">节点执行时间线</div>
      <div className="space-y-1.5 flex-1">
        <TimelineRows
          nodes={nodes}
          expanded={expanded}
          setExpanded={setExpanded}
          collapsedGroups={collapsedGroups}
          toggleGroup={toggleGroup}
          pinNode={pinNode}
          runOnly={runOnly}
        />
        {nodes.length === 0 && <p className="text-caption text-ink-faint">无节点结果</p>}
      </div>
    </div>
  )
}

// ── 时间线：子流程分组 + 缩进 ────────────────────────────────────────────
// 分组纯函数（buildTimelineRows / GroupHeaderInfo / TimelineRow）在 ./timeline

function TimelineRows({
  nodes,
  expanded,
  setExpanded,
  collapsedGroups,
  toggleGroup,
  pinNode,
  runOnly,
}: {
  nodes: DryRunNodeResult[]
  expanded: Record<string, boolean>
  setExpanded: (fn: (s: Record<string, boolean>) => Record<string, boolean>) => void
  collapsedGroups: Set<string>
  toggleGroup: (key: string) => void
  pinNode: (n: DryRunNodeResult) => void
  runOnly: (id: string) => void
}) {
  const rows = buildTimelineRows(nodes, collapsedGroups)
  return (
    <>
      {rows.map((r, i) =>
        r.kind === 'group' ? (
          <button
            key={`g-${r.info.key}-${i}`}
            onClick={() => toggleGroup(r.info.key)}
            className="flex items-center gap-1 w-full text-left py-1 rounded-md hover:bg-elevated transition-colors"
            style={{ paddingLeft: 6 + r.info.indent * 14 }}
            title={collapsedGroups.has(r.info.key) ? '展开子流程' : '折叠子流程'}
          >
            {collapsedGroups.has(r.info.key) ? <ChevronRight size={11} /> : <ChevronDown size={11} />}
            <span className="font-mono text-[11px] text-plaita-400">{r.info.label}</span>
            <span className="text-[10px] text-ink-faint">子流程 · {r.info.count} 节点</span>
            {r.info.errored && <span className="w-1.5 h-1.5 rounded-full bg-status-error shrink-0" />}
          </button>
        ) : r.hidden ? null : (
          <NodeRow
            key={r.node.id || `n${i}`}
            n={r.node}
            depth={r.depth}
            expanded={Boolean(expanded[r.node.id || `n${i}`])}
            onToggle={() =>
              setExpanded((s) => ({ ...s, [r.node.id || `n${i}`]: !expanded[r.node.id || `n${i}`] }))
            }
            onPin={() => pinNode(r.node)}
            onRunOnly={() => r.node.id != null && runOnly(r.node.id)}
          />
        )
      )}
    </>
  )
}

// ── 节点行 ───────────────────────────────────────────────────────────────

function NodeRow({
  n,
  depth,
  expanded,
  onToggle,
  onPin,
  onRunOnly,
}: {
  n: DryRunNodeResult
  depth: number
  expanded: boolean
  onToggle: () => void
  onPin: () => void
  onRunOnly: () => void
}) {
  return (
    <div
      className={`p-2 rounded-md border text-caption ${
        n.status === 'error'
          ? 'bg-status-error/10 border-status-error/50'
          : n.type === 'mock'
            ? 'bg-inset border-plaita-500/40'
            : 'bg-inset border-line'
      }`}
      style={{ marginLeft: depth * 14 }}
    >
      <div className="flex items-center justify-between gap-1">
        <button className="flex items-center gap-1 min-w-0" onClick={onToggle} title={expanded ? '收起输入/输出' : '展开输入/输出'}>
          {expanded ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
          <span className="font-mono text-data-sm text-ink-primary truncate">{n.name || n.id}</span>
          {n.type === 'mock' && (
            <span className="text-[10px] text-plaita-400 border border-plaita-500/50 rounded-md px-1">mock</span>
          )}
        </button>
        <div className="flex items-center gap-1 shrink-0">
          {depth > 0 ? (
            <>
              <Pin size={11} className="text-ink-faint" />
              <Play size={11} className="text-ink-faint" />
              <span className="text-[10px] text-ink-faint" title="子流程内节点暂不支持固定/单跑（调试变换仅作用于顶层）">
                顶层限定
              </span>
            </>
          ) : (
            <>
              <span className="text-ink-muted">{n.type}</span>
              {n.output !== undefined && n.output !== null && n.type !== 'mock' && (
                <button onClick={onPin} className="text-ink-muted hover:text-plaita-400" title="固定此输出：后续试跑跳过该节点真实执行">
                  <Pin size={11} />
                </button>
              )}
              {n.type !== 'mock' && n.type !== 'start' && (
                <button onClick={onRunOnly} className="text-ink-muted hover:text-plaita-400" title="仅运行此节点（上游取固定值，下游 mock 无副作用）">
                  <Play size={11} />
                </button>
              )}
            </>
          )}
        </div>
      </div>
      {expanded ? (
        <div className="mt-1 space-y-1">
          <NodeIOTree title="input" value={n.input} />
          <NodeIOTree title="output" value={n.output} tone="green" />
        </div>
      ) : (
        <>
          {n.input !== undefined && n.input !== null && (
            <div className="mt-1 text-ink-muted">in: <span className="text-ink-secondary">{short(n.input)}</span></div>
          )}
          {n.output !== undefined && n.output !== null && (
            <div className="mt-0.5 text-ink-muted">out: <span className="text-status-success">{short(n.output)}</span></div>
          )}
        </>
      )}
      {n.error && <div className="mt-1 text-status-error">{n.error}</div>}
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
  const toneCls = tone === 'green' ? 'text-status-success' : 'text-ink-secondary'
  return (
    <div>
      <button className="text-ink-muted flex items-center gap-0.5" onClick={() => setOpen((v) => !v)}>
        {open ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
        {title}
      </button>
      {open ? (
        <pre className={`mt-0.5 text-data-sm leading-4 whitespace-pre-wrap break-all bg-canvas rounded-md p-1.5 border border-line font-mono ${toneCls}`}>
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
