/* eslint-disable react-refresh/only-export-components */
import { memo, useMemo } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { useFlowEditor } from '../../stores/flowEditor'

// 节点类型配置：图标与配色
export const nodeTypeConfig: Record<string, { shape: string; color: string; icon: string }> = {
  start: { shape: 'circle', color: 'plaita', icon: '▶' },
  end: { shape: 'circle', color: 'blue', icon: '■' },
  code: { shape: 'rect', color: 'purple', icon: '{ }' },
  http: { shape: 'rect', color: 'orange', icon: '🌐' },
  switch: { shape: 'diamond', color: 'yellow', icon: '?' },
  if: { shape: 'diamond', color: 'yellow', icon: '?' },
  case: { shape: 'diamond', color: 'yellow', icon: '?' },
  loop: { shape: 'rect', color: 'cyan', icon: '↻' },
  map: { shape: 'rect', color: 'cyan', icon: '↻' },
  filter: { shape: 'rect', color: 'cyan', icon: '↻' },
  find: { shape: 'rect', color: 'cyan', icon: '↻' },
  reduce: { shape: 'rect', color: 'cyan', icon: '↻' },
  delay: { shape: 'rect', color: 'pink', icon: '⏱' },
  approval: { shape: 'rect', color: 'green', icon: '✓' },
  event: { shape: 'rect', color: 'red', icon: '⚡' },
  assignment: { shape: 'rect', color: 'gray', icon: '=' },
  calculate: { shape: 'rect', color: 'indigo', icon: '∑' },
  child: { shape: 'rect', color: 'teal', icon: '↳' },
  reference: { shape: 'rect', color: 'teal', icon: '↳' },
  parallel: { shape: 'rect', color: 'amber', icon: '⇉' },
}

// ── 业务节点族规则（plaita-nodes 通用节点 + mediaflow 业务节点）──────────
// 每族一个色系与代表图标；命中即获得独立的左边条与徽标配色。
interface FamilyRule {
  test: RegExp
  color: string
  icon: string
  label: string
}

export const NODE_FAMILIES: FamilyRule[] = [
  { test: /(^|_)agent(run)?($|_)/, color: 'violet', icon: '🤖', label: 'Agent' },
  { test: /(capture|require_ok|^cap_json|spawn)/, color: 'sky', icon: '⌨️', label: '命令执行' },
  { test: /(hitl|approval|confirm$)/, color: 'amber', icon: '🙋', label: '人工确认' },
  { test: /(notify|report$)/, color: 'emerald', icon: '📣', label: '通知' },
  { test: /(writefile|write_file|save_draft)/, color: 'slate', icon: '📝', label: '落盘' },
  { test: /(^|_)pool(_|$)|breaker|mark_dropped/, color: 'teal', icon: '🗃', label: '内容池' },
  { test: /(metrics|insights|summar|aggregate|snapshot)/, color: 'cyan', icon: '📈', label: '数据闭环' },
  { test: /(parse|extract|verdict|json|merge_brief|resolve_|first_non_null|wechat_article)/,
    color: 'indigo', icon: '⚖️', label: '解析判定' },
  { test: /^(tw_|twitter)/, color: 'indigo', icon: '🐦', label: 'Twitter' },
  { test: /^(xhs|wechat)/, color: 'rose', icon: '📕', label: '平台发布' },
  { test: /(ctx|context|build_items|prompt$)/, color: 'plaita', icon: '🧰', label: '流程上下文' },
  { test: /(video|minimax|tts|compose)/, color: 'fuchsia', icon: '🎬', label: '视频生产' },
]

// 12 色轮换兜底：未知类型也能得到稳定且相互区分的配色
const FALLBACK_COLORS = ['violet', 'sky', 'amber', 'teal', 'rose', 'indigo', 'cyan',
  'emerald', 'fuchsia', 'slate']

function hashColor(type: string): string {
  let h = 0
  for (let i = 0; i < type.length; i++) h = (h * 31 + type.charCodeAt(i)) | 0
  return FALLBACK_COLORS[Math.abs(h) % FALLBACK_COLORS.length]
}

/** 解析节点的视觉配置：内置精确表 → 族规则 → hash 色带兜底。 */
export function resolveNodeTypeConfig(type: string): {
  shape: string; color: string; icon: string; family?: string
} {
  const exact = nodeTypeConfig[type]
  if (exact) return { ...exact, family: undefined }
  for (const rule of NODE_FAMILIES) {
    if (rule.test.test(type)) return { shape: 'rect', color: rule.color, icon: rule.icon, family: rule.label }
  }
  return { shape: 'rect', color: hashColor(type), icon: '◆' }
}

// 类型面色板（左色条 + 图标 chip 底；标准 Tailwind 名称，构建期可静态提取）
const COLOR_STYLES: Record<string, { bar: string; chipBg: string }> = {
  violet:  { bar: 'bg-violet-500',  chipBg: 'bg-violet-500/15' },
  sky:     { bar: 'bg-sky-500',     chipBg: 'bg-sky-500/15' },
  amber:   { bar: 'bg-amber-500',   chipBg: 'bg-amber-500/15' },
  emerald: { bar: 'bg-emerald-500', chipBg: 'bg-emerald-500/15' },
  slate:   { bar: 'bg-slate-400',   chipBg: 'bg-slate-400/15' },
  teal:    { bar: 'bg-teal-500',    chipBg: 'bg-teal-500/15' },
  cyan:    { bar: 'bg-cyan-500',    chipBg: 'bg-cyan-500/15' },
  indigo:  { bar: 'bg-indigo-500',  chipBg: 'bg-indigo-500/15' },
  rose:    { bar: 'bg-rose-500',    chipBg: 'bg-rose-500/15' },
  fuchsia: { bar: 'bg-fuchsia-500', chipBg: 'bg-fuchsia-500/15' },
  purple:  { bar: 'bg-purple-500',  chipBg: 'bg-purple-500/15' },
  orange:  { bar: 'bg-orange-500',  chipBg: 'bg-orange-500/15' },
  yellow:  { bar: 'bg-yellow-500',  chipBg: 'bg-yellow-500/15' },
  red:     { bar: 'bg-red-500',     chipBg: 'bg-red-500/15' },
  green:   { bar: 'bg-green-500',   chipBg: 'bg-green-500/15' },
  pink:    { bar: 'bg-pink-500',    chipBg: 'bg-pink-500/15' },
  gray:    { bar: 'bg-gray-400',    chipBg: 'bg-gray-400/15' },
  plaita:  { bar: 'bg-plaita-500',  chipBg: 'bg-plaita-500/15' },
  blue:    { bar: 'bg-blue-500',    chipBg: 'bg-blue-500/15' },
}

export type NodeStatus = 'executed' | 'current' | 'suspended' | 'pending' | 'error' | 'idle'

// 节点状态只表达在底色/描边上，节点名保持 ink-primary（DESIGN.md §5）
export const statusStyles: Record<NodeStatus, { bg: string; border: string }> = {
  executed: { bg: 'bg-status-success-dim', border: 'border-status-success/40' },
  current: { bg: 'bg-status-running-dim', border: 'border-status-running/50' },
  suspended: { bg: 'bg-status-warning-dim', border: 'border-status-warning/40' },
  pending: { bg: 'bg-inset', border: 'border-line' },
  error: { bg: 'bg-status-error-dim', border: 'border-status-error/40' },
  idle: { bg: 'bg-surface', border: 'border-line' },
}

export interface NodeLabelData {
  type: string
  name: string
  status: NodeStatus
  /** 人类可读描述（@flow 编译产物自带）——仅悬停 tooltip，不上画布 */
  desc?: string
  /** @flow 源码行号；name 缺失（旧 flow 的 _n 节点）时用于可读兜底 */
  sourceLine?: number
  /** 类型特定字段（副标题参数摘要的数据源） */
  fields?: Record<string, unknown>
  /** if 真分支目标（副标题显示去向） */
  next?: string
  /** if 假分支目标 */
  elseNext?: string
  /** switch/case 分支去向（handle=分支名 → 画布边 target）：jsonToFlow 把
   *  fields.branches 的 next 剥掉由画布边推导，副标题需从边反查（MC3-③） */
  branchTargets?: Record<string, string>
  /** 方案 A：展开/收拢容器（编辑器画布传入；FlowViewer 不传则不渲染按钮） */
  onToggleExpand?: () => void
  /** 容器形态标记与尺寸（展开时由画布传入） */
  expanded?: boolean
  containerW?: number
  containerH?: number
}

/** 副标题截断（副标题 10px mono，约为 10 字符宽 ×2） */
function sub(v: unknown, n = 22): string {
  if (typeof v !== 'string') return ''
  return v.length > n ? v.slice(0, n) + '…' : v
}

/**
 * 第二行 = 类型相关的高价值参数摘要（类型辨识交给图标/色条，不再重复）。
 * 逐类型定制；无话可说返回空串（第二行不渲染）。
 */
export function renderSubtitle(
  type: string,
  fields: Record<string, unknown> = {},
  next?: string,
  elseNext?: string,
  branchTargets?: Record<string, string>,
): string {
  if (type === 'if') {
    const parts: string[] = []
    if (next) parts.push('✓ ' + sub(next, 14))
    if (elseNext) parts.push('✗ ' + sub(elseNext, 14))
    return parts.join('   ')
  }
  if (type === 'switch' || type === 'case') {
    const bs = (fields.branches as Array<{ name?: string; next?: string }>) || []
    // 分支去向：优先画布边反查（fields.branches 被 jsonToFlow 剥掉 next，
    // MC3-③），无边信息时退回 IR 原值（FlowViewer 等直读 IR 的场景）
    return bs.map(b => {
      const target = (b.name && branchTargets?.[b.name]) || b.next || ''
      return `${sub(b.name, 8)}→${sub(target, 12)}`
    }).join('  ')
  }
  if (type === 'assignment') return sub(fields.output, 28)
  if (type === 'http' || type === 'generic_webhook') return sub(fields.url, 28)
  if (['map', 'filter', 'find', 'loop', 'reduce'].includes(type)) {
    // 存量实例可能带 camelCase 别名键（引擎 validator 兼容接受），一并识别（MC3-②）
    const cf = (fields.child_flow || fields.childFlow) as { nodes?: unknown[] } | undefined
    const bodyN = cf?.nodes ? Math.max(0, cf.nodes.length - 1) : undefined
    const bits: string[] = []
    const coll = sub(fields.collection, 18)
    if (coll) bits.push('× ' + coll)
    if (bodyN !== undefined) bits.push(`体 ${bodyN}`)
    if (fields.concurrent === true) bits.push('并发')
    return bits.join(' · ')
  }
  if (type === 'while') {
    const cf = (fields.child_flow || fields.childFlow) as { nodes?: unknown[] } | undefined
    if (cf?.nodes) return `体 ${Math.max(0, cf.nodes.length - 1)} · ≤${(fields.max_iterations as number) || 1000} 轮`
    return ''
  }
  // 业务节点通用：常见关键参数白名单，取第一个有值者
  for (const k of ['agent', 'model', 'path', 'to', 'channel', 'topic', 'sql', 'webhook', 'library', 'flow_id']) {
    const v = sub(fields[k], 24)
    if (v) return `${k}=${v}`
  }
  return ''
}

/** 子流程迷你带 chip：体节点逐个（跳过 start），while 尾部加回条件标记 */
export function subflowBandChips(fields: Record<string, unknown> = {}): Array<{ icon: string; label: string; title: string }> {
  const cf = (fields.childFlow || fields.child_flow) as { nodes?: Array<Record<string, unknown>> } | undefined
  const nodes = (cf?.nodes || []).filter(n => n.type !== 'start')
  if (!nodes.length) return []
  const chips = nodes.slice(0, 4).map(n => {
    const cfg = resolveNodeTypeConfig(String(n.type ?? ''))
    const label = String(n.name || n.id || '')
    return { icon: cfg.icon, label: label.length > 12 ? label.slice(0, 12) + '…' : label, title: label }
  })
  if (nodes.length > 4) chips.push({ icon: '⋯', label: `+${nodes.length - 4}`, title: `其余 ${nodes.length - 4} 个节点` })
  if (String(nodes[0]?.type) === 'end' || type_is_while(nodes)) chips.push({ icon: '↺', label: '回条件', title: '每轮回到循环条件' })
  return chips
}
// while 体的 return 即下一轮 item——带尾加「回条件」标记
function type_is_while(nodes: Array<Record<string, unknown>>): boolean {
  return nodes.length > 0 && nodes.every(n => n.type === 'end')
}

export function renderNodeLabel({ type, name, status, desc, sourceLine, fields, next, elseNext, branchTargets, onBandClick, onToggleExpand, expanded, containerW, containerH }: NodeLabelData & { onBandClick?: () => void }) {
  const cfg = resolveNodeTypeConfig(type)
  const style = statusStyles[status] ?? statusStyles.idle
  const cs = COLOR_STYLES[cfg.color] ?? COLOR_STYLES.gray

  // ---- 方案 A：容器形态（expanded）——虚线框 + 紧凑标题栏 + 收拢钮 ----
  // 子节点由 React Flow 以 parentId 挂载，坐标相对容器，铺在标题栏下方。
  if (expanded) {
    const headerName = name.length > 24 ? name.slice(0, 24) + '…' : name
    return (
      <div
        className="w-full h-full rounded-xl border-2 border-dashed border-plaita-500/60 bg-canvas/50 overflow-hidden"
        title={desc ? `${desc} · 容器 ${containerW ?? '?'}×${containerH ?? '?'}` : undefined}
      >
        <div className={`flex items-center gap-2 px-3 py-2 border-b border-dashed border-plaita-500/40 ${style.bg}`}>
          <span className={`w-6 h-6 flex items-center justify-center rounded-md ${cs.chipBg} text-[13px] shrink-0`}>{cfg.icon}</span>
          <div className="min-w-0 flex-1">
            <div className="font-mono text-[13px] leading-4 font-medium truncate text-ink-primary">{headerName}</div>
            {renderSubtitle(type, fields, next, elseNext, branchTargets) && (
              <div className="text-[10px] leading-tight font-mono text-ink-faint truncate">
                {renderSubtitle(type, fields, next, elseNext, branchTargets)}
              </div>
            )}
          </div>
          {onToggleExpand && (
            <button
              onClick={(e) => { e.stopPropagation(); onToggleExpand() }}
              className="shrink-0 rounded px-1.5 py-0.5 text-[10px] font-mono text-plaita-400 hover:bg-plaita-500/10"
              title="收拢子流程"
            >
              [−]
            </button>
          )}
        </div>
      </div>
    )
  }
  // 渲染层兜底：name 缺失（多为旧 flow 的 _n{id} 节点）显示「类型 · L行号」，
  // 不裸显合成 id；不回写 data.name，避免编辑保存时污染 IR。
  const fallbackName = sourceLine ? `${type} · L${sourceLine}` : name
  const label = /^_n\d+$/.test(name) ? fallbackName : name
  // 截断阈值与卡片 max-w-[240px] / 布局 NODE_WIDTH=240 对齐（24 字符 mono）
  const displayName = label.length > 24 ? label.slice(0, 24) + '…' : label
  // 第二行 = 参数摘要（类型辨识交给图标/色条）；无摘要则不渲染该行
  const secondLine = renderSubtitle(type, fields, next, elseNext, branchTargets)
  return (
    <div className={`relative px-3 py-2 rounded-lg border shadow-card ${style.bg} ${style.border} min-w-[140px] max-w-[240px] overflow-hidden`} title={desc || undefined}>
      {/* 族别左色条：一眼区分节点类别 */}
      <span className={`absolute left-0 top-0 bottom-0 w-1 ${cs.bar}`} />
      <div className="flex items-center gap-2">
        <span className={`w-6 h-6 flex items-center justify-center rounded-md ${cs.chipBg} text-[13px] shrink-0`}>{cfg.icon}</span>
        <div className="min-w-0">
          {/* 节点名 = 数据声道（mono，DESIGN.md §1） */}
          <div className="font-mono text-[13px] leading-4 font-medium truncate text-ink-primary">{displayName}</div>
          {secondLine && (
            <div className="text-[10px] leading-tight font-mono text-ink-faint truncate">
              {secondLine}
            </div>
          )}
        </div>
      </div>
      {(() => {
        // 子流程迷你带（方案 B）：循环族卡片第三行，只读拓扑概览；
        // onBandClick 由编辑器画布传入（点击整条带进入子图编辑），FlowViewer 只读
        if (!fields) return null
        const chips = subflowBandChips(fields)
        if (!chips.length) return null
        return (
          <div
            className={`-mx-3 -mb-2 mt-1.5 flex items-center gap-1 border-t px-2 py-1 bg-inset/60 overflow-hidden ${onBandClick ? 'cursor-pointer hover:bg-inset' : 'pointer-events-none'}`}
            onClick={onBandClick ? (e) => { e.stopPropagation(); onBandClick() } : undefined}
            title="子流程概览（点击进入子图编辑）"
          >
            {chips.map((c, i) => (
              <span key={i} className="flex items-center gap-1 shrink-0" title={c.title}>
                {i > 0 && <span className="text-dark-500 text-[9px]">─▶</span>}
                <span className="rounded bg-dark-800/70 px-1 py-px text-[9px] font-mono text-ink-secondary whitespace-nowrap">
                  {c.icon} {c.label}
                </span>
              </span>
            ))}
          </div>
        )
      })()}
      {onToggleExpand && (
        <button
          onClick={(e) => { e.stopPropagation(); onToggleExpand() }}
          className="absolute top-1 right-1 rounded px-1 text-[10px] font-mono text-plaita-400/80 hover:text-plaita-400 hover:bg-plaita-500/10"
          title="展开子流程（原位容器，编辑写入 childFlow）"
        >
          [⊞]
        </button>
      )}
      {status === 'executed' && (
        <span className="absolute top-1 right-6 text-status-success text-xs">✓</span>
      )}
      {status === 'current' && (
        <span className={`absolute top-1.5 ${onToggleExpand ? 'right-6' : 'right-1.5'} w-1.5 h-1.5 rounded-full bg-status-running animate-breathe`} />
      )}
      {status === 'error' && (
        <span className={`absolute top-1 ${onToggleExpand ? 'right-6' : 'right-1.5'} text-status-error text-xs`}>!</span>
      )}
    </div>
  )
}

// 编辑器用的自定义 xyflow 节点：带输入/输出 Handle，可连线
export interface PlaitaNodeData {
  type: string
  name: string
  status?: NodeStatus
  desc?: string
  sourceLine?: number
  [key: string]: unknown
}

function PlaitaNodeComponent({ data, selected, id }: NodeProps) {
  const d = data as PlaitaNodeData
  // switch/case 分支去向：fields.branches 的 next 被 jsonToFlow 剥掉，从画布
  // 出边反查（handle=分支名 → target）。订阅边数组在边变化时才触发本节点重渲染
  const edges = useFlowEditor((s) => s.edges)
  const branchTargets = useMemo(() => {
    const out: Record<string, string> = {}
    for (const e of edges) {
      if (e.source !== id || !e.sourceHandle) continue
      if (e.sourceHandle === 'true' || e.sourceHandle === 'false') continue
      out[e.sourceHandle] = String(e.target)
    }
    return out
  }, [edges, id])
  const isContainerChild = d.isContainerChild === true
  return (
    <div className={`relative ${d.expanded ? 'w-full h-full' : ''} ${selected ? 'ring-2 ring-plaita-400/80 rounded-lg' : ''}`}>
      <Handle type="target" position={Position.Top} id="in" className="!bg-plaita-500 !w-2.5 !h-2.5 !border-2 !border-canvas" />
      {renderNodeLabel({
        type: d.type, name: d.name, status: d.status ?? 'idle', desc: d.desc, sourceLine: d.sourceLine,
        fields: d.fields as Record<string, unknown> | undefined, next: d.next as string | undefined, elseNext: d.elseNext as string | undefined,
        branchTargets,
        onBandClick: () => useFlowEditor.getState().enterSubgraph(id, 'child_flow'),
        // 方案 Y（MC1）：容器子节点不再渲染 [⊞]——嵌套原位展开的写回极易丢层，
        // 嵌套体编辑统一走「进入子图编辑」（面包屑逐层进出，天然单层写回）
        onToggleExpand: !isContainerChild &&
          (d.fields && ((d.fields as Record<string, unknown>).childFlow || (d.fields as Record<string, unknown>).child_flow))
          ? () => useFlowEditor.getState().toggleSubflowExpanded(id)
          : undefined,
        expanded: d.expanded as boolean | undefined,
        containerW: d.containerW as number | undefined,
        containerH: d.containerH as number | undefined,
      })}
      <Handle type="source" position={Position.Bottom} id="true" className="!bg-plaita-500 !w-2.5 !h-2.5 !border-2 !border-canvas" />
      <Handle type="source" position={Position.Right} id="false" className="!bg-dark-400 !w-2.5 !h-2.5 !border-2 !border-canvas" />
    </div>
  )
}

export const PlaitaNode = memo(PlaitaNodeComponent)

// 供编辑器使用的 nodeTypes 映射
export const editorNodeTypes = { plaitaNode: PlaitaNode }
