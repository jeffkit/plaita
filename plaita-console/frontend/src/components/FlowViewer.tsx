import { useMemo } from 'react'
import { ReactFlow, Node, Edge, Background, Controls, MarkerType } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { ArrowRight, Loader2 } from 'lucide-react'
import { renderNodeLabel, type NodeStatus } from './flow/nodeTypes'
import { EDGE_COLOR, EDGE_TYPE, NODE_WIDTH } from './flow/flowLayout'
import { jsonToFlow } from './flow/flowConverter'
import { definitionOrder, isRoutingNodeId, nodeOutgoing } from './flow/flowDefinition'
import { STATUS_CHIP } from './flow/nodeStatusStyles'
import type { FlowDefinitionState } from '../hooks/useFlowDefinition'
import { cn } from './ui'

interface FlowViewerProps {
  context: Record<string, unknown>
  status: string
  /** 流程定义状态（与节点时间线共用同一次查询） */
  flowDef: FlowDefinitionState
}

/** 已执行边（两端都已跑过）：成功色，与执行态节点呼应 */
const EXECUTED_EDGE_COLOR = 'rgb(var(--c-status-success))'
const ERROR_EDGE_COLOR = 'rgb(var(--c-status-error))'

/**
 * 环绕式布局（只读视图专用）。
 *
 * 编辑器/`symmetricLayout` 是「一层一行」的纵向展开：线性流程 13 个节点会摊成
 * 1600px 高的细长一列，塞进卡片后被 fitView 缩成一条线。执行详情页的空间是
 * 「宽而矮」，所以这里按执行顺序横向铺开、满一行换行（蛇形），既连贯又可用。
 * 流程定义自带 layout 坐标时不动它（尊重编辑器里的人工排布）。
 */
function wrapLayout(nodes: Node[], orderIds: string[], perColumn: number): Node[] {
  const order = orderIds.length > 0 ? orderIds : nodes.map((n) => n.id)
  const index = new Map(order.map((id, i) => [id, i]))
  let next = order.length
  for (const n of nodes) if (!index.has(n.id)) index.set(n.id, next++)

  const rows = Math.max(2, Math.min(perColumn, Math.ceil(Math.sqrt(order.length + 1))))
  return nodes.map((n) => {
    const i = index.get(n.id) ?? 0
    const col = Math.floor(i / rows)
    const raw = i % rows
    const row = col % 2 === 0 ? raw : rows - 1 - raw // 蛇形折返，减少长跨行连线
    return { ...n, position: { x: col * (NODE_WIDTH + 90), y: row * 104 } }
  })
}

export default function FlowViewer({ context, status, flowDef }: FlowViewerProps) {
  const isFailed = status === 'error' || status === 'failed'

  const view = useMemo(() => {
    /** $NODE 的键顺序 = 真实执行先后（引擎按执行顺序写入） */
    const nodeMap = context.$NODE
    const resultIds =
      nodeMap && typeof nodeMap === 'object' ? Object.keys(nodeMap as Record<string, unknown>) : []
    const executed = new Set(resultIds)
    const lastNodeId =
      (typeof context.$LAST_NODE === 'string' ? (context.$LAST_NODE as string) : undefined) ??
      resultIds[resultIds.length - 1]

    const statusFor = (id: string): NodeStatus => {
      if (isFailed && id === lastNodeId) return 'error'
      if (executed.has(id)) {
        if (status === 'suspended' && id === lastNodeId) return 'suspended'
        if (status === 'running' && id === lastNodeId) return 'current'
        return 'executed'
      }
      return 'pending'
    }

    const metaById = new Map(flowDef.nodes.map((n) => [n.id, n]))

    const labelFor = (id: string, raw: Record<string, unknown> | undefined): Node => {
      const meta = metaById.get(id)
      const type = String(raw?.type ?? meta?.type ?? (isRoutingNodeId(id) ? 'route' : 'unknown'))
      const name = String(raw?.name ?? meta?.name ?? id)
      return {
        id,
        type: 'default',
        position: { x: 0, y: 0 },
        style: { background: 'transparent', border: 'none' },
        data: {
          label: renderNodeLabel({
            type,
            name,
            status: statusFor(id),
            desc: meta?.desc,
            sourceLine: meta?.sourceLine ?? (typeof raw?.source_line === 'number' ? raw.source_line : undefined),
            fields: (raw?.fields as Record<string, unknown> | undefined) ?? undefined,
            next: (raw?.next as string | undefined) ?? meta?.next,
            elseNext: (raw?.elseNext as string | undefined) ?? meta?.elseNext,
            branchTargets:
              meta && meta.branches.length
                ? Object.fromEntries(meta.branches.map((b) => [b.name, b.next]))
                : undefined,
          }),
        },
      }
    }

    const makeEdge = (source: string, target: string, label?: string): Edge => {
      const failedEdge = isFailed && target === lastNodeId
      const done = executed.has(source) && executed.has(target)
      const color = failedEdge ? ERROR_EDGE_COLOR : done ? EXECUTED_EDGE_COLOR : EDGE_COLOR
      return {
        id: `ve-${source}-${target}${label ? `-${label}` : ''}`,
        source,
        target,
        // 只读视图没有编辑器节点的具名 handle（'true'/'false'/分支名）：
        // 沿用它会让 xyflow 找不到 handle 而**静默丢弃**这条边——这正是
        // 执行页「有节点、无连线」的根因。
        type: EDGE_TYPE,
        label,
        labelStyle: { fill: 'rgb(var(--c-ink-secondary))', fontSize: 10, fontFamily: 'ui-monospace, monospace' },
        labelBgStyle: { fill: 'rgb(var(--c-surface))' },
        labelBgPadding: [3, 1] as [number, number],
        labelBgBorderRadius: 3,
        markerEnd: { type: MarkerType.ArrowClosed, color, width: 16, height: 16 },
        style: { stroke: color, strokeWidth: failedEdge || done ? 1.6 : 1.2 },
      }
    }

    // 路径 A：有真实定义 → 节点沿用定义转换（容器/嵌套不画错），边自建（带分支标签、无失效 handle）
    if (flowDef.definition && flowDef.nodes.length > 0) {
      try {
        const { nodes } = jsonToFlow(flowDef.definition, flowDef.layout)
        const known = new Set(flowDef.nodes.map((n) => n.id))
        const nodesOut = nodes
          .filter((n) => known.has(n.id))
          .map((n) => ({ ...labelFor(n.id, n.data as Record<string, unknown>), position: n.position }))
        if (nodesOut.length > 0) {
          const edgesOut: Edge[] = []
          for (const src of flowDef.nodes) {
            for (const out of nodeOutgoing(src)) {
              if (known.has(out.target)) edgesOut.push(makeEdge(src.id, out.target, out.label))
            }
          }
          const orderIds = resultIds.length > 0 ? resultIds : definitionOrder(flowDef.nodes)
          const hasSavedLayout = Object.keys(flowDef.layout).length > 0
          return {
            nodes: hasSavedLayout ? nodesOut : wrapLayout(nodesOut, orderIds, 4),
            edges: edgesOut,
            orderIds,
            orderFromTrace: resultIds.length > 0,
            statusFor,
          }
        }
      } catch {
        /* 落到路径 B */
      }
    }

    // 路径 B：无定义（404 / 解析失败 / 尚未返回）→ 按真实执行顺序线性还原，先看清先后
    const orderIds = resultIds.length > 0 ? resultIds : []
    const fallbackNodes = orderIds.map((id) => labelFor(id, undefined))
    const fallbackEdges = orderIds.slice(1).map((id, i) => makeEdge(orderIds[i], id))
    return {
      nodes: wrapLayout(fallbackNodes, orderIds, 4),
      edges: fallbackEdges,
      orderIds,
      orderFromTrace: orderIds.length > 0,
      statusFor,
    }
  }, [context, status, flowDef, isFailed])

  const executedCount =
    flowDef.nodes.length > 0
      ? flowDef.nodes.filter((n) => view.statusFor(n.id) === 'executed' || view.statusFor(n.id) === 'error').length
      : view.orderIds.length
  const pendingCount = flowDef.nodes.length > 0 ? flowDef.nodes.length - executedCount : 0

  if (view.nodes.length === 0 && flowDef.isLoading) {
    return (
      <div className="h-full flex items-center justify-center gap-2 text-ink-muted text-data-sm">
        <Loader2 size={14} className="animate-spin" />
        流程定义加载中…
      </div>
    )
  }

  if (view.nodes.length === 0) {
    return (
      <div className="h-full flex flex-col items-center justify-center gap-1 text-ink-muted text-data-sm px-6 text-center">
        <span>无法还原流程结构</span>
        <span className="text-caption text-ink-faint">
          {flowDef.errorMessage
            ? `流程定义不可用：${flowDef.errorMessage}`
            : '执行上下文里没有 $NODE 记录，且流程定义不可用'}
        </span>
      </div>
    )
  }

  return (
    <div className="h-full flex flex-col">
      {/* 执行顺序 + 图例：回答「这些节点是什么顺序、跑到哪了」 */}
      <div className="px-4 py-2 border-b border-line flex items-start gap-3 text-caption flex-wrap">
        <span className="text-ink-muted shrink-0 pt-0.5">
          {view.orderFromTrace ? '执行顺序' : '声明顺序'}
          {!view.orderFromTrace && <span className="text-ink-faint">（无执行痕迹）</span>}
          {flowDef.isLoading && (
            <span className="text-ink-faint inline-flex items-center gap-1 ml-1.5">
              <Loader2 size={10} className="animate-spin" />
              定义加载中
            </span>
          )}
        </span>
        <div className="flex items-center gap-1 flex-wrap max-h-20 overflow-auto flex-1 min-w-[200px]">
          {view.orderIds.map((id, i) => (
            <span key={id} className="flex items-center gap-1">
              {i > 0 && <ArrowRight size={10} className="text-ink-faint shrink-0" />}
              <span
                className={cn(
                  'rounded px-1.5 py-0.5 font-mono text-[11px] whitespace-nowrap',
                  STATUS_CHIP[view.statusFor(id)]
                )}
                title={id}
              >
                {i + 1} {id}
              </span>
            </span>
          ))}
        </div>
        <div className="flex items-center gap-2.5 shrink-0 text-ink-muted pt-0.5">
          <Legend dot="bg-status-success" label={`已执行 ${executedCount}`} />
          {pendingCount > 0 && <Legend dot="bg-status-pending" label={`未执行 ${pendingCount}`} />}
          {isFailed && <Legend dot="bg-status-error" label="错误节点" />}
        </div>
      </div>

      {flowDef.errorMessage && (
        <div className="px-4 py-1.5 text-caption text-status-warning bg-status-warning-dim border-b border-line">
          流程定义不可用（{flowDef.errorMessage}），已按执行顺序回退展示
        </div>
      )}

      <div className="flex-1 min-h-0">
        <ReactFlow
          nodes={view.nodes}
          edges={view.edges}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          minZoom={0.2}
          attributionPosition="bottom-left"
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
        >
          <Background color="rgb(var(--c-dark-500))" gap={20} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  )
}

function Legend({ dot, label }: { dot: string; label: string }) {
  return (
    <span className="flex items-center gap-1">
      <span className={cn('w-1.5 h-1.5 rounded-full', dot)} />
      <span>{label}</span>
    </span>
  )
}
