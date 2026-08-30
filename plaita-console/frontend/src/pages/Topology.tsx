import { useEffect, useMemo, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  ReactFlowProvider,
  Node,
  Edge,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  useReactFlow,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api } from '../services/api'
import { EmptyState } from '../components/ui'
import { GitBranch } from 'lucide-react'

// 服务类型 → 展示名（与集群管理卡片对齐）
const SERVICE_DISPLAY: Record<string, string> = {
  flow_worker: '流程执行器',
  delay_service: '延迟服务',
  event_filter: '事件恢复器',
  schedule_service: '调度服务',
  http_callback_service: 'HTTP 回调服务',
  redis_queue_service: 'Redis 队列服务',
  approval_service: '审批服务',
  kafka_queue_service: 'Kafka 队列服务',
}

const nodeStyles: Record<string, { bg: string; border: string; icon: string }> = {
  flow_worker: { bg: 'bg-status-running-dim', border: 'border-status-running/40', icon: '⚙️' },
  delay_service: { bg: 'bg-status-pending-dim', border: 'border-status-pending/40', icon: '⏱️' },
  schedule_service: { bg: 'bg-status-warning-dim', border: 'border-status-warning/40', icon: '⏰' },
  event_filter: { bg: 'bg-status-warning-dim', border: 'border-status-warning/40', icon: '📡' },
  redis_queue_service: { bg: 'bg-status-error-dim', border: 'border-status-error/40', icon: '📬' },
  kafka_queue_service: { bg: 'bg-status-warning-dim', border: 'border-status-warning/40', icon: '📨' },
  approval_service: { bg: 'bg-status-success-dim', border: 'border-status-success/40', icon: '✅' },
  http_callback_service: { bg: 'bg-plaita-500/10', border: 'border-plaita-500/40', icon: '🔔' },
  resource: { bg: 'bg-inset', border: 'border-line-strong', icon: '🗄️' },
}

// 边语义：edge_type → 可读标签
function edgeLabel(edgeType: string, label: string): string {
  if (edgeType === 'uses_queue') return '读写任务队列'
  if (edgeType === 'subscribes_event') return '订阅事件'
  return label || edgeType
}

export default function Topology() {
  return (
    <ReactFlowProvider>
      <TopologyInner />
    </ReactFlowProvider>
  )
}

function TopologyInner() {
  const navigate = useNavigate()
  const { data: topologyData, isLoading } = useQuery({
    queryKey: ['topology'],
    queryFn: api.getTopology,
    refetchInterval: 10000,
  })

  // 语义化变换：实例 → 服务组（按 service_type 聚合），边聚到组级。
  // 解决旧渲染的三类可读性问题：无意义实例 ID、重复条目各画一框、
  // 资源节点标签过暗。数据为空时清空画布（不再残留旧节点）。
  const { flowNodes, flowEdges, summary } = useMemo(() => {
    const nodes: Node[] = []
    const edges: Edge[] = []
    const topo = topologyData
    if (!topo) return { flowNodes: nodes, flowEdges: edges, summary: { groups: 0, instances: 0 } }

    const serviceNodes = topo.nodes.filter((n) => n.service_type !== 'resource')
    const resourceNodes = topo.nodes.filter((n) => n.service_type === 'resource')

    // ---- 服务组节点 ----
    const groups = new Map<string, { ids: string[]; running: number }>()
    const idToGroup = new Map<string, string>()
    for (const n of serviceNodes) {
      const g = groups.get(n.service_type) || { ids: [], running: 0 }
      g.ids.push(n.instance_id)
      if (n.status === 'running') g.running += 1
      groups.set(n.service_type, g)
      idToGroup.set(n.instance_id, n.service_type)
    }

    let col = 0
    for (const [svcType, g] of groups) {
      const style = nodeStyles[svcType] || nodeStyles.resource
      const display = SERVICE_DISPLAY[svcType] || svcType
      const live = g.running > 0
      const chips = g.ids.slice(0, 3).map((id) => id.slice(0, 14))
      // 确定性网格布点：不能交给 dagre/symmetricLayout——未测量节点无尺寸会退化成全 (0,0)
      nodes.push({
        id: `group:${svcType}`,
        position: { x: 60 + col * 280, y: 60 },
        data: {
          label: (
            <div className={`px-4 py-3 rounded-lg border shadow-card min-w-[190px] ${style.bg} ${style.border}`}>
              <div className="flex items-center gap-2">
                <span>{style.icon}</span>
                <span className="text-body font-medium text-ink-primary">{display}</span>
                <span
                  className={`ml-auto text-caption font-mono tabular-nums ${
                    live ? 'text-status-success' : 'text-status-error'
                  }`}
                >
                  {g.running}/{g.ids.length}
                </span>
              </div>
              <div className="mt-1.5 space-y-0.5">
                {chips.map((c) => (
                  <div key={c} className="font-mono text-[10px] text-ink-faint truncate">
                    {c}
                  </div>
                ))}
                {g.ids.length > chips.length && (
                  <div className="text-[10px] text-ink-faint">+{g.ids.length - chips.length} 实例</div>
                )}
              </div>
            </div>
          ),
          serviceType: svcType,
        },
        style: { background: 'transparent', border: 'none' },
      })
      col += 1
    }

    // ---- 资源节点（Redis / 事件总线…）：排在服务组下方 ----
    resourceNodes.forEach((n, index) => {
      const style = nodeStyles.resource
      nodes.push({
        id: n.instance_id,
        position: { x: 200 + index * 280, y: 380 },
        data: {
          label: (
            <div className={`px-4 py-2.5 rounded-lg border shadow-card ${style.bg} ${style.border}`}>
              <div className="flex items-center gap-2">
                <span>{style.icon}</span>
                <span className="text-body font-medium text-ink-primary">{n.name}</span>
              </div>
              <div className="text-[10px] font-mono text-ink-faint mt-0.5">共享资源</div>
            </div>
          ),
          serviceType: 'resource',
        },
        style: { background: 'transparent', border: 'none' },
      })
      void index
    })

    // ---- 边：实例级 → 组级聚合（同源同目标合并为一条）----
    const seen = new Set<string>()
    for (const edge of topo.edges) {
      const srcGroup = idToGroup.get(edge.source_id) || edge.source_id
      const dstId = edge.target_id
      const dstGroup =
        idToGroup.get(dstId) || (resourceNodes.some((rn) => rn.instance_id === dstId) ? dstId : dstId)
      const key = `${srcGroup}|${dstGroup}|${edgeLabel(edge.edge_type, edge.label)}`
      if (seen.has(key)) continue
      seen.add(key)
      edges.push({
        id: `e-${key}`,
        source: `group:${srcGroup}`,
        target: dstGroup.startsWith('resource:') ? dstGroup : `group:${dstGroup}`,
        label: edgeLabel(edge.edge_type, edge.label),
        labelStyle: { fill: 'rgb(var(--c-ink-muted))', fontSize: 10 },
        labelBgStyle: { fill: 'rgb(var(--c-surface))' },
        animated: edge.edge_type.includes('event'),
        style: { stroke: 'rgb(var(--c-ink-muted))' },
        markerEnd: { type: MarkerType.ArrowClosed, color: '#7a828f' },
      })
    }

    return {
      flowNodes: nodes,
      flowEdges: edges as Edge[],
      summary: { groups: groups.size, instances: serviceNodes.length },
    }
  }, [topologyData])

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const rf = useReactFlow()

  // 仅在内容真正变化时写回 state：轮询每 10s 产生新数组对象，
  // 无条件 setNodes 会让 React Flow 反复重挂载节点（闪烁/丢文本）。
  // 签名用 API 原始数据（纯 JSON）——flowNodes 含 JSX 不可序列化
  const rawKey = useMemo(() => JSON.stringify(topologyData), [topologyData])
  const lastKey = useRef('')
  useEffect(() => {
    if (rawKey === lastKey.current) return
    lastKey.current = rawKey
    setNodes(flowNodes)
    setEdges(flowEdges)
  }, [rawKey, flowNodes, flowEdges, setNodes, setEdges])

  // fitView prop 对异步载入的节点不生效（首挂时节点未测量）——节点就绪后主动适配
  useEffect(() => {
    if (nodes.length > 0) {
      const t = setTimeout(() => rf.fitView({ padding: 0.15, maxZoom: 1 }), 150)
      return () => clearTimeout(t)
    }
  }, [nodes, rf])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <EmptyState message="加载中…" />
      </div>
    )
  }

  // 拓扑图来自服务注册表（plaita:registry:*），没有实例注册时是「无数据」而不是「空图」
  if (!topologyData || topologyData.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <EmptyState
          icon={<GitBranch size={20} />}
          message="暂无拓扑数据"
          hint="拓扑图由注册到服务注册表（plaita:registry）的实例自动生成；可在「集群管理」启动 FlowWorker 等托管实例"
          action={
            <button onClick={() => navigate('/cluster')} className="text-caption text-plaita-400 hover:underline">
              去集群管理 →
            </button>
          }
        />
      </div>
    )
  }

  return (
    <div className="h-full relative">
      {/* 标题栏：计数与画布同源（渲染出来的服务组/实例） */}
      <div className="absolute top-4 left-4 z-10 bg-surface/95 rounded-lg px-4 py-2.5 border border-line shadow-card max-w-[340px]">
        <h1 className="text-section text-ink-primary">服务拓扑</h1>
        <p className="text-caption text-ink-muted mt-0.5">
          <span className="font-mono tabular-nums">{summary.groups}</span> 类服务 ·{' '}
          <span className="font-mono tabular-nums">{summary.instances}</span> 个实例
        </p>
        <p className="text-[10px] text-ink-faint mt-1">
          框内数字 = 运行中/实例数；节点由服务注册表（plaita:registry）心跳生成
        </p>
      </div>

      {/* 图例 */}
      <div className="absolute top-4 right-4 z-10 bg-surface/95 rounded-lg px-3 py-2.5 border border-line shadow-card">
        <p className="text-micro uppercase text-ink-muted mb-2">图例</p>
        <div className="space-y-1 text-caption text-ink-secondary">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-status-running"></span>
            <span>FlowWorker</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-status-warning"></span>
            <span>调度 / 事件恢复</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-status-pending"></span>
            <span>延迟服务</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-dark-400"></span>
            <span>共享资源</span>
          </div>
        </div>
      </div>

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        fitView
        minZoom={0.4}
        attributionPosition="bottom-left"
      >
        <Background variant={BackgroundVariant.Dots} gap={20} />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable />
      </ReactFlow>
    </div>
  )
}
