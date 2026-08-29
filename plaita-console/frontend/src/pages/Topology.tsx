import { useCallback, useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api } from '../services/api'
import { EmptyState } from '../components/ui'

// 服务类型 → 语义色（底/描边用状态 dim 体系，图标区分类型）
const nodeStyles: Record<string, { bg: string; border: string; icon: string }> = {
  flow_worker: {
    bg: 'bg-status-running-dim',
    border: 'border-status-running/40',
    icon: '⚙️',
  },
  delay_service: {
    bg: 'bg-status-pending-dim',
    border: 'border-status-pending/40',
    icon: '⏱️',
  },
  redis_queue_service: {
    bg: 'bg-status-error-dim',
    border: 'border-status-error/40',
    icon: '📬',
  },
  kafka_queue_service: {
    bg: 'bg-status-warning-dim',
    border: 'border-status-warning/40',
    icon: '📨',
  },
  approval_service: {
    bg: 'bg-status-success-dim',
    border: 'border-status-success/40',
    icon: '✅',
  },
  resource: {
    bg: 'bg-inset',
    border: 'border-line',
    icon: '🗄️',
  },
}

export default function Topology() {
  const { data: topologyData, isLoading } = useQuery({
    queryKey: ['topology'],
    queryFn: api.getTopology,
    refetchInterval: 10000,
  })

  // 转换为 ReactFlow 格式
  const { nodes: flowNodes, edges: flowEdges } = useMemo(
    () => convertToFlowData(topologyData),
    [topologyData]
  )

  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])

  // 当数据变化时更新节点和边
  useEffect(() => {
    if (flowNodes.length > 0) {
      setNodes(flowNodes)
      setEdges(flowEdges)
    }
  }, [flowNodes, flowEdges, setNodes, setEdges])

  // 节点点击处理
  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    console.log('Node clicked:', node)
    // TODO: 显示节点详情面板
  }, [])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <EmptyState message="加载中…" />
      </div>
    )
  }

  return (
    <div className="h-full relative">
      {/* 标题栏 */}
      <div className="absolute top-4 left-4 z-10 bg-surface/95 rounded-lg px-4 py-2.5 border border-line shadow-card">
        <h1 className="text-section text-ink-primary">服务拓扑</h1>
        <p className="text-caption text-ink-muted mt-0.5">
          共 <span className="font-mono tabular-nums">{topologyData?.nodes.length || 0}</span> 个节点
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
        onNodeClick={onNodeClick}
        fitView
        attributionPosition="bottom-left"
      >
        <Background variant={BackgroundVariant.Dots} gap={20} />
        {/* Controls / MiniMap / 连线配色由 index.css 的 .react-flow__* 规则统一主题化 */}
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable />
      </ReactFlow>
    </div>
  )
}

// 转换拓扑数据为 ReactFlow 格式
function convertToFlowData(topology?: {
  nodes: Array<{
    instance_id: string
    service_type: string
    name: string
    status: string
  }>
  edges: Array<{
    source_id: string
    target_id: string
    edge_type: string
    label: string
  }>
}) {
  if (!topology) {
    return { nodes: [], edges: [] }
  }

  // 计算节点位置（简单的网格布局）
  const serviceNodes = topology.nodes.filter(n => n.service_type !== 'resource')
  const resourceNodes = topology.nodes.filter(n => n.service_type === 'resource')

  const nodes: Node[] = []

  // 服务节点（上方）
  serviceNodes.forEach((node, index) => {
    const style = nodeStyles[node.service_type] || nodeStyles.resource
    const col = index % 4
    const row = Math.floor(index / 4)

    nodes.push({
      id: node.instance_id,
      position: { x: 100 + col * 250, y: 100 + row * 150 },
      data: {
        label: (
          <div className={`relative px-3.5 py-2.5 rounded-lg border shadow-card overflow-hidden ${style.bg} ${style.border}`}>
            <div className="flex items-center gap-2">
              <span>{style.icon}</span>
              <span className="font-mono text-data-sm font-medium text-ink-primary">{node.service_type}</span>
            </div>
            <div className="font-mono text-[10px] text-ink-faint mt-1 truncate max-w-[150px]">
              {node.instance_id.slice(0, 12)}…
            </div>
            <div className={`text-[10px] font-mono mt-1 flex items-center gap-1 ${
              node.status === 'running' ? 'text-status-success' : 'text-status-error'
            }`}>
              <span className={`w-1.5 h-1.5 rounded-full ${
                node.status === 'running' ? 'bg-status-success' : 'bg-status-error'
              }`} />
              {node.status}
            </div>
          </div>
        ),
        serviceType: node.service_type,
      },
      style: { background: 'transparent', border: 'none' },
    })
  })

  // 资源节点（下方中央）
  resourceNodes.forEach((node, index) => {
    const style = nodeStyles.resource

    nodes.push({
      id: node.instance_id,
      position: { x: 300 + index * 250, y: 400 },
      data: {
        label: (
          <div className={`relative px-5 py-3.5 rounded-lg border shadow-card overflow-hidden ${style.bg} ${style.border}`}>
            <div className="flex items-center gap-2">
              <span>{style.icon}</span>
              <span className="font-mono text-data-sm font-medium text-ink-primary">{node.name}</span>
            </div>
          </div>
        ),
        serviceType: 'resource',
      },
      style: { background: 'transparent', border: 'none' },
    })
  })

  // 边（配色走 CSS 变量，随主题翻转；箭头由 index.css 的 .react-flow__arrowhead 接管）
  const edges: Edge[] = topology.edges.map((edge, index) => ({
    id: `edge-${index}`,
    source: edge.source_id,
    target: edge.target_id,
    label: edge.label,
    labelStyle: { fill: 'rgb(var(--c-ink-muted))', fontSize: 10 },
    labelBgStyle: { fill: 'rgb(var(--c-surface))' },
    animated: edge.edge_type.includes('event'),
    style: { stroke: 'rgb(var(--c-dark-500))' },
    markerEnd: {
      type: MarkerType.ArrowClosed,
      color: '#7a828f',
    },
  }))

  return { nodes, edges }
}
