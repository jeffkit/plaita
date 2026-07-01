import { useCallback, useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api } from '../services/api'

// 节点类型样式
const nodeStyles: Record<string, { bg: string; border: string; icon: string }> = {
  flow_worker: {
    bg: 'bg-plaita-500/20',
    border: 'border-plaita-500',
    icon: '⚙️',
  },
  delay_service: {
    bg: 'bg-blue-500/20',
    border: 'border-blue-500',
    icon: '⏱️',
  },
  redis_queue_service: {
    bg: 'bg-red-500/20',
    border: 'border-red-500',
    icon: '📬',
  },
  kafka_queue_service: {
    bg: 'bg-orange-500/20',
    border: 'border-orange-500',
    icon: '📨',
  },
  approval_service: {
    bg: 'bg-purple-500/20',
    border: 'border-purple-500',
    icon: '✅',
  },
  resource: {
    bg: 'bg-dark-600',
    border: 'border-dark-400',
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
        <div className="text-dark-400">加载中...</div>
      </div>
    )
  }

  return (
    <div className="h-full relative">
      {/* 标题栏 */}
      <div className="absolute top-4 left-4 z-10 bg-dark-800/90 backdrop-blur-sm rounded-lg px-4 py-2 border border-dark-700">
        <h1 className="text-xl font-bold">服务拓扑</h1>
        <p className="text-dark-400 text-sm">
          共 {topologyData?.nodes.length || 0} 个节点
        </p>
      </div>

      {/* 图例 */}
      <div className="absolute top-4 right-4 z-10 bg-dark-800/90 backdrop-blur-sm rounded-lg px-4 py-3 border border-dark-700">
        <p className="text-sm font-medium mb-2">图例</p>
        <div className="space-y-1 text-xs">
          <div className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full bg-plaita-500"></span>
            <span>FlowWorker</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full bg-blue-500"></span>
            <span>延迟服务</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-3 h-3 rounded-full bg-dark-400"></span>
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
        <Background color="#475569" gap={20} />
        <Controls className="bg-dark-800 border-dark-700" />
        <MiniMap
          nodeColor={(node) => {
            const st = node.data?.serviceType as string | undefined
            const style = (st && nodeStyles[st]) || nodeStyles.resource
            return style.border.replace('border-', '')
          }}
          className="bg-dark-800 border border-dark-700"
        />
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
          <div className={`px-4 py-3 rounded-lg border-2 ${style.bg} ${style.border}`}>
            <div className="flex items-center gap-2">
              <span>{style.icon}</span>
              <span className="font-medium">{node.service_type}</span>
            </div>
            <div className="text-xs text-dark-400 mt-1 truncate max-w-[150px]">
              {node.instance_id.slice(0, 12)}...
            </div>
            <div className={`text-xs mt-1 ${
              node.status === 'running' ? 'text-plaita-400' : 'text-red-400'
            }`}>
              ● {node.status}
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
          <div className={`px-6 py-4 rounded-xl border-2 ${style.bg} ${style.border}`}>
            <div className="flex items-center gap-2">
              <span>{style.icon}</span>
              <span className="font-medium">{node.name}</span>
            </div>
          </div>
        ),
        serviceType: 'resource',
      },
      style: { background: 'transparent', border: 'none' },
    })
  })

  // 边
  const edges: Edge[] = topology.edges.map((edge, index) => ({
    id: `edge-${index}`,
    source: edge.source_id,
    target: edge.target_id,
    label: edge.label,
    labelStyle: { fill: '#94a3b8', fontSize: 10 },
    labelBgStyle: { fill: '#1e293b' },
    animated: edge.edge_type.includes('event'),
    style: { stroke: '#475569' },
    markerEnd: {
      type: MarkerType.ArrowClosed,
      color: '#475569',
    },
  }))

  return { nodes, edges }
}

