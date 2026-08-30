import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api } from '../services/api'
import { renderNodeLabel, type NodeStatus } from './flow/nodeTypes'
import { EDGE_COLOR, EDGE_TYPE } from './flow/flowLayout'
import { symmetricLayout } from './flow/symmetricLayout'
import { jsonToFlow } from './flow/flowConverter'

interface FlowViewerProps {
  /** 流程 ID：提供时优先用真实定义建图（分支/并行不会被画错） */
  flowId?: string
  /** 执行所用的流程版本；缺省时取最新已发布版本 */
  version?: string
  context: Record<string, unknown>
  status: string
}

export default function FlowViewer({ flowId, version, context, status }: FlowViewerProps) {
  // 真实定义：从存储的版本定义 + 布局还原结构；context 只负责上色
  const defQuery = useQuery({
    queryKey: ['viewer-version', flowId, version],
    queryFn: async () => {
      if (version) return api.getVersion(flowId!, version)
      const detail = await api.getFlow(flowId!)
      const versions = (detail.versions || []) as Array<{ version: string; status?: string }>
      const best = versions.find((v) => v.status === 'published') || versions[versions.length - 1]
      if (!best) throw new Error('流程暂无任何版本')
      return api.getVersion(flowId!, best.version)
    },
    enabled: !!flowId,
    retry: false,
    staleTime: 60_000,
  })

  const fromDefinition = useMemo<{ nodes: Node[]; edges: Edge[] } | null>(() => {
    if (!flowId || !defQuery.data) return null
    try {
      const def = JSON.parse(defQuery.data.definition || '{}') as Record<string, unknown>
      const layout = JSON.parse(defQuery.data.layout || '{}') as Record<string, { x: number; y: number }>
      const { nodes, edges } = jsonToFlow(def, layout)
      if (nodes.length === 0) return null
      // 画布坐标优先（layout 缺失时对称布局兜底）；jsonToFlow 产出的是编辑器
      // 专用节点类型 plaitaNode，这里没有注册表，统一落到默认节点 + label 渲染
      const positioned = nodes.some((n) => layout[n.id])
        ? (nodes as Node[])
        : symmetricLayout(nodes as Node[], edges as Edge[], 'TB')
      const colored = positioned.map((n) => {
        const d = n.data as Record<string, unknown>
        return {
          ...n,
          type: 'default',
          data: {
            ...d,
            label: renderNodeLabel({
              type: String(d.type ?? ''),
              name: String(d.name ?? d.type ?? n.id),
              status: getNodeStatus(String(n.id), context, status),
            }),
          },
        }
      })
      return { nodes: colored, edges: edges as Edge[] }
    } catch {
      return null
    }
  }, [flowId, defQuery.data, context, status])

  // 兜底：无流程 ID / 定义加载失败 / 解析失败 → 退回旧的 context 推导
  const fallback = useMemo(() => extractFlowStructure(context, status), [context, status])
  const { nodes, edges } = fromDefinition ?? fallback

  if (nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-ink-muted">
        无法解析流程结构
      </div>
    )
  }

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      fitView
      attributionPosition="bottom-left"
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
    >
      <Background color="rgb(var(--c-dark-500))" gap={20} />
      <Controls showInteractive={false} />
    </ReactFlow>
  )
}

// 执行状态上色：错误/挂起/当前节点优先，其次 executed_nodes 集合
function getNodeStatus(
  nodeId: string,
  context: Record<string, unknown>,
  executionStatus: string
): NodeStatus {
  const currentNodeId = context.current_node_id as string | undefined
  const executedNodes = (context.executed_nodes as string[]) || []
  const suspendedAt = context.suspended_at as string | undefined

  if (executionStatus === 'error' && nodeId === currentNodeId) {
    return 'error'
  }
  if (suspendedAt && nodeId === suspendedAt) {
    return 'suspended'
  }
  if (nodeId === currentNodeId) {
    return 'current'
  }
  if (executedNodes.includes(nodeId)) {
    return 'executed'
  }
  return 'pending'
}

// 从执行上下文提取流程结构（兜底路径：按先后执行次序连线）
function extractFlowStructure(
  context: Record<string, unknown>,
  executionStatus: string
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  const flowNodes = (context.nodes as Array<Record<string, unknown>>) || []
  const currentNodeId = context.current_node_id as string

  if (flowNodes.length === 0) {
    if (currentNodeId) {
      nodes.push(createFlowNode({
        id: currentNodeId,
        type: 'unknown',
        name: currentNodeId,
        x: 200,
        y: 200,
        status: getNodeStatus(currentNodeId, context, executionStatus),
      }))
    }
    return { nodes, edges }
  }

  // 执行轨迹按顺序连线（语义：先后执行次序）；坐标用 dagre 单向布局，
  // 替代旧的三列网格，保持与编辑器一致的纵向展开。
  flowNodes.forEach((node, index) => {
    const nodeId = (node.id as string) || `node-${index}`
    const nodeType = (node.type as string) || 'unknown'
    const nodeName = (node.name as string) || nodeId

    nodes.push(
      createFlowNode({
        id: nodeId,
        type: nodeType,
        name: nodeName,
        x: 0,
        y: 0,
        status: getNodeStatus(nodeId, context, executionStatus),
      })
    )

    if (index > 0) {
      const prevNode = flowNodes[index - 1]
      const prevNodeId = (prevNode.id as string) || `node-${index - 1}`
      edges.push({
        id: `edge-${prevNodeId}-${nodeId}`,
        source: prevNodeId,
        target: nodeId,
        type: EDGE_TYPE,
        style: { stroke: EDGE_COLOR },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: EDGE_COLOR,
        },
      })
    }
  })

  return { nodes: symmetricLayout(nodes, edges, 'TB'), edges }
}

function createFlowNode({
  id,
  type,
  name,
  x,
  y,
  status,
}: {
  id: string
  type: string
  name: string
  x: number
  y: number
  status: NodeStatus
}): Node {
  return {
    id,
    position: { x, y },
    data: {
      label: renderNodeLabel({ type, name, status }),
    },
    style: { background: 'transparent', border: 'none' },
  }
}
