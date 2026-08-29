import { useMemo } from 'react'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { renderNodeLabel, type NodeStatus } from './flow/nodeTypes'
import { autoLayout, EDGE_COLOR, EDGE_TYPE } from './flow/flowLayout'

interface FlowViewerProps {
  context: Record<string, unknown>
  status: string
}

export default function FlowViewer({ context, status }: FlowViewerProps) {
  const { nodes, edges } = useMemo(() => {
    return extractFlowStructure(context, status)
  }, [context, status])

  if (nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-dark-400">
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
      <Background color="#475569" gap={20} />
      <Controls className="bg-dark-800 border-dark-700" />
    </ReactFlow>
  )
}

// 从执行上下文提取流程结构
function extractFlowStructure(
  context: Record<string, unknown>,
  executionStatus: string
): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = []
  const edges: Edge[] = []

  const flowNodes = (context.nodes as Array<Record<string, unknown>>) || []
  const currentNodeId = context.current_node_id as string
  const executedNodes = (context.executed_nodes as string[]) || []
  const suspendedAt = context.suspended_at as string

  if (flowNodes.length === 0) {
    if (currentNodeId) {
      nodes.push(createFlowNode({
        id: currentNodeId,
        type: 'unknown',
        name: currentNodeId,
        x: 200,
        y: 200,
        status: getNodeStatus(currentNodeId, currentNodeId, executedNodes, suspendedAt, executionStatus),
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
        status: getNodeStatus(nodeId, currentNodeId, executedNodes, suspendedAt, executionStatus),
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

  return { nodes: autoLayout(nodes, edges, 'TB'), edges }
}

function getNodeStatus(
  nodeId: string,
  currentNodeId: string | undefined,
  executedNodes: string[],
  suspendedAt: string | undefined,
  executionStatus: string
): NodeStatus {
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
