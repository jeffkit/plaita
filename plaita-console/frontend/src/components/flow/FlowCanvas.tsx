import { useCallback, useRef } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  MarkerType,
  type Node,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useFlowEditor } from '../../stores/flowEditor'
import { editorNodeTypes } from './nodeTypes'
import type { FlowNodeData } from './flowConverter'

let _nodeSeq = 0

export default function FlowCanvas() {
  const wrapperRef = useRef<HTMLDivElement>(null)
  const nodes = useFlowEditor((s) => s.nodes)
  const edges = useFlowEditor((s) => s.edges)
  const onNodesChange = useFlowEditor((s) => s.onNodesChange)
  const onEdgesChange = useFlowEditor((s) => s.onEdgesChange)
  const onConnect = useFlowEditor((s) => s.onConnect)
  const addNode = useFlowEditor((s) => s.addNode)
  const setSelected = useFlowEditor((s) => s.setSelected)
  const selectedNodeId = useFlowEditor((s) => s.selectedNodeId)

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      const raw = e.dataTransfer.getData('application/plaita-node')
      if (!raw) return
      const { nodeType, name } = JSON.parse(raw) as { nodeType: string; name: string }
      const bounds = wrapperRef.current?.getBoundingClientRect()
      const position = bounds
        ? { x: e.clientX - bounds.left - 60, y: e.clientY - bounds.top - 20 }
        : { x: 200, y: 100 }
      _nodeSeq += 1
      const id = `${nodeType}_${Date.now()}_${_nodeSeq}`
      const data: FlowNodeData = { type: nodeType, name, fields: {} }
      const node: Node = {
        id,
        type: 'plaitaNode',
        position,
        data,
        selected: false,
      }
      addNode(node)
    },
    [addNode]
  )

  return (
    <div ref={wrapperRef} className="flex-1 h-full" onDragOver={onDragOver} onDrop={onDrop}>
      <ReactFlow
        nodes={nodes as Node[]}
        edges={edges as Edge[]}
        nodeTypes={editorNodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={(_, n) => setSelected(n.id)}
        onPaneClick={() => setSelected(null)}
        defaultEdgeOptions={{
          style: { stroke: '#64748b' },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#64748b' },
        }}
        fitView
        attributionPosition="bottom-left"
      >
        <Background color="#475569" gap={20} />
        <Controls className="bg-dark-800 border-dark-700" />
        <MiniMap
          className="bg-dark-800 border border-dark-700"
          nodeColor={() => '#334155'}
        />
      </ReactFlow>
      {selectedNodeId && (
        <div className="absolute bottom-4 left-4 text-xs text-dark-400 bg-dark-800/80 px-2 py-1 rounded">
          已选中: {selectedNodeId}
        </div>
      )}
    </div>
  )
}
