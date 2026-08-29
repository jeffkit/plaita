import { useCallback, useRef } from 'react'
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  type Node,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useFlowEditor } from '../../stores/flowEditor'
import { editorNodeTypes } from './nodeTypes'
import { defaultEdgeStyle } from './flowLayout'
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
        defaultEdgeOptions={defaultEdgeStyle}
        fitView
        attributionPosition="bottom-left"
      >
        <Background variant={BackgroundVariant.Dots} gap={20} />
        {/* Controls / MiniMap 的配色由 index.css 的 .react-flow__* 规则统一主题化 */}
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable />
      </ReactFlow>
      {selectedNodeId && (
        <div className="absolute bottom-4 left-4 text-caption text-ink-muted bg-elevated/90 border border-line px-2 py-1 rounded-md">
          已选中: <span className="font-mono">{selectedNodeId}</span>
        </div>
      )}
    </div>
  )
}
