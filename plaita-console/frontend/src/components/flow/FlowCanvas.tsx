import { useCallback, useEffect, useRef } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useReactFlow,
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
  return (
    <ReactFlowProvider>
      <FlowCanvasInner />
    </ReactFlowProvider>
  )
}

function FlowCanvasInner() {
  const wrapperRef = useRef<HTMLDivElement>(null)
  const { screenToFlowPosition } = useReactFlow()
  const nodes = useFlowEditor((s) => s.nodes)
  const edges = useFlowEditor((s) => s.edges)
  const onNodesChange = useFlowEditor((s) => s.onNodesChange)
  const onEdgesChange = useFlowEditor((s) => s.onEdgesChange)
  const onConnect = useFlowEditor((s) => s.onConnect)
  const addNode = useFlowEditor((s) => s.addNode)
  const setSelected = useFlowEditor((s) => s.setSelected)
  const selectedNodeId = useFlowEditor((s) => s.selectedNodeId)

  /** 构造新节点（拖拽落点 / 调色板点击共用） */
  const buildNode = useCallback(
    (nodeType: string, name: string, position: { x: number; y: number }): Node => {
      _nodeSeq += 1
      const id = `${nodeType}_${Date.now()}_${_nodeSeq}`
      const data: FlowNodeData = { type: nodeType, name, fields: {} }
      return { id, type: 'plaitaNode', position, data, selected: false }
    },
    []
  )

  // 调色板「点击添加」（2026-10 表单评审：此前只能拖拽，点击无反馈）：
  // 落在画布视口中心附近，小幅级联偏移避免连点堆叠，并选中以直接开配置
  const addToCanvas = useCallback(
    (nodeType: string, name: string) => {
      const bounds = wrapperRef.current?.getBoundingClientRect()
      const center = bounds
        ? screenToFlowPosition({
            x: bounds.left + bounds.width / 2,
            y: bounds.top + bounds.height / 2,
          })
        : { x: 200, y: 120 }
      const node = buildNode(nodeType, name, {
        x: center.x - 60 + (_nodeSeq % 5) * 28,
        y: center.y - 20 + (_nodeSeq % 5) * 20,
      })
      addNode(node)
      setSelected(node.id)
    },
    [addNode, setSelected, buildNode, screenToFlowPosition]
  )

  useEffect(() => {
    useFlowEditor.setState({ addNodeFromPalette: addToCanvas })
    return () => useFlowEditor.setState({ addNodeFromPalette: null })
  }, [addToCanvas])

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
      addNode(buildNode(nodeType, name, position))
    },
    [addNode, buildNode]
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
