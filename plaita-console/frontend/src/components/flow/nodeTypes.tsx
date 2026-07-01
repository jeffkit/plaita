/* eslint-disable react-refresh/only-export-components */
import { memo } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'

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

export type NodeStatus = 'executed' | 'current' | 'suspended' | 'pending' | 'error' | 'idle'

export const statusStyles: Record<NodeStatus, { bg: string; border: string; text: string }> = {
  executed: { bg: 'bg-plaita-500/30', border: 'border-plaita-500', text: 'text-plaita-400' },
  current: { bg: 'bg-blue-500/30', border: 'border-blue-500 animate-pulse', text: 'text-blue-400' },
  suspended: { bg: 'bg-yellow-500/30', border: 'border-yellow-500', text: 'text-yellow-400' },
  pending: { bg: 'bg-dark-700', border: 'border-dark-500', text: 'text-dark-400' },
  error: { bg: 'bg-red-500/30', border: 'border-red-500', text: 'text-red-400' },
  idle: { bg: 'bg-dark-700', border: 'border-dark-600', text: 'text-dark-300' },
}

export interface NodeLabelData {
  type: string
  name: string
  status: NodeStatus
}

export function renderNodeLabel({ type, name, status }: NodeLabelData) {
  const config = nodeTypeConfig[type] || nodeTypeConfig.assignment
  const style = statusStyles[status]
  const displayName = name.length > 15 ? name.slice(0, 15) + '...' : name
  return (
    <div className={`px-4 py-2 rounded-lg border-2 ${style.bg} ${style.border} min-w-[120px]`}>
      <div className="flex items-center justify-center gap-2">
        <span className="text-lg">{config.icon}</span>
        <span className={`font-medium text-sm ${style.text}`}>{displayName}</span>
      </div>
      <div className="text-xs text-center text-dark-500 mt-1">{type}</div>
    </div>
  )
}

// 编辑器用的自定义 xyflow 节点：带输入/输出 Handle，可连线
export interface PlaitaNodeData {
  type: string
  name: string
  status?: NodeStatus
  [key: string]: unknown
}

function PlaitaNodeComponent({ data, selected }: NodeProps) {
  const d = data as PlaitaNodeData
  return (
    <div className={`relative ${selected ? 'ring-2 ring-plaita-400 rounded-lg' : ''}`}>
      <Handle type="target" position={Position.Top} id="in" className="!bg-plaita-500" />
      {renderNodeLabel({ type: d.type, name: d.name, status: d.status ?? 'idle' })}
      <Handle type="source" position={Position.Bottom} id="true" className="!bg-plaita-500" />
      <Handle type="source" position={Position.Right} id="false" className="!bg-blue-500" />
    </div>
  )
}

export const PlaitaNode = memo(PlaitaNodeComponent)

// 供编辑器使用的 nodeTypes 映射
export const editorNodeTypes = { plaitaNode: PlaitaNode }
