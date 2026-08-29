import dagre from '@dagrejs/dagre'
import { MarkerType, type Node, type Edge } from '@xyflow/react'

/** 画布节点布局估计尺寸（与 nodeTypes.tsx 的节点渲染尺寸对齐） */
export const NODE_WIDTH = 200
export const NODE_HEIGHT = 60

export type LayoutDirection = 'TB' | 'LR'

/**
 * dagre 自动布局：单方向展开（TB 自上而下 / LR 自左向右），
 * 主干一条线、分支自然分叉，避免连线交叉绕行。
 */
export function autoLayout(
  nodes: Node[],
  edges: Edge[],
  direction: LayoutDirection = 'TB',
): Node[] {
  const g = new dagre.graphlib.Graph({ compound: true })
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: direction, nodesep: 60, ranksep: 120, marginx: 40, marginy: 40 })
  nodes.forEach((n) => g.setNode(n.id, { width: NODE_WIDTH, height: NODE_HEIGHT }))
  edges.forEach((e) => {
    if (g.hasNode(e.source) && g.hasNode(e.target)) g.setEdge(e.source, e.target)
  })
  dagre.layout(g)
  return nodes.map((n) => {
    const pos = g.node(n.id)
    return {
      ...n,
      position: {
        x: (pos?.x ?? n.position.x + NODE_WIDTH / 2) - NODE_WIDTH / 2,
        y: (pos?.y ?? n.position.y + NODE_HEIGHT / 2) - NODE_HEIGHT / 2,
      },
    }
  })
}

/**
 * 连线默认样式（DESIGN.md §5：随主题翻转）。
 * stroke 走内联 style，可消费 CSS 变量；SVG marker 的 fill 是属性、无法吃变量，
 * 故用中性灰（两主题下均可读），选中/hover 高亮由 index.css 的 !important 规则接管。
 * smoothstep 直角走线在分层布局下不斜穿节点。
 */
export const EDGE_COLOR = 'rgb(var(--c-dark-500))'
export const EDGE_MARKER_COLOR = '#7a828f'
export const EDGE_TYPE = 'smoothstep'

export const defaultEdgeStyle = {
  type: EDGE_TYPE,
  style: { stroke: EDGE_COLOR },
  markerEnd: { type: MarkerType.ArrowClosed as const, color: EDGE_MARKER_COLOR },
}
