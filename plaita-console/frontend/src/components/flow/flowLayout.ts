import dagre from '@dagrejs/dagre'
import { MarkerType, type Node, type Edge } from '@xyflow/react'

/** 画布节点布局估计尺寸（与 nodeTypes.tsx 的节点渲染尺寸对齐：
 *  卡片 min-w-140 / max-w-240，语义名截断 24 字符时可达 ~240px */
export const NODE_WIDTH = 240
export const NODE_HEIGHT = 60

/** 循环族节点第三行（子流程迷你带）高度 */
export const SUBFLOW_BAND_H = 24

/** 循环族类型：卡片带子流程迷你带（体更高，视觉上与普通节点区分） */
const SUBFLOW_BAND_TYPES = new Set(['map', 'filter', 'find', 'loop', 'reduce', 'while', 'child', 'reference'])

/** 该节点是否渲染子流程迷你带（有体数据才带） */
export function hasSubflowBand(n: { type?: string; data?: unknown }): boolean {
  const d = (n.data ?? {}) as Record<string, unknown>
  const f = (d.fields ?? {}) as Record<string, unknown>
  return SUBFLOW_BAND_TYPES.has(n.type ?? '') && !!(f.childFlow || f.child_flow)
}

/** dagre 布局用节点高度（循环族 + 迷你带；展开容器用容器实际高度） */
export function nodeHeightFor(n: { type?: string; data?: unknown }): number {
  const d = (n.data ?? {}) as Record<string, unknown>
  if (d.expanded && typeof d.containerH === 'number') return d.containerH
  return hasSubflowBand(n) ? NODE_HEIGHT + SUBFLOW_BAND_H : NODE_HEIGHT
}

/** dagre 布局用节点宽度（展开容器用容器实际宽度） */
export function nodeWidthFor(n: { type?: string; data?: unknown }): number {
  const d = (n.data ?? {}) as Record<string, unknown>
  if (d.expanded && typeof d.containerW === 'number') return d.containerW
  return NODE_WIDTH
}

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
  nodes.forEach((n) => {
    g.setNode(n.id, { width: nodeWidthFor(n), height: nodeHeightFor(n) })
  })
  edges.forEach((e) => {
    if (g.hasNode(e.source) && g.hasNode(e.target)) g.setEdge(e.source, e.target)
  })
  dagre.layout(g)
  return nodes.map((n) => {
    const pos = g.node(n.id)
    const w = nodeWidthFor(n)
    const h = nodeHeightFor(n)
    return {
      ...n,
      position: {
        x: (pos?.x ?? n.position.x + w / 2) - w / 2,
        y: (pos?.y ?? n.position.y + h / 2) - h / 2,
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
