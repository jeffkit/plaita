import type { Node, Edge } from '@xyflow/react'
import { autoLayout, NODE_WIDTH, NODE_HEIGHT, EDGE_TYPE } from './flowLayout'

// 画布节点 data 结构：type + 展示名 + 类型特定配置字段（不含 next/branches/else_next，
// 这些由画布边推导）。id 用作 Flow 节点 id。
export interface FlowNodeData {
  type: string
  name: string
  fields: Record<string, unknown>
  status?: string
  [key: string]: unknown
}

export interface FlowMeta {
  flow_id?: string
  version?: string
  desc?: string
  author?: string
  inputType?: unknown
  outputType?: unknown
  globalContext?: Record<string, unknown>
  metadata?: Record<string, unknown>
}

const BRANCHING_TYPES = new Set(['switch', 'case'])
const IF_TYPE = 'if'

/**
 * 画布（xyflow nodes/edges） → Flow JSON
 * - 线性边 A→B 折叠为 nodes[A].next = "B"
 * - if 节点：sourceHandle="false" 的边 → else_next；其余 → next
 * - switch/case：按 sourceHandle(分支名) 匹配 branches[i].next
 * - 不生成顶层 edges 数组；childFlow 等嵌套结构保留在节点 fields 内
 */
export function flowToJson(
  nodes: Node<FlowNodeData>[],
  edges: Edge[],
  meta: FlowMeta = {}
): Record<string, unknown> {
  const outNodes: Record<string, unknown>[] = []

  for (const n of nodes) {
    const d = n.data as FlowNodeData
    const nodeObj: Record<string, unknown> = { type: d.type, id: n.id }
    if (d.name) nodeObj.name = d.name
    // 类型特定字段（排除由边推导的连接字段）
    for (const [k, v] of Object.entries(d.fields || {})) {
      nodeObj[k] = v
    }

    const outEdges = edges.filter((e) => e.source === n.id)
    if (d.type === IF_TYPE) {
      for (const e of outEdges) {
        if (e.sourceHandle === 'false') {
          nodeObj.else_next = e.target
        } else {
          nodeObj.next = e.target
        }
      }
    } else if (BRANCHING_TYPES.has(d.type)) {
      const branches = (d.fields.branches as Array<Record<string, unknown>>) || []
      const resolved = branches.map((b) => ({ ...b }))
      for (const e of outEdges) {
        const handle = e.sourceHandle
        const idx = handle ? resolved.findIndex((b) => b.name === handle) : -1
        if (idx >= 0) {
          resolved[idx].next = e.target
        }
      }
      nodeObj.branches = resolved
    } else {
      // 线性：取第一条出边
      if (outEdges.length > 0) {
        nodeObj.next = outEdges[0].target
      }
    }

    outNodes.push(nodeObj)
  }

  const flow: Record<string, unknown> = { nodes: outNodes }
  if (meta.flow_id) flow.flow_id = meta.flow_id
  if (meta.version) flow.version = meta.version
  if (meta.desc) flow.desc = meta.desc
  if (meta.author) flow.author = meta.author
  if (meta.inputType !== undefined) flow.inputType = meta.inputType
  if (meta.outputType !== undefined) flow.outputType = meta.outputType
  if (meta.globalContext !== undefined) flow.globalContext = meta.globalContext
  if (meta.metadata !== undefined) flow.metadata = meta.metadata
  return flow
}

/**
 * Flow JSON → 画布（xyflow nodes/edges）+ layout
 */
export function jsonToFlow(
  flowJson: Record<string, unknown>,
  layout: Record<string, { x: number; y: number }> = {}
): { nodes: Node<FlowNodeData>[]; edges: Edge[] } {
  const rawNodes = (flowJson.nodes as Array<Record<string, unknown>>) || []
  const nodes: Node<FlowNodeData>[] = []
  const edges: Edge[] = []

  rawNodes.forEach((raw, i) => {
    const id = (raw.id as string) || `node-${i}`
    const type = (raw.type as string) || 'unknown'
    const name = (raw.name as string) || id
    // 提取类型特定字段：排除连接字段与元字段
    const excluded = new Set(['type', 'id', 'name', 'next', 'else_next', 'branches'])
    const fields: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(raw)) {
      if (!excluded.has(k)) fields[k] = v
    }
    nodes.push({
      id,
      type: 'plaitaNode',
      position: layout[id] || { x: 0, y: 0 },
      data: { type, name, fields },
    })

    // 线性 next（统一从 'true' handle 出发）
    if (typeof raw.next === 'string') {
      edges.push({
        id: `e-${id}-${raw.next}`,
        source: id,
        target: raw.next,
        sourceHandle: 'true',
        type: EDGE_TYPE,
      })
    }
    // if 假分支
    if (typeof raw.else_next === 'string') {
      edges.push({
        id: `e-${id}-else-${raw.else_next}`,
        source: id,
        target: raw.else_next,
        sourceHandle: 'false',
        type: EDGE_TYPE,
      })
    }
    // switch/case 分支
    if (BRANCHING_TYPES.has(type)) {
      const branches = (raw.branches as Array<Record<string, unknown>>) || []
      for (const b of branches) {
        const target = b.next as string | undefined
        const bname = b.name as string | undefined
        if (target && bname) {
          edges.push({
            id: `e-${id}-${bname}-${target}`,
            source: id,
            target,
            sourceHandle: bname,
            type: EDGE_TYPE,
          })
        }
      }
    }
  })

  return { nodes: assignPositions(nodes, edges, layout), edges }
}

/**
 * 坐标分配：优先后端存储的 layout；完全没有时用 dagre 单向布局兜底
 * （从入口单方向展开、分支分叉，替代旧的三列表格式布局）；个别节点缺坐标
 * （如外部新增）放到现有包围盒右下角，不整体重排，保护已保存的手工布局。
 */
function assignPositions(
  nodes: Node<FlowNodeData>[],
  edges: Edge[],
  layout: Record<string, { x: number; y: number }>,
): Node<FlowNodeData>[] {
  const missing = nodes.filter((n) => !layout[n.id])
  if (nodes.length > 0 && missing.length === nodes.length) {
    return autoLayout(nodes, edges, 'TB') as Node<FlowNodeData>[]
  }
  const bounds = nodes.reduce(
    (acc, n) => {
      const p = layout[n.id]
      if (!p) return acc
      return {
        x: Math.max(acc.x, p.x + NODE_WIDTH),
        y: Math.max(acc.y, p.y + NODE_HEIGHT),
      }
    },
    { x: 0, y: 0 },
  )
  let seq = 0
  return nodes.map((n) => {
    const p = layout[n.id]
    if (p) return { ...n, position: p }
    seq += 1
    return { ...n, position: { x: bounds.x + 60, y: bounds.y + 80 * seq } }
  })
}

/** 从画布节点提取 layout（坐标） */
export function extractLayout(nodes: Node[]): Record<string, { x: number; y: number }> {
  const layout: Record<string, { x: number; y: number }> = {}
  for (const n of nodes) {
    layout[n.id] = { x: n.position.x, y: n.position.y }
  }
  return layout
}
