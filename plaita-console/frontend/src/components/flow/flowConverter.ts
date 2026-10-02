import type { Node, Edge } from '@xyflow/react'
import { NODE_WIDTH, NODE_HEIGHT, EDGE_TYPE } from './flowLayout'
import { symmetricLayout } from './symmetricLayout'

// 画布节点 data 结构：type + 展示名 + 类型特定配置字段（不含 next/branches/else_next，
// 这些由画布边推导）。id 用作 Flow 节点 id。
export interface FlowNodeData {
  type: string
  name: string
  /** 人类可读描述（@flow 编译产物自带；形如 "if INPUT.score >= 90（第 4 行）"） */
  desc?: string
  /** @flow 编译期回标的源码行号（配合 flow 定义的 metadata.source 可跳转源码） */
  sourceLine?: number
  fields: Record<string, unknown>
  status?: string
  /** 容器展开态子节点显式标记（jsonToFlow 注入，2026-10 评审 MC1）：
   *  子节点编辑经 ownerId 链写回各层 childFlow IR，取代 id 含 '::' 的
   *  字符串启发式——顶层 id 恰好含 :: 不再误触发写回 */
  isContainerChild?: boolean
  /** isContainerChild 的宿主容器画布节点 id */
  ownerId?: string
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
    if (n.parentId) continue // 容器展开态的子节点：已双写回 owner 的 childFlow，不产顶层内容
    const d = n.data as FlowNodeData
    const nodeObj: Record<string, unknown> = { type: d.type, id: n.id }
    if (d.name) nodeObj.name = d.name
    // 类型特定字段（排除由边推导的连接字段）
    // null/undefined 一律剔除：显式 None 会让 pydantic 的 list/dict 字段
    // 校验直接失败（如 upstream_output=None → "Input should be a valid list"），
    // 而「不写该键」对 Optional 字段是等价且安全的
    for (const [k, v] of Object.entries(d.fields || {})) {
      if (v === null || v === undefined) continue
      nodeObj[k] = v
    }
    // assignment 缺 outputType 时注入引擎默认（后端校验要求；老流程静默补齐）
    if (d.type === 'assignment' && nodeObj.outputType === undefined && nodeObj.output_type === undefined) {
      nodeObj.outputType = { dataType: 'object' }
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
  layout: Record<string, { x: number; y: number }> = {},
  _meta: FlowMeta = {},
  opts: { parentId?: string } = {}
): { nodes: Node<FlowNodeData>[]; edges: Edge[] } {
  const rawNodes = (flowJson.nodes as Array<Record<string, unknown>>) || []
  const nodes: Node<FlowNodeData>[] = []
  const edges: Edge[] = []
  const owner = opts.parentId
  const ns = (x: string) => (owner ? `${owner}::${x}` : x)

  rawNodes.forEach((raw, i) => {
    const id = ns((raw.id as string) || `node-${i}`)
    const type = (raw.type as string) || 'unknown'
    // name 保持 IR 原语义（无 name 即 id），可读性兜底在渲染层做（避免保存时把
    // 合成名污染回 IR）；desc/sourceLine 透传给节点卡片展示与源码跳转。
    const sourceLine = raw.source_line as number | undefined
    const desc = (raw.desc as string) || ''
    const name = (raw.name as string) || id
    // 分支目标透传（画布 if 节点副标题显示去向；保存仍走边推导，不回写）
    const nextId = (raw.next as string) || undefined
    const elseNextId = (raw.else_next as string) || undefined
    // 分支结构保留进 fields（剥离 next：分支目标由画布边推导，保存时回填）。
    // 覆盖 switch/case 的分支条件与 parallel 的分支子图，避免 round-trip 丢失。
    const fieldsBranches = Array.isArray(raw.branches)
      ? (raw.branches as Array<Record<string, unknown>>).map((b) => {
          const { next: _n, ...rest } = b
          void _n
          return rest
        })
      : undefined
    // 提取类型特定字段：排除连接字段与元字段
    const excluded = new Set(['type', 'id', 'name', 'next', 'else_next', 'branches'])
    const fields: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(raw)) {
      if (!excluded.has(k)) fields[k] = v
    }
    if (fieldsBranches !== undefined) fields.branches = fieldsBranches
    nodes.push({
      id,
      type: 'plaitaNode',
      position: layout[id] || { x: 0, y: 0 },
      ...(owner ? { parentId: owner, extent: 'parent' as const, connectable: false, deletable: false } : {}),
      data: {
        type, name, desc, sourceLine, next: nextId, elseNext: elseNextId, fields,
        // 容器子节点显式打标（MC1）：写回路径据此沿 owner 链镜像进各层 IR，
        // 不再依赖「id 含 ::」启发式；同时容器内禁止再展开（方案 Y）
        ...(owner ? { isContainerChild: true, ownerId: owner } : {}),
      },
    })

    // 线性 next（统一从 'true' handle 出发）
    if (typeof raw.next === 'string') {
      edges.push({
        id: ns(`e-${id}-${raw.next}`),
        source: id,
        target: ns(raw.next),
        sourceHandle: 'true',
        type: EDGE_TYPE,
      })
    }
    // if 假分支
    if (typeof raw.else_next === 'string') {
      edges.push({
        id: ns(`e-${id}-else-${raw.else_next}`),
        source: id,
        target: ns(raw.else_next),
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
            id: ns(`e-${id}-${bname}-${target}`),
            source: id,
            // 容器内分支目标同样要挂 owner 前缀（MC3-①）：否则边指向不存在的
            // 顶层 id，画布悬空、保存时 flowToJson 按 sourceHandle 回填丢目标
            target: ns(target),
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
    // 兜底用对称树布局：分支左右均匀分布，主干一条线
    return symmetricLayout(nodes, edges, 'TB') as Node<FlowNodeData>[]
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
    if (n.parentId) continue // 子节点坐标相对容器，不进主图 layout
    layout[n.id] = { x: n.position.x, y: n.position.y }
  }
  return layout
}
