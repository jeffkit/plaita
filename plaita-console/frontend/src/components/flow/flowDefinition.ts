/**
 * 流程定义（version.definition）只读解析工具。
 *
 * 执行详情页需要在「没有编辑器」的场景下讲清楚结构：节点类型/名称、声明顺序、
 * 分支去向。这里只做纯函数解析——不做画布、不依赖 React，方便被
 * FlowViewer（连线与着色）和节点时间线（名称/类型对照）共用。
 *
 * 定义形态见 plaita IR：线性连接写在节点字段上（``next`` / ``else_next`` /
 * ``branches[].next``），顶层没有 edges 数组。
 */

export interface FlowBranch {
  name: string
  next: string
}

export interface FlowNodeMeta {
  id: string
  type: string
  name?: string
  desc?: string
  sourceLine?: number
  /** 真/顺序后继 */
  next?: string
  /** if 的假分支后继 */
  elseNext?: string
  /** switch/case 分支后继 */
  branches: FlowBranch[]
  /** 定义里的静态配置字段（已剔除连接键、id/type 与子流程体） */
  fields: Record<string, unknown>
  /** childFlow 子流程节点数（0 = 非容器节点） */
  subflowNodes: number
}

/** @flow 编译器为分支/跳转合成的内部路由节点（``_n1`` 系） */
export function isRoutingNodeId(id: string): boolean {
  return /^_n\d+$/.test(id)
}

/** 连接/元信息键：不属于「配置」字段（连接由边表达，子流程体单独计数） */
const NON_CONFIG_KEYS = new Set([
  'id',
  'type',
  'name',
  'desc',
  'next',
  'else_next',
  'branches',
  'childFlow',
  'source_line',
])

/** 从版本定义里取顶层节点元信息（跳过容器子流程等无 id 项） */
export function extractFlowNodes(definition: Record<string, unknown> | null | undefined): FlowNodeMeta[] {
  const raw = (definition?.nodes as Array<Record<string, unknown>> | undefined) || []
  const out: FlowNodeMeta[] = []
  for (const n of raw) {
    const id = typeof n?.id === 'string' ? n.id : undefined
    if (!id) continue
    const rawBranches = Array.isArray(n.branches) ? (n.branches as Array<Record<string, unknown>>) : []
    const branches: FlowBranch[] = []
    for (const b of rawBranches) {
      if (typeof b?.next !== 'string') continue
      branches.push({
        name: typeof b.name === 'string' && b.name ? b.name : `分支${branches.length + 1}`,
        next: b.next,
      })
    }
    const fields: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(n)) {
      if (NON_CONFIG_KEYS.has(k)) continue
      fields[k] = v
    }
    const child = n.childFlow as { nodes?: unknown[] } | undefined
    out.push({
      id,
      type: typeof n.type === 'string' ? n.type : 'unknown',
      name: typeof n.name === 'string' && n.name ? n.name : undefined,
      desc: typeof n.desc === 'string' ? n.desc : undefined,
      sourceLine: typeof n.source_line === 'number' ? n.source_line : undefined,
      next: typeof n.next === 'string' ? n.next : undefined,
      elseNext: typeof n.else_next === 'string' ? n.else_next : undefined,
      branches,
      fields,
      subflowNodes: Array.isArray(child?.nodes) ? child.nodes.length : 0,
    })
  }
  return out
}

/** 节点在定义中的出边（含分支标签）：线性 next 不打标签，分支才有 */
export function nodeOutgoing(
  node: FlowNodeMeta
): Array<{ target: string; label?: string }> {
  const out: Array<{ target: string; label?: string }> = []
  if (node.next) out.push({ target: node.next })
  if (node.elseNext) out.push({ target: node.elseNext, label: '否' })
  for (const b of node.branches) out.push({ target: b.next, label: b.name })
  return out
}
