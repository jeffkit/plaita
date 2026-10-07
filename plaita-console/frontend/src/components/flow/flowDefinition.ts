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
}

/** @flow 编译器为分支/跳转合成的内部路由节点（``_n1`` 系） */
export function isRoutingNodeId(id: string): boolean {
  return /^_n\d+$/.test(id)
}

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
    out.push({
      id,
      type: typeof n.type === 'string' ? n.type : 'unknown',
      name: typeof n.name === 'string' && n.name ? n.name : undefined,
      desc: typeof n.desc === 'string' ? n.desc : undefined,
      sourceLine: typeof n.source_line === 'number' ? n.source_line : undefined,
      next: typeof n.next === 'string' ? n.next : undefined,
      elseNext: typeof n.else_next === 'string' ? n.else_next : undefined,
      branches,
    })
  }
  return out
}

/**
 * 定义声明的遍历顺序：从 start（或缺入度节点）沿 next/else/branches 广度优先。
 * 用于在缺少执行痕迹时，仍能给出「流程本来的先后」。
 */
export function definitionOrder(nodes: FlowNodeMeta[]): string[] {
  if (nodes.length === 0) return []
  const byId = new Map(nodes.map((n) => [n.id, n]))
  const pointed = new Set<string>()
  for (const n of nodes) {
    if (n.next) pointed.add(n.next)
    if (n.elseNext) pointed.add(n.elseNext)
    for (const b of n.branches) pointed.add(b.next)
  }
  const entryId = byId.has('start') ? 'start' : nodes.find((n) => !pointed.has(n.id))?.id ?? nodes[0].id

  const seen = new Set<string>()
  const order: string[] = []
  const queue: string[] = [entryId]
  while (queue.length > 0) {
    const id = queue.shift() as string
    if (seen.has(id)) continue
    seen.add(id)
    order.push(id)
    const n = byId.get(id)
    if (!n) continue
    const succ = [n.next, n.elseNext, ...n.branches.map((b) => b.next)].filter(
      (t): t is string => !!t && !seen.has(t)
    )
    queue.push(...succ)
  }
  for (const n of nodes) if (!seen.has(n.id)) order.push(n.id)
  return order
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
