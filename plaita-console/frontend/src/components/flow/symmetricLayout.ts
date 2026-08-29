import type { Node, Edge } from '@xyflow/react'
import { NODE_WIDTH, NODE_HEIGHT } from './flowLayout'

const SIBLING_GAP = 56 // 同层兄弟节点水平间隙
const LEVEL_GAP = 130 // 层间距

interface LayoutTree {
  id: string
  children: LayoutTree[]
}

/**
 * 对称树布局（tidy-tree 变体）：
 * - 从入口（start / 零入度节点）沿边建布局树，每个节点只取首条入边为树边；
 *   回边与汇合边不参与布局，仅照常绘制（smoothstep 长线绕行）
 * - 分支节点（if/switch/case/parallel）的各子树按分支顺序水平排开、
 *   父节点水平居中于分支组上方——if 的真假分支天然左右对称于主干
 * - TB：y = 层深；LR：x = 层深。输出为 React Flow 左上角坐标
 */
export function symmetricLayout(
  nodes: Node[],
  edges: Edge[],
  direction: 'TB' | 'LR' = 'TB',
): Node[] {
  if (nodes.length === 0) return nodes
  const byId = new Map(nodes.map((n) => [n.id, n]))

  // 根：start 优先，其次零入度节点，兜底第一个
  const inDeg = new Map<string, number>(nodes.map((n) => [n.id, 0]))
  for (const e of edges) {
    if (inDeg.has(e.target)) inDeg.set(e.target, (inDeg.get(e.target) ?? 0) + 1)
  }
  const root =
    nodes.find((n) => (n.data as { type?: string } | undefined)?.type === 'start')?.id ??
    nodes.find((n) => (inDeg.get(n.id) ?? 0) === 0)?.id ??
    nodes[0].id

  // 邻接表（保持边出现顺序 = 分支声明顺序）
  const adj = new Map<string, string[]>()
  for (const e of edges) {
    if (!byId.has(e.source) || !byId.has(e.target) || e.source === e.target) continue
    if (!adj.has(e.source)) adj.set(e.source, [])
    const list = adj.get(e.source)!
    if (!list.includes(e.target)) list.push(e.target)
  }

  // DFS 建树：每个节点只入树一次，防环
  const inTree = new Set<string>([root])
  const build = (id: string, guard: number): LayoutTree => {
    const t: LayoutTree = { id, children: [] }
    if (guard > 300) return t
    for (const child of adj.get(id) ?? []) {
      if (inTree.has(child)) continue
      inTree.add(child)
      t.children.push(build(child, guard + 1))
    }
    return t
  }
  const tree = build(root, 0)

  // 子树占宽（含右侧间隙）
  const measure = (t: LayoutTree): number =>
    t.children.length === 0
      ? NODE_WIDTH + SIBLING_GAP
      : t.children.reduce((sum, c) => sum + measure(c), 0)

  // center 沿水平轴；depth 沿主轴
  const centerOf = new Map<string, number>()
  const depthOf = new Map<string, number>()
  const place = (t: LayoutTree, depth: number, left: number) => {
    depthOf.set(t.id, depth)
    if (t.children.length === 0) {
      centerOf.set(t.id, left + (measure(t) - SIBLING_GAP) / 2)
      return
    }
    let x = left
    let first = 0
    let last = 0
    t.children.forEach((c, i) => {
      place(c, depth + 1, x)
      const cCenter = centerOf.get(c.id)!
      if (i === 0) first = cCenter
      last = cCenter
      x += measure(c)
    })
    centerOf.set(t.id, (first + last) / 2)
  }
  place(tree, 0, 0)

  // 未入树的孤立节点放到树下方一行
  const orphans = nodes.filter((n) => !centerOf.has(n.id))
  const maxDepth = Math.max(0, ...[...depthOf.values()])

  return nodes.map((n) => {
    const center = centerOf.get(n.id)
    if (center === undefined) {
      // 孤立节点：排队放到布局树正下方
      const idx = orphans.indexOf(n)
      return {
        ...n,
        position:
          direction === 'TB'
            ? { x: idx * (NODE_WIDTH + SIBLING_GAP), y: (maxDepth + 2) * LEVEL_GAP }
            : { x: (maxDepth + 2) * LEVEL_GAP, y: idx * (NODE_HEIGHT + SIBLING_GAP) },
      }
    }
    const depth = depthOf.get(n.id) ?? 0
    return {
      ...n,
      position:
        direction === 'TB'
          ? { x: center - NODE_WIDTH / 2, y: depth * LEVEL_GAP }
          : { x: depth * LEVEL_GAP, y: center - NODE_HEIGHT / 2 },
    }
  })
}
