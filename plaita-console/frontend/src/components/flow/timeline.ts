// 试跑时间线的分组逻辑（纯函数，无 JSX 依赖；导出供行为抽查/单测）。
import type { DryRunNodeResult } from '../../services/api'

export interface GroupHeaderInfo {
  key: string
  label: string
  /** 视觉缩进层级（根流程为 0，子流程组从 0 起算） */
  indent: number
  count: number
  errored: boolean
}

export type TimelineRow =
  | { kind: 'group'; info: GroupHeaderInfo }
  | { kind: 'node'; node: DryRunNodeResult; depth: number; hidden: boolean }

/** 把扁平 nodes 按 flow_path 展开为「组头 + 节点行」序列（试跑为顺序执行，列表有序）。
 *  flow_path=[主流程, c1] 的节点归到组 "主流程/c1"；并行分支共享启动节点名，
 *  天然合并进同一组。flow_path 缺失（旧后端）时全部 depth=0 平铺，向后兼容。 */
export function buildTimelineRows(nodes: DryRunNodeResult[], collapsed: Set<string>): TimelineRow[] {
  const rows: TimelineRow[] = []
  const counts = new Map<string, number>()
  const errored = new Map<string, boolean>()
  // open 与 flow 层级 1..N 对齐（层级 0 是主流程，不出组头）
  const open: Array<{ key: string; level: number }> = []
  for (const n of nodes) {
    const depth = n.depth ?? 0
    const path: string[] = n.flow_path ?? []
    // 关闭比当前节点更深的组
    while (open.length > depth) open.pop()
    // 逐层对齐：同层 key 变化（兄弟子流程）时关闭并重开
    for (let level = 1; level <= depth; level++) {
      const key = path.slice(0, level + 1).join('/') || `/${level}`
      const cur = open[level - 1]
      if (cur && cur.key !== key) open.length = level - 1
      if (open.length < level) {
        rows.push({
          kind: 'group',
          info: {
            key,
            label: path[level] ?? '子流程',
            indent: level - 1,
            count: 0,
            errored: false,
          },
        })
        open.push({ key, level })
      }
      counts.set(key, (counts.get(key) ?? 0) + 1)
      if (n.status === 'error') errored.set(key, true)
    }
    const hidden =
      depth > 0 &&
      path.some((_p, i) => i >= 1 && collapsed.has(path.slice(0, i + 1).join('/') || `/${i}`))
    rows.push({ kind: 'node', node: n, depth, hidden })
  }
  // 回填组头计数/错误聚合（组头入列时成员尚未走完，最后统一读取）
  return rows.map((r) =>
    r.kind === 'group'
      ? {
          ...r,
          info: {
            ...r.info,
            count: counts.get(r.info.key) ?? 0,
            errored: errored.has(r.info.key),
          },
        }
      : r
  )
}
