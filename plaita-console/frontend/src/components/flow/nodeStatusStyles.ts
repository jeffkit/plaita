import type { NodeStatus } from './nodeTypes'

/** 节点状态 → 语义 class（执行详情页的时间线与流程图共用，DESIGN.md §2.5） */
export const STATUS_CHIP: Record<NodeStatus, string> = {
  executed: 'bg-status-success-dim text-status-success',
  current: 'bg-status-running-dim text-status-running',
  suspended: 'bg-status-warning-dim text-status-warning',
  error: 'bg-status-error-dim text-status-error',
  pending: 'bg-inset text-ink-muted',
  idle: 'bg-inset text-ink-muted',
}

export const STATUS_DOT: Record<NodeStatus, string> = {
  executed: 'bg-status-success',
  current: 'bg-status-running',
  suspended: 'bg-status-warning',
  error: 'bg-status-error',
  pending: 'bg-status-pending',
  idle: 'bg-status-pending',
}

/** 节点状态的中文标签（列表/图例用） */
export const STATUS_LABEL: Record<NodeStatus, string> = {
  executed: '已执行',
  current: '执行中',
  suspended: '已挂起',
  error: '错误',
  pending: '未执行',
  idle: '未执行',
}

/**
 * 由执行状态 + ``$NODE``/``$LAST_NODE`` 推出单个节点的状态。
 * 流程图、时间线、节点详情三处共用，避免各自维护一套判断。
 */
export function computeNodeStatus(
  id: string,
  opts: { status: string; lastNodeId?: string; executed: Set<string> }
): NodeStatus {
  const { status, lastNodeId, executed } = opts
  if ((status === 'error' || status === 'failed') && id === lastNodeId) return 'error'
  if (executed.has(id)) {
    if (status === 'suspended' && id === lastNodeId) return 'suspended'
    if (status === 'running' && id === lastNodeId) return 'current'
    return 'executed'
  }
  return 'pending'
}
