import { useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Play, Square, RefreshCw, ChevronRight, Plus, Trash2, Loader2, X } from 'lucide-react'
import { api } from '../services/api'
import StartFlowDialog from '../components/StartFlowDialog'
import { Page, PageHeader, Card, Button, StatusBadge, EmptyState, Table, Th, Tr, Td, TdData, ConfirmDialog } from '../components/ui'

export default function Executions() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  // 筛选/页码进 URL：仪表盘下钻、启动后回跳都可分享、可刷新
  const [searchParams, setSearchParams] = useSearchParams()
  const page = Number(searchParams.get('page')) || 1
  const statusFilter = searchParams.get('status') || ''
  const flowFilter = searchParams.get('flow_id') || ''
  const [showStartDialog, setShowStartDialog] = useState(false)
  const [actioningId, setActioningId] = useState<string | null>(null)
  const [confirmCancelId, setConfirmCancelId] = useState<string | null>(null)
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null)

  const setParam = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams)
    if (value) next.set(key, value)
    else next.delete(key)
    // 筛选条件变化时回到第一页
    if (key !== 'page') next.delete('page')
    setSearchParams(next)
  }

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['executions', page, statusFilter, flowFilter],
    queryFn: () =>
      api.getExecutions({
        page,
        size: 20,
        status: statusFilter || undefined,
        flow_id: flowFilter || undefined,
      }),
    refetchInterval: 5000,
  })

  const cancelMutation = useMutation({
    mutationFn: api.cancelExecution,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['executions'] })
      setActioningId(null)
      setConfirmCancelId(null)
    },
    onError: () => {
      setActioningId(null)
    },
  })

  const deleteMutation = useMutation({
    mutationFn: api.deleteExecution,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['executions'] })
      setActioningId(null)
      setConfirmDeleteId(null)
    },
    onError: () => {
      setActioningId(null)
    },
  })

  const executions = data?.executions || []
  const total = data?.total || 0
  const totalPages = Math.ceil(total / 20)

  return (
    <Page>
      <PageHeader
        title="执行实例"
        subtitle="全部流程执行记录与状态"
        actions={
          <>
            <select
              value={statusFilter}
              onChange={(e) => setParam('status', e.target.value)}
              className="input w-28"
            >
              <option value="">全部状态</option>
              <option value="running">运行中</option>
              <option value="completed">已完成</option>
              <option value="suspended">已暂停</option>
              <option value="error">错误</option>
              <option value="cancelled">已取消</option>
            </select>
            <Button variant="ghost" onClick={() => refetch()}>
              <RefreshCw size={14} />
              刷新
            </Button>
            <Button variant="primary" onClick={() => setShowStartDialog(true)}>
              <Plus size={14} />
              启动流程
            </Button>
          </>
        }
      />

      {/* 流程筛选 chip（从启动对话框/流程页跳转而来时可移除） */}
      {flowFilter && (
        <div className="flex items-center gap-2">
          <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md border border-line bg-plaita-500/10 text-caption text-plaita-400">
            流程：{flowFilter}
            <button onClick={() => setParam('flow_id', '')} title="移除筛选" aria-label="移除流程筛选">
              <X size={12} />
            </button>
          </span>
        </div>
      )}

      {/* 启动流程对话框 */}
      <StartFlowDialog
        isOpen={showStartDialog}
        onClose={() => setShowStartDialog(false)}
      />

      {/* 执行列表：整行可点进详情 */}
      <Card className="overflow-hidden">
        <Table>
          <thead>
            <tr>
              <Th>执行 ID</Th>
              <Th>流程 ID</Th>
              <Th>状态</Th>
              <Th>开始时间</Th>
              <Th>持续时间</Th>
              <Th className="text-right">操作</Th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr>
                <td colSpan={6}>
                  <EmptyState message="加载中…" />
                </td>
              </tr>
            ) : executions.length === 0 ? (
              <tr>
                <td colSpan={6}>
                  <EmptyState message="暂无执行记录" hint="点击右上角「启动流程」发起一次执行" />
                </td>
              </tr>
            ) : (
              executions.map((exec) => (
                <Tr
                  key={exec.execution_id}
                  className="cursor-pointer"
                  onClick={() => navigate(`/executions/${exec.execution_id}`)}
                >
                  <TdData className="text-ink-primary">{exec.execution_id.slice(0, 16)}…</TdData>
                  <TdData>{exec.flow_id}</TdData>
                  <Td><StatusBadge status={exec.status} /></Td>
                  <TdData className="text-ink-muted">
                    {exec.start_time ? new Date(exec.start_time).toLocaleString() : '-'}
                  </TdData>
                  <TdData className="text-ink-muted tabular-nums">
                    {calculateDuration(exec.start_time, exec.end_time)}
                  </TdData>
                  <Td onClick={(e) => e.stopPropagation()}>
                    <div className="flex items-center justify-end gap-0.5">
                      {/* 取消按钮 - 仅对运行中的执行显示 */}
                      {exec.status === 'running' && (
                        <button
                          onClick={() => setConfirmCancelId(exec.execution_id)}
                          disabled={actioningId === exec.execution_id}
                          className="p-1.5 rounded-md text-status-error hover:bg-status-error-dim transition-colors
                                     disabled:opacity-50 disabled:cursor-not-allowed"
                          title="取消执行"
                        >
                          {actioningId === exec.execution_id && cancelMutation.isPending ? (
                            <Loader2 size={15} className="animate-spin" />
                          ) : (
                            <Square size={15} />
                          )}
                        </button>
                      )}
                      {/* 恢复 - 跳详情页操作（详情页有完整的恢复对话框） */}
                      {exec.status === 'suspended' && (
                        <button
                          onClick={() => navigate(`/executions/${exec.execution_id}`)}
                          className="p-1.5 rounded-md text-plaita-400 hover:bg-plaita-500/10 transition-colors"
                          title="恢复执行"
                        >
                          <Play size={15} />
                        </button>
                      )}
                      {/* 删除按钮 - 对所有状态显示 */}
                      <button
                        onClick={() => setConfirmDeleteId(exec.execution_id)}
                        disabled={actioningId === exec.execution_id}
                        className="p-1.5 rounded-md text-ink-muted hover:text-status-error hover:bg-status-error-dim transition-colors
                                   disabled:opacity-50 disabled:cursor-not-allowed"
                        title="删除记录"
                      >
                        {actioningId === exec.execution_id && deleteMutation.isPending ? (
                          <Loader2 size={15} className="animate-spin" />
                        ) : (
                          <Trash2 size={15} />
                        )}
                      </button>
                      {/* 查看详情按钮 */}
                      <button
                        onClick={() => navigate(`/executions/${exec.execution_id}`)}
                        className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors"
                        title="查看详情"
                      >
                        <ChevronRight size={15} />
                      </button>
                    </div>
                  </Td>
                </Tr>
              ))
            )}
          </tbody>
        </Table>
      </Card>

      {/* 分页 */}
      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            onClick={() => setParam('page', String(Math.max(1, page - 1)))}
            disabled={page === 1}
          >
            上一页
          </Button>
          <span className="px-3 text-caption text-ink-muted tabular-nums">
            {page} / {totalPages}
          </span>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => setParam('page', String(Math.min(totalPages, page + 1)))}
            disabled={page === totalPages}
          >
            下一页
          </Button>
        </div>
      )}

      {/* 取消执行确认 */}
      <ConfirmDialog
        open={!!confirmCancelId}
        title="取消这条执行？"
        variant="danger"
        confirmLabel="确认取消"
        busy={cancelMutation.isPending}
        onCancel={() => setConfirmCancelId(null)}
        onConfirm={() => {
          if (confirmCancelId) {
            setActioningId(confirmCancelId)
            cancelMutation.mutate(confirmCancelId)
          }
        }}
      >
        运行中的流程将被中断，状态标记为「已取消」。
      </ConfirmDialog>

      {/* 删除记录确认 */}
      <ConfirmDialog
        open={!!confirmDeleteId}
        title="删除这条执行记录？"
        variant="danger"
        confirmLabel="确认删除"
        busy={deleteMutation.isPending}
        onCancel={() => setConfirmDeleteId(null)}
        onConfirm={() => {
          if (confirmDeleteId) {
            setActioningId(confirmDeleteId)
            deleteMutation.mutate(confirmDeleteId)
          }
        }}
      >
        删除后不可恢复。
      </ConfirmDialog>
    </Page>
  )
}

// 计算持续时间
function calculateDuration(start?: string, end?: string): string {
  if (!start) return '-'

  const startTime = new Date(start).getTime()
  const endTime = end ? new Date(end).getTime() : Date.now()
  const duration = endTime - startTime

  if (duration < 1000) return `${duration}ms`
  if (duration < 60000) return `${(duration / 1000).toFixed(1)}s`
  if (duration < 3600000) return `${(duration / 60000).toFixed(1)}m`
  return `${(duration / 3600000).toFixed(1)}h`
}
