import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Play, Square, RefreshCw, ChevronRight, Plus, Trash2, Loader2 } from 'lucide-react'
import { api } from '../services/api'
import StartFlowDialog from '../components/StartFlowDialog'

export default function Executions() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [page, setPage] = useState(1)
  const [statusFilter, setStatusFilter] = useState<string>('')
  const [showStartDialog, setShowStartDialog] = useState(false)
  const [actioningId, setActioningId] = useState<string | null>(null)

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['executions', page, statusFilter],
    queryFn: () => api.getExecutions({ page, size: 20, status: statusFilter || undefined }),
    refetchInterval: 5000,
  })

  const cancelMutation = useMutation({
    mutationFn: api.cancelExecution,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['executions'] })
      setActioningId(null)
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
    },
    onError: () => {
      setActioningId(null)
    },
  })

  const handleCancel = (executionId: string) => {
    setActioningId(executionId)
    cancelMutation.mutate(executionId)
  }

  const handleDelete = (executionId: string) => {
    if (confirm('确定要删除这条执行记录吗？此操作不可恢复。')) {
      setActioningId(executionId)
      deleteMutation.mutate(executionId)
    }
  }

  const executions = data?.executions || []
  const total = data?.total || 0
  const totalPages = Math.ceil(total / 20)

  return (
    <div className="p-8">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-3xl font-bold">执行实例</h1>
        <div className="flex items-center gap-4">
          {/* 状态筛选 */}
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="bg-dark-700 border border-dark-600 rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500"
          >
            <option value="">全部状态</option>
            <option value="running">运行中</option>
            <option value="completed">已完成</option>
            <option value="suspended">已暂停</option>
            <option value="error">错误</option>
          </select>

          {/* 刷新按钮 */}
          <button
            onClick={() => refetch()}
            className="flex items-center gap-2 bg-dark-700 hover:bg-dark-600 px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw size={16} />
            刷新
          </button>

          {/* 启动流程按钮 */}
          <button
            onClick={() => setShowStartDialog(true)}
            className="flex items-center gap-2 bg-plaita-500 hover:bg-plaita-600 px-4 py-2 rounded-lg transition-colors"
          >
            <Plus size={16} />
            启动流程
          </button>
        </div>
      </div>

      {/* 启动流程对话框 */}
      <StartFlowDialog
        isOpen={showStartDialog}
        onClose={() => setShowStartDialog(false)}
      />

      {/* 执行列表 */}
      <div className="bg-dark-800/50 rounded-xl border border-dark-700 overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="bg-dark-700/50 text-dark-400 text-sm">
              <th className="text-left py-4 px-6">执行 ID</th>
              <th className="text-left py-4 px-6">流程 ID</th>
              <th className="text-left py-4 px-6">状态</th>
              <th className="text-left py-4 px-6">开始时间</th>
              <th className="text-left py-4 px-6">持续时间</th>
              <th className="text-left py-4 px-6">操作</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr>
                <td colSpan={6} className="py-12 text-center text-dark-400">
                  加载中...
                </td>
              </tr>
            ) : executions.length === 0 ? (
              <tr>
                <td colSpan={6} className="py-12 text-center text-dark-400">
                  暂无执行记录
                </td>
              </tr>
            ) : (
              executions.map((exec) => (
                <tr
                  key={exec.execution_id}
                  className="border-t border-dark-700/50 hover:bg-dark-700/30 transition-colors"
                >
                  <td className="py-4 px-6">
                    <span className="font-mono text-sm">{exec.execution_id.slice(0, 16)}...</span>
                  </td>
                  <td className="py-4 px-6">{exec.flow_id}</td>
                  <td className="py-4 px-6">
                    <StatusBadge status={exec.status} />
                  </td>
                  <td className="py-4 px-6 text-dark-400 text-sm">
                    {exec.start_time ? new Date(exec.start_time).toLocaleString() : '-'}
                  </td>
                  <td className="py-4 px-6 text-dark-400 text-sm">
                    {calculateDuration(exec.start_time, exec.end_time)}
                  </td>
                  <td className="py-4 px-6">
                    <div className="flex items-center gap-1">
                      {/* 取消按钮 - 仅对运行中的执行显示 */}
                      {exec.status === 'running' && (
                        <button
                          onClick={() => handleCancel(exec.execution_id)}
                          disabled={actioningId === exec.execution_id}
                          className="p-2 hover:bg-red-500/20 rounded-lg text-red-400 transition-colors
                                     disabled:opacity-50 disabled:cursor-not-allowed"
                          title="取消执行"
                        >
                          {actioningId === exec.execution_id && cancelMutation.isPending ? (
                            <Loader2 size={16} className="animate-spin" />
                          ) : (
                            <Square size={16} />
                          )}
                        </button>
                      )}
                      {/* 恢复按钮 - 仅对暂停的执行显示 */}
                      {exec.status === 'suspended' && (
                        <button
                          className="p-2 hover:bg-plaita-500/20 rounded-lg text-plaita-400 transition-colors"
                          title="恢复执行"
                        >
                          <Play size={16} />
                        </button>
                      )}
                      {/* 删除按钮 - 对所有状态显示 */}
                      <button
                        onClick={() => handleDelete(exec.execution_id)}
                        disabled={actioningId === exec.execution_id}
                        className="p-2 hover:bg-red-500/20 rounded-lg text-dark-400 hover:text-red-400 transition-colors
                                   disabled:opacity-50 disabled:cursor-not-allowed"
                        title="删除记录"
                      >
                        {actioningId === exec.execution_id && deleteMutation.isPending ? (
                          <Loader2 size={16} className="animate-spin" />
                        ) : (
                          <Trash2 size={16} />
                        )}
                      </button>
                      {/* 查看详情按钮 */}
                      <button
                        onClick={() => navigate(`/executions/${exec.execution_id}`)}
                        className="p-2 hover:bg-dark-600 rounded-lg text-dark-400 transition-colors"
                        title="查看详情"
                      >
                        <ChevronRight size={16} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* 分页 */}
      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2 mt-6">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page === 1}
            className="px-4 py-2 bg-dark-700 hover:bg-dark-600 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg transition-colors"
          >
            上一页
          </button>
          <span className="px-4 py-2 text-dark-400">
            {page} / {totalPages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page === totalPages}
            className="px-4 py-2 bg-dark-700 hover:bg-dark-600 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg transition-colors"
          >
            下一页
          </button>
        </div>
      )}
    </div>
  )
}

// 状态徽章
function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    running: 'bg-plaita-500/20 text-plaita-400 border-plaita-500/30',
    completed: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    suspended: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    error: 'bg-red-500/20 text-red-400 border-red-500/30',
    cancelled: 'bg-dark-600 text-dark-400 border-dark-500',
  }

  const labels: Record<string, string> = {
    running: '运行中',
    completed: '已完成',
    suspended: '已暂停',
    error: '错误',
    cancelled: '已取消',
  }

  return (
    <span className={`px-3 py-1 rounded-full text-xs border ${styles[status] || 'bg-dark-600 text-dark-400'}`}>
      {labels[status] || status}
    </span>
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

