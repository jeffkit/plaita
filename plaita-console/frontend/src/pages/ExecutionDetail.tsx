import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  ArrowLeft,
  Play,
  Square,
  RefreshCw,
  Clock,
  AlertCircle,
  CheckCircle,
  PauseCircle,
  Loader2,
  X,
  Zap,
  Timer,
  XCircle,
  ChevronRight,
  Radio,
} from 'lucide-react'
import { api, ExecutionInfo } from '../services/api'
import FlowViewer from '../components/FlowViewer'

type ResumeType = 'continue' | 'event' | 'timeout' | 'cancel'

function useExecutionSSE(executionId: string | undefined, enabled: boolean) {
  const queryClient = useQueryClient()

  useEffect(() => {
    if (!executionId || !enabled) return

    const evtSource = new EventSource(`/api/executions/${executionId}/stream`)

    evtSource.addEventListener('initial_state', (e) => {
      try {
        const data = JSON.parse(e.data)
        queryClient.setQueryData(['execution', executionId], data)
      } catch { /* ignore parse errors */ }
    })

    evtSource.addEventListener('update', (e) => {
      try {
        const data = JSON.parse(e.data)
        queryClient.setQueryData(['execution', executionId], data)
      } catch { /* ignore parse errors */ }
    })

    evtSource.onerror = () => {
      evtSource.close()
    }

    return () => evtSource.close()
  }, [executionId, enabled, queryClient])
}

export default function ExecutionDetail() {
  const { executionId } = useParams<{ executionId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [showResumeDialog, setShowResumeDialog] = useState(false)
  const [useSSE, setUseSSE] = useState(true)

  useExecutionSSE(executionId, useSSE)

  const { data: execution, isLoading, refetch } = useQuery({
    queryKey: ['execution', executionId],
    queryFn: () => api.getExecution(executionId!),
    enabled: !!executionId,
    refetchInterval: useSSE ? false : 5000,
  })

  const cancelMutation = useMutation({
    mutationFn: () => api.cancelExecution(executionId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['execution', executionId] })
    },
  })

  const resumeMutation = useMutation({
    mutationFn: (params: { resume_type: string; data?: Record<string, unknown> }) =>
      api.resumeExecution(executionId!, params),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['execution', executionId] })
      setShowResumeDialog(false)
    },
  })

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="animate-spin text-plaita-400" size={32} />
      </div>
    )
  }

  if (!execution) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-dark-400">
        <AlertCircle size={48} className="mb-4" />
        <p>执行不存在</p>
        <button
          onClick={() => navigate('/executions')}
          className="mt-4 text-plaita-400 hover:underline"
        >
          返回列表
        </button>
      </div>
    )
  }

  return (
    <div className="p-8 h-full overflow-auto">
      {/* 头部 */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate('/executions')}
            className="p-2 hover:bg-dark-700 rounded-lg transition-colors"
          >
            <ArrowLeft size={20} />
          </button>
          <div>
            <h1 className="text-2xl font-bold">执行详情</h1>
            <p className="text-dark-400 font-mono text-sm mt-1">
              {execution.execution_id}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={() => refetch()}
            className="flex items-center gap-2 bg-dark-700 hover:bg-dark-600 px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw size={16} />
            刷新
          </button>

          {execution.status === 'running' && (
            <button
              onClick={() => cancelMutation.mutate()}
              disabled={cancelMutation.isPending}
              className="flex items-center gap-2 bg-red-500/20 hover:bg-red-500/30 text-red-400 px-4 py-2 rounded-lg transition-colors"
            >
              <Square size={16} />
              停止
            </button>
          )}

          {execution.status === 'suspended' && (
            <button
              onClick={() => setShowResumeDialog(true)}
              className="flex items-center gap-2 bg-plaita-500 hover:bg-plaita-600 px-4 py-2 rounded-lg transition-colors"
            >
              <Play size={16} />
              恢复
            </button>
          )}

          <button
            onClick={() => setUseSSE(!useSSE)}
            className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors ${
              useSSE
                ? 'bg-plaita-500/20 text-plaita-400 border border-plaita-500/30'
                : 'bg-dark-700 hover:bg-dark-600 text-dark-300'
            }`}
            title={useSSE ? '使用 SSE 实时更新中' : '使用轮询模式'}
          >
            <Radio size={14} />
            {useSSE ? '实时' : '轮询'}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* 左侧：基本信息 */}
        <div className="lg:col-span-1 space-y-6">
          {/* 状态卡片 */}
          <StatusCard execution={execution} />

          {/* 时间信息 */}
          <InfoCard title="时间信息">
            <InfoRow
              label="开始时间"
              value={
                execution.start_time
                  ? new Date(execution.start_time).toLocaleString()
                  : '-'
              }
            />
            <InfoRow
              label="更新时间"
              value={
                execution.last_update_time
                  ? new Date(execution.last_update_time).toLocaleString()
                  : '-'
              }
            />
            <InfoRow
              label="结束时间"
              value={
                execution.end_time
                  ? new Date(execution.end_time).toLocaleString()
                  : '-'
              }
            />
            <InfoRow
              label="持续时间"
              value={calculateDuration(execution.start_time, execution.end_time)}
            />
          </InfoCard>

          {/* 错误信息 */}
          {execution.error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4">
              <h3 className="font-medium text-red-400 mb-2 flex items-center gap-2">
                <AlertCircle size={16} />
                错误信息
              </h3>
              <pre className="text-sm text-red-300 whitespace-pre-wrap font-mono">
                {JSON.stringify(execution.error, null, 2)}
              </pre>
            </div>
          )}
        </div>

        {/* 右侧：上下文和流程图 */}
        <div className="lg:col-span-2 space-y-6">
          {/* 流程信息 */}
          <InfoCard title="流程信息">
            <InfoRow label="流程 ID" value={execution.flow_id} />
            <InfoRow label="版本" value={execution.flow_version || '最新'} />
            <InfoRow label="调用者" value={execution.invoker || '-'} />
          </InfoCard>

          {/* 执行上下文 */}
          <div className="bg-dark-800/50 rounded-xl border border-dark-700 overflow-hidden">
            <div className="px-4 py-3 border-b border-dark-700">
              <h3 className="font-medium">执行上下文</h3>
            </div>
            <div className="p-4 bg-dark-900 max-h-96 overflow-auto">
              <pre className="text-sm font-mono text-dark-300 whitespace-pre-wrap">
                {execution.context
                  ? JSON.stringify(execution.context, null, 2)
                  : '无上下文数据'}
              </pre>
            </div>
          </div>

          {/* 流程可视化 */}
          {execution.context && (
            <div className="bg-dark-800/50 rounded-xl border border-dark-700 overflow-hidden">
              <div className="px-4 py-3 border-b border-dark-700">
                <h3 className="font-medium">流程可视化</h3>
              </div>
              <div className="h-96">
                <FlowViewer context={execution.context} status={execution.status} />
              </div>
            </div>
          )}
        </div>
      </div>

      {showResumeDialog && (
        <ResumeDialog
          onResume={(resumeType, data) => resumeMutation.mutate({ resume_type: resumeType, data })}
          onClose={() => setShowResumeDialog(false)}
          isPending={resumeMutation.isPending}
        />
      )}
    </div>
  )
}

function ResumeDialog({
  onResume,
  onClose,
  isPending,
}: {
  onResume: (resumeType: ResumeType, data?: Record<string, unknown>) => void
  onClose: () => void
  isPending: boolean
}) {
  const [resumeType, setResumeType] = useState<ResumeType>('continue')
  const [eventData, setEventData] = useState('{\n  \n}')
  const [jsonError, setJsonError] = useState('')

  const resumeOptions: { type: ResumeType; icon: React.ReactNode; label: string; desc: string }[] = [
    { type: 'continue', icon: <ChevronRight size={18} />, label: '继续执行', desc: '从挂起点继续执行流程' },
    { type: 'event', icon: <Zap size={18} />, label: '事件触发', desc: '通过事件数据恢复挂起的 EventNode' },
    { type: 'timeout', icon: <Timer size={18} />, label: '超时恢复', desc: '以超时方式恢复挂起节点' },
    { type: 'cancel', icon: <XCircle size={18} />, label: '取消节点', desc: '取消当前挂起的节点并继续' },
  ]

  const handleSubmit = () => {
    let data: Record<string, unknown> | undefined
    if (resumeType === 'event' && eventData.trim()) {
      try {
        data = JSON.parse(eventData)
        setJsonError('')
      } catch {
        setJsonError('JSON 格式错误，请检查')
        return
      }
    }
    onResume(resumeType, data)
  }

  const handleFormat = () => {
    try {
      const parsed = JSON.parse(eventData)
      setEventData(JSON.stringify(parsed, null, 2))
      setJsonError('')
    } catch {
      setJsonError('JSON 格式错误，无法格式化')
    }
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-dark-800 border border-dark-600 rounded-2xl w-full max-w-lg shadow-2xl">
        <div className="flex items-center justify-between px-6 py-4 border-b border-dark-700">
          <h2 className="text-lg font-semibold">恢复执行</h2>
          <button onClick={onClose} className="p-1 hover:bg-dark-700 rounded-lg transition-colors">
            <X size={18} />
          </button>
        </div>

        <div className="p-6 space-y-5">
          <div className="grid grid-cols-2 gap-3">
            {resumeOptions.map((opt) => (
              <button
                key={opt.type}
                onClick={() => setResumeType(opt.type)}
                className={`flex items-start gap-3 p-3 rounded-xl border transition-all text-left ${
                  resumeType === opt.type
                    ? 'bg-plaita-500/15 border-plaita-500/40 text-plaita-300'
                    : 'bg-dark-700/50 border-dark-600 hover:border-dark-500 text-dark-300'
                }`}
              >
                <div className={`mt-0.5 ${resumeType === opt.type ? 'text-plaita-400' : 'text-dark-400'}`}>
                  {opt.icon}
                </div>
                <div>
                  <div className="font-medium text-sm">{opt.label}</div>
                  <div className="text-xs text-dark-400 mt-0.5">{opt.desc}</div>
                </div>
              </button>
            ))}
          </div>

          {resumeType === 'event' && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-sm font-medium text-dark-300">事件数据 (JSON)</label>
                <button
                  onClick={handleFormat}
                  className="text-xs text-plaita-400 hover:text-plaita-300 transition-colors"
                >
                  格式化
                </button>
              </div>
              <textarea
                value={eventData}
                onChange={(e) => { setEventData(e.target.value); setJsonError('') }}
                rows={6}
                className="w-full bg-dark-900 border border-dark-600 rounded-lg px-4 py-3 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500 resize-none"
                placeholder='{"event_type": "approval", "approved": true}'
              />
              {jsonError && (
                <p className="text-xs text-red-400 flex items-center gap-1">
                  <AlertCircle size={12} /> {jsonError}
                </p>
              )}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-dark-700">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded-lg bg-dark-700 hover:bg-dark-600 text-sm transition-colors"
          >
            取消
          </button>
          <button
            onClick={handleSubmit}
            disabled={isPending}
            className="flex items-center gap-2 px-5 py-2 rounded-lg bg-plaita-500 hover:bg-plaita-600 text-sm font-medium transition-colors disabled:opacity-50"
          >
            {isPending ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
            恢复执行
          </button>
        </div>
      </div>
    </div>
  )
}

// 状态卡片
function StatusCard({ execution }: { execution: ExecutionInfo }) {
  const statusConfig = {
    running: {
      icon: <Loader2 className="animate-spin" size={32} />,
      color: 'text-plaita-400',
      bg: 'bg-plaita-500/10',
      border: 'border-plaita-500/30',
      label: '运行中',
    },
    completed: {
      icon: <CheckCircle size={32} />,
      color: 'text-blue-400',
      bg: 'bg-blue-500/10',
      border: 'border-blue-500/30',
      label: '已完成',
    },
    suspended: {
      icon: <PauseCircle size={32} />,
      color: 'text-yellow-400',
      bg: 'bg-yellow-500/10',
      border: 'border-yellow-500/30',
      label: '已暂停',
    },
    error: {
      icon: <AlertCircle size={32} />,
      color: 'text-red-400',
      bg: 'bg-red-500/10',
      border: 'border-red-500/30',
      label: '错误',
    },
  }

  const config = statusConfig[execution.status as keyof typeof statusConfig] || {
    icon: <Clock size={32} />,
    color: 'text-dark-400',
    bg: 'bg-dark-700',
    border: 'border-dark-600',
    label: execution.status,
  }

  return (
    <div className={`rounded-xl border p-6 ${config.bg} ${config.border}`}>
      <div className="flex items-center gap-4">
        <div className={config.color}>{config.icon}</div>
        <div>
          <p className="text-dark-400 text-sm">当前状态</p>
          <p className={`text-2xl font-bold ${config.color}`}>{config.label}</p>
        </div>
      </div>
    </div>
  )
}

// 信息卡片
function InfoCard({
  title,
  children,
}: {
  title: string
  children: React.ReactNode
}) {
  return (
    <div className="bg-dark-800/50 rounded-xl border border-dark-700 overflow-hidden">
      <div className="px-4 py-3 border-b border-dark-700">
        <h3 className="font-medium">{title}</h3>
      </div>
      <div className="p-4 space-y-3">{children}</div>
    </div>
  )
}

// 信息行
function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-dark-400 text-sm">{label}</span>
      <span className="font-mono text-sm">{value}</span>
    </div>
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

