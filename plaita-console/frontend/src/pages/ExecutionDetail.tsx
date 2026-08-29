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
import { Button, Card } from '../components/ui'

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
    <div className="p-6 h-full overflow-auto space-y-5">
      {/* 头部 */}
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <Button variant="ghost" size="sm" onClick={() => navigate('/executions')} title="返回列表">
            <ArrowLeft size={16} />
          </Button>
          <div className="min-w-0">
            <h1 className="text-page-title text-ink-primary">执行详情</h1>
            <p className="text-data-sm text-ink-muted mt-0.5 truncate">
              {execution.execution_id}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <Button variant="ghost" size="sm" onClick={() => refetch()}>
            <RefreshCw size={13} />
            刷新
          </Button>

          {execution.status === 'running' && (
            <Button variant="danger" size="sm" onClick={() => cancelMutation.mutate()} disabled={cancelMutation.isPending}>
              <Square size={13} />
              停止
            </Button>
          )}

          {execution.status === 'suspended' && (
            <Button variant="primary" size="sm" onClick={() => setShowResumeDialog(true)}>
              <Play size={13} />
              恢复
            </Button>
          )}

          <Button
            variant="ghost"
            size="sm"
            onClick={() => setUseSSE(!useSSE)}
            className={useSSE ? 'bg-plaita-500/10 text-plaita-400 hover:text-plaita-400' : undefined}
            title={useSSE ? '使用 SSE 实时更新中' : '使用轮询模式'}
          >
            <Radio size={13} />
            {useSSE ? '实时' : '轮询'}
          </Button>
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
            <div className="bg-status-error-dim border border-status-error/30 rounded-xl p-4">
              <h3 className="text-section text-status-error mb-2 flex items-center gap-2">
                <AlertCircle size={15} />
                错误信息
              </h3>
              <pre className="text-data-sm text-status-error whitespace-pre-wrap font-mono opacity-90">
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
          <Card className="overflow-hidden">
            <div className="px-4 py-3 border-b border-line">
              <h3 className="text-section text-ink-primary">执行上下文</h3>
            </div>
            <div className="p-4 bg-inset max-h-96 overflow-auto">
              <pre className="text-data-sm font-mono text-ink-secondary whitespace-pre-wrap">
                {execution.context
                  ? JSON.stringify(execution.context, null, 2)
                  : '无上下文数据'}
              </pre>
            </div>
          </Card>

          {/* 流程可视化 */}
          {execution.context && (
            <Card className="overflow-hidden">
              <div className="px-4 py-3 border-b border-line">
                <h3 className="text-section text-ink-primary">流程可视化</h3>
              </div>
              <div className="h-96">
                <FlowViewer context={execution.context} status={execution.status} />
              </div>
            </Card>
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
      <div className="bg-elevated border border-line-strong rounded-xl w-full max-w-lg shadow-pop">
        <div className="flex items-center justify-between px-6 py-4 border-b border-line">
          <h2 className="text-section text-ink-primary">恢复执行</h2>
          <button onClick={onClose} className="p-1 rounded-md text-ink-muted hover:text-ink-primary hover:bg-dark-700 transition-colors">
            <X size={16} />
          </button>
        </div>

        <div className="p-6 space-y-5">
          <div className="grid grid-cols-2 gap-3">
            {resumeOptions.map((opt) => (
              <button
                key={opt.type}
                onClick={() => setResumeType(opt.type)}
                className={`flex items-start gap-3 p-3 rounded-lg border transition-colors text-left ${
                  resumeType === opt.type
                    ? 'bg-plaita-500/10 border-plaita-400/40 text-plaita-400'
                    : 'bg-surface border-line hover:border-line-strong text-ink-secondary'
                }`}
              >
                <div className={`mt-0.5 ${resumeType === opt.type ? 'text-plaita-400' : 'text-ink-muted'}`}>
                  {opt.icon}
                </div>
                <div>
                  <div className="font-medium text-body">{opt.label}</div>
                  <div className="text-caption text-ink-muted mt-0.5">{opt.desc}</div>
                </div>
              </button>
            ))}
          </div>

          {resumeType === 'event' && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-body font-medium text-ink-secondary">事件数据 (JSON)</label>
                <button
                  onClick={handleFormat}
                  className="text-caption text-plaita-400 hover:text-plaita-300 transition-colors"
                >
                  格式化
                </button>
              </div>
              <textarea
                value={eventData}
                onChange={(e) => { setEventData(e.target.value); setJsonError('') }}
                rows={6}
                className="input font-mono text-data-sm resize-none"
                placeholder='{"event_type": "approval", "approved": true}'
              />
              {jsonError && (
                <p className="text-caption text-status-error flex items-center gap-1">
                  <AlertCircle size={12} /> {jsonError}
                </p>
              )}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 px-6 py-4 border-t border-line">
          <Button variant="secondary" size="sm" onClick={onClose}>
            取消
          </Button>
          <Button variant="primary" size="sm" onClick={handleSubmit} disabled={isPending}>
            {isPending ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />}
            恢复执行
          </Button>
        </div>
      </div>
    </div>
  )
}

// 状态卡片：语义状态色（DESIGN.md §2.5）
function StatusCard({ execution }: { execution: ExecutionInfo }) {
  const statusConfig = {
    running: {
      icon: <Loader2 className="animate-spin" size={30} />,
      color: 'text-status-running',
      bg: 'bg-status-running-dim',
      border: 'border-status-running/30',
      label: '运行中',
    },
    completed: {
      icon: <CheckCircle size={30} />,
      color: 'text-status-success',
      bg: 'bg-status-success-dim',
      border: 'border-status-success/30',
      label: '已完成',
    },
    suspended: {
      icon: <PauseCircle size={30} />,
      color: 'text-status-warning',
      bg: 'bg-status-warning-dim',
      border: 'border-status-warning/30',
      label: '已暂停',
    },
    error: {
      icon: <AlertCircle size={30} />,
      color: 'text-status-error',
      bg: 'bg-status-error-dim',
      border: 'border-status-error/30',
      label: '错误',
    },
  }

  const config = statusConfig[execution.status as keyof typeof statusConfig] || {
    icon: <Clock size={30} />,
    color: 'text-ink-muted',
    bg: 'bg-inset',
    border: 'border-line',
    label: execution.status,
  }

  return (
    <Card className={`p-5 ${config.bg} ${config.border}`}>
      <div className="flex items-center gap-4">
        <div className={config.color}>{config.icon}</div>
        <div>
          <p className="text-micro uppercase text-ink-muted">当前状态</p>
          <p className={`text-2xl font-bold font-sans ${config.color}`}>{config.label}</p>
        </div>
      </div>
    </Card>
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
    <Card className="overflow-hidden">
      <div className="px-4 py-3 border-b border-line">
        <h3 className="text-section text-ink-primary">{title}</h3>
      </div>
      <div className="p-4 space-y-3">{children}</div>
    </Card>
  )
}

// 信息行
function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-caption text-ink-muted shrink-0">{label}</span>
      <span className="font-mono text-data-sm text-ink-primary truncate">{value}</span>
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

