import { useState, useEffect, useCallback } from 'react'
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
import { Button, Card, StatusBadge } from '../components/ui'

type ResumeType = 'continue' | 'event' | 'timeout' | 'cancel'

function useExecutionSSE(
  executionId: string | undefined,
  enabled: boolean,
  onLoss: () => void
) {
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
      // 断开不允许静默：通知调用方回落轮询，页面冻结比报错更危险
      evtSource.close()
      onLoss()
    }

    return () => evtSource.close()
  }, [executionId, enabled, queryClient, onLoss])
}

// 从 error 对象中解析人话：优先常见 message 键，stack/traceback 归入详情
function parseExecutionError(err: unknown): { message: string; details?: string } {
  if (err == null) return { message: '' }
  if (typeof err === 'string') {
    try {
      return parseExecutionError(JSON.parse(err))
    } catch {
      return { message: err }
    }
  }
  if (typeof err === 'object') {
    const obj = err as Record<string, unknown>
    const messageKey = ['message', 'msg', 'error', 'exception', 'detail', 'reason'].find(
      (k) => typeof obj[k] === 'string' && (obj[k] as string).trim()
    )
    const message = messageKey ? (obj[messageKey] as string) : JSON.stringify(obj)
    const stackKey = ['stack', 'traceback', 'details'].find(
      (k) => typeof obj[k] === 'string' && (obj[k] as string).trim()
    )
    const details =
      stackKey && obj[stackKey] !== (messageKey ? obj[messageKey] : undefined)
        ? (obj[stackKey] as string)
        : messageKey
          ? JSON.stringify(obj, null, 2)
          : undefined
    return { message, details }
  }
  return { message: String(err) }
}

export default function ExecutionDetail() {
  const { executionId } = useParams<{ executionId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [showResumeDialog, setShowResumeDialog] = useState(false)
  const [useSSE, setUseSSE] = useState(true)
  const [sseLost, setSseLost] = useState(false)

  const handleSSELoss = useCallback(() => {
    setUseSSE(false)
    setSseLost(true)
  }, [])

  useExecutionSSE(executionId, useSSE, handleSSELoss)

  const { data: execution, isLoading, isError, error, refetch } = useQuery({
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

  if (isError) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-ink-muted">
        <AlertCircle size={48} className="mb-4 text-status-error" />
        <p>执行加载失败：{(error as Error).message}</p>
        <div className="mt-4 flex gap-2">
          <Button variant="secondary" size="sm" onClick={() => refetch()}>
            重试
          </Button>
          <button
            onClick={() => navigate('/executions')}
            className="text-plaita-400 hover:underline text-body"
          >
            返回列表
          </button>
        </div>
      </div>
    )
  }

  if (!execution) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-ink-muted">
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
          <Button variant="ghost" size="sm" onClick={() => navigate('/executions')} aria-label="返回列表" title="返回列表">
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
            onClick={() => {
              setSseLost(false)
              setUseSSE(!useSSE)
            }}
            className={
              useSSE
                ? 'bg-plaita-500/10 text-plaita-400 hover:text-plaita-400'
                : sseLost
                  ? 'text-status-warning hover:text-status-warning'
                  : undefined
            }
            title={
              useSSE
                ? '使用 SSE 实时更新中'
                : sseLost
                  ? '实时连接已断开，自动切换为轮询（点击重连）'
                  : '使用轮询模式'
            }
          >
            <Radio size={13} />
            {useSSE ? '实时' : sseLost ? '轮询（已断开）' : '轮询'}
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

          {/* 错误信息：人话优先，原始详情折叠 */}
          {execution.error && (() => {
            const { message, details } = parseExecutionError(execution.error)
            return (
              <div className="bg-status-error-dim border border-status-error/30 rounded-xl p-4">
                <h3 className="text-section text-status-error mb-2 flex items-center gap-2">
                  <AlertCircle size={15} />
                  错误信息
                </h3>
                <p className="text-body text-status-error whitespace-pre-wrap break-all">{message}</p>
                {details && (
                  <details className="mt-2.5">
                    <summary className="text-caption text-status-error/70 cursor-pointer select-none">
                      原始错误数据
                    </summary>
                    <pre className="mt-2 text-data-sm text-status-error whitespace-pre-wrap font-mono opacity-90 max-h-60 overflow-auto">
                      {details}
                    </pre>
                  </details>
                )}
              </div>
            )
          })()}
        </div>

        {/* 右侧：上下文和流程图 */}
        <div className="lg:col-span-2 space-y-6">
          {/* 流程信息：flow_id 可点回编辑器，接上「失败 → 改流程」的断点 */}
          <InfoCard title="流程信息">
            <div className="flex items-center justify-between gap-3">
              <span className="text-caption text-ink-muted shrink-0">流程 ID</span>
              <button
                onClick={() => navigate(`/flows/${execution.flow_id}/edit${execution.flow_version ? `?version=${encodeURIComponent(execution.flow_version)}` : ''}`)}
                className="font-mono text-data-sm text-plaita-400 hover:underline truncate"
                title="在编辑器中打开该流程"
              >
                {execution.flow_id}
              </button>
            </div>
            <InfoRow label="版本" value={execution.flow_version || '最新'} />
            <InfoRow label="调用者" value={execution.invoker || '-'} />
          </InfoCard>

          {/* 节点时间线：从上下文里还原每个节点的执行痕迹，替代整包 JSON dump */}
          <NodeTimeline context={execution.context} />

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
                <FlowViewer
                  flowId={execution.flow_id}
                  version={execution.flow_version}
                  context={execution.context}
                  status={execution.status}
                />
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
    <div className="fixed inset-0 animate-fade bg-black/60 backdrop-blur-sm flex items-center justify-center z-50">
      <div className="bg-elevated border border-line-strong rounded-xl w-full max-w-lg shadow-pop animate-pop">
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

// 节点时间线：兼容两种 context 形态——
// 分布式运行态 ``$NODE``（dict: node_id → 节点结果）与历史 ``nodes`` 数组；
// 每个节点可展开看原始 in/out，错误节点标红。
function NodeTimeline({ context }: { context?: Record<string, unknown> }) {
  interface TimelineEntry {
    id: string
    raw: Record<string, unknown>
  }
  const entries: TimelineEntry[] = []
  const nodeMap = context?.$NODE as Record<string, unknown> | undefined
  if (nodeMap && typeof nodeMap === 'object') {
    for (const [id, result] of Object.entries(nodeMap)) {
      entries.push({
        id,
        raw: (result && typeof result === 'object' ? result : {}) as Record<string, unknown>,
      })
    }
  }
  const nodeList = (context?.nodes as Array<Record<string, unknown>>) || []
  nodeList.forEach((node, index) => {
    entries.push({ id: String(node.id ?? `node-${index}`), raw: node })
  })
  if (entries.length === 0) return null

  return (
    <Card className="overflow-hidden">
      <div className="px-4 py-3 border-b border-line flex items-center justify-between">
        <h3 className="text-section text-ink-primary">节点时间线</h3>
        <span className="text-data-sm text-ink-muted tabular-nums">{entries.length} 个节点</span>
      </div>
      <div className="divide-y divide-line">
        {entries.map((entry, index) => {
          const raw = entry.raw
          const type = String(raw.type ?? raw.node_subtype ?? '')
          const name = typeof raw.name === 'string' && raw.name ? raw.name : entry.id
          const status = typeof raw.status === 'string' ? raw.status : ''
          const errorText =
            typeof raw.error === 'string'
              ? raw.error
              : raw.error != null
                ? JSON.stringify(raw.error)
                : ''
          const hasError = !!errorText
          const rest = { ...raw }
          delete (rest as Record<string, unknown>).error
          return (
            <details key={`${entry.id}-${index}`} className="group px-4 py-2.5">
              <summary className="flex items-center gap-2.5 cursor-pointer select-none list-none">
                <span className="font-mono text-data-sm text-ink-faint tabular-nums w-6 text-right shrink-0">
                  {index + 1}
                </span>
                <span className="font-mono text-data-sm text-ink-primary truncate">{name}</span>
                {type && <span className="text-caption text-ink-faint shrink-0">{type}</span>}
                {status && <StatusBadge status={status} />}
                {hasError && (
                  <span className="ml-auto text-caption text-status-error truncate max-w-[50%]">
                    {errorText.split('\n')[0]}
                  </span>
                )}
              </summary>
              <div className="mt-2 ml-8">
                {hasError && (
                  <pre className="mb-2 text-data-sm text-status-error whitespace-pre-wrap font-mono bg-status-error-dim rounded-md p-2.5">
                    {errorText}
                  </pre>
                )}
                <pre className="text-data-sm font-mono text-ink-faint whitespace-pre-wrap max-h-52 overflow-auto bg-inset rounded-md p-2.5">
                  {JSON.stringify(rest, null, 2)}
                </pre>
              </div>
            </details>
          )
        })}
      </div>
    </Card>
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

