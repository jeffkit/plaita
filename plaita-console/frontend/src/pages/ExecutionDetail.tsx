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
  ExternalLink,
} from 'lucide-react'
import { api, API_BASE, authHeaders, ExecutionInfo } from '../services/api'
import FlowViewer from '../components/FlowViewer'
import { Button, Card, StatusBadge, JsonViewer, jsonSummary, cn } from '../components/ui'
import { useFlowDefinition } from '../hooks/useFlowDefinition'
import { isRoutingNodeId, type FlowNodeMeta } from '../components/flow/flowDefinition'
import { STATUS_CHIP, STATUS_DOT, STATUS_LABEL } from '../components/flow/nodeStatusStyles'
import type { NodeStatus } from '../components/flow/nodeTypes'

type ResumeType = 'continue' | 'event' | 'timeout' | 'cancel'

function useExecutionSSE(
  executionId: string | undefined,
  enabled: boolean,
  onLoss: () => void
) {
  const queryClient = useQueryClient()

  useEffect(() => {
    if (!executionId || !enabled) return

    let cancelled = false
    let evtSource: EventSource | null = null

    // C4-4：EventSource 无法携带 Authorization/X-Admin-API-Key 头，鉴权部署
    // 下直连必然 401——先带头 POST 换 60s 一次性票据，再 ?ticket= 连接。
    // 票据不可用（本地单机模式 503 / viewer 403 / 网络失败）时退回直连：
    // 免鉴权部署照常工作，严格鉴权部署由 onerror 回落轮询。
    const connect = (ticket: string | null) => {
      if (cancelled) return
      const query = ticket ? `?ticket=${encodeURIComponent(ticket)}` : ''
      evtSource = new EventSource(`${API_BASE}/executions/${executionId}/stream${query}`)

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
        // （票据一次性：EventSource 自动重连也会 401，统一走回落）
        evtSource?.close()
        onLoss()
      }
    }

    void (async () => {
      let ticket: string | null = null
      try {
        const resp = await fetch(
          `${API_BASE}/executions/${executionId}/stream/ticket`,
          { method: 'POST', headers: authHeaders() }
        )
        if (resp.ok) {
          const body = await resp.json()
          ticket = typeof body?.ticket === 'string' ? body.ticket : null
        }
      } catch { /* 票据获取失败 → 直连尝试 */ }
      connect(ticket)
    })()

    return () => {
      cancelled = true
      evtSource?.close()
    }
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

  // 流程定义：流程图与节点时间线共用一次查询（版本号缺失时取最新已发布版本）
  const flowDef = useFlowDefinition(execution?.flow_id, execution?.flow_version)

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
            <p className="text-data-sm text-ink-muted mt-0.5 truncate flex items-center gap-2">
              <span className="truncate">{execution.execution_id}</span>
              {execution.langfuse_trace_url && (
                <a
                  href={execution.langfuse_trace_url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1 text-plaita-400 hover:text-plaita-300 hover:underline shrink-0"
                  title="在 Langfuse 中查看完整 trace（含 agent 内部事件与 token 用量）"
                >
                  <ExternalLink size={12} />
                  Langfuse
                </a>
              )}
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
          {/* 流程输出（本地模式） */}
          {execution.output !== undefined && execution.output !== null && (
            <Card className="overflow-hidden">
              <div className="px-4 py-3 border-b border-line">
                <h3 className="text-section text-ink-primary">流程输出</h3>
              </div>
              <div className="p-4">
                <JsonViewer value={execution.output} rootName="output" defaultExpandDepth={1} maxHeightClass="max-h-72" />
              </div>
            </Card>
          )}

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

          {/* 本地单机模式：真实节点级 trace（回调采集，含输入/输出） */}
          {execution.nodes && execution.nodes.length > 0 && (
            <Card className="overflow-hidden">
              <div className="px-4 py-3 border-b border-line flex items-center justify-between">
                <h3 className="text-section text-ink-primary">节点执行</h3>
                <span className="text-[11px] text-ink-faint">本地模式 · 采集自执行回调</span>
              </div>
              <div className="p-4 space-y-2">
                {execution.nodes.map((n, i) => (
                  <NodeTraceRow key={`${n.id}-${i}`} node={n} />
                ))}
              </div>
            </Card>
          )}

          {/* 节点时间线：从上下文里还原每个节点的执行痕迹，替代整包 JSON dump */}
          <NodeTimeline
            context={execution.context}
            status={execution.status}
            flowNodes={flowDef.nodes}
            defLoading={flowDef.isLoading}
            defError={flowDef.errorMessage}
          />

          {/* 执行上下文：树形折叠 + 语法高亮，替代整块 <pre> */}
          <Card className="overflow-hidden">
            <div className="px-4 py-3 border-b border-line flex items-center justify-between gap-3">
              <h3 className="text-section text-ink-primary">执行上下文</h3>
              <span className="text-[11px] text-ink-faint">
                $INPUT / $NODE / $GLOBAL 等引擎变量
              </span>
            </div>
            <div className="p-4">
              <JsonViewer
                value={execution.context}
                rootName="context"
                defaultExpandDepth={1}
                maxHeightClass="max-h-[32rem]"
                emptyText="无上下文数据"
              />
            </div>
          </Card>

          {/* 流程可视化 */}
          {execution.context && (
            <Card className="overflow-hidden">
              <div className="px-4 py-3 border-b border-line flex items-center justify-between gap-3">
                <h3 className="text-section text-ink-primary">流程可视化</h3>
                <span className="text-[11px] text-ink-faint">节点状态按 $NODE 执行痕迹着色</span>
              </div>
              <div className="h-[26rem]">
                <FlowViewer
                  context={execution.context}
                  status={execution.status}
                  flowDef={flowDef}
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
    failed: {
      icon: <AlertCircle size={30} />,
      color: 'text-status-error',
      bg: 'bg-status-error-dim',
      border: 'border-status-error/30',
      label: '失败',
    },
    cancelled: {
      icon: <XCircle size={30} />,
      color: 'text-status-cancelled',
      bg: 'bg-inset',
      border: 'border-line',
      label: '已取消',
    },
    pending: {
      icon: <Clock size={30} />,
      color: 'text-status-pending',
      bg: 'bg-inset',
      border: 'border-line',
      label: '等待中',
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

// 节点时间线：从执行上下文还原节点级的执行先后与结果。
//
// 数据来源是引擎写入的 ``$NODE``（node_id → 节点返回值，键顺序即执行顺序）：
// - 旧实现把非对象返回值统一强转成 ``{}``，于是 ``null`` / 字符串节点展开后
//   只剩一个「大括号」——这里按真实类型展示，空值显式说明；
// - 折叠态直接给类型 + 一行摘要，不必逐条展开才有信息；
// - 与流程定义对照补上节点类型/名称，并标注「内部路由节点」与「未执行节点」。
function NodeTimeline({
  context,
  status,
  flowNodes,
  defLoading,
  defError,
}: {
  context?: Record<string, unknown>
  status: string
  flowNodes: FlowNodeMeta[]
  defLoading: boolean
  defError: string | null
}) {
  const nodeMap =
    context?.$NODE && typeof context.$NODE === 'object'
      ? (context.$NODE as Record<string, unknown>)
      : null
  const resultIds = nodeMap ? Object.keys(nodeMap) : []
  const metaById = new Map(flowNodes.map((n) => [n.id, n]))
  const lastNodeId =
    (typeof context?.$LAST_NODE === 'string' ? (context.$LAST_NODE as string) : undefined) ??
    resultIds[resultIds.length - 1]
  const isFailed = status === 'error' || status === 'failed'
  const executed = new Set(resultIds)

  const rowStatus = (id: string): NodeStatus => {
    if (isFailed && id === lastNodeId) return 'error'
    if (id === lastNodeId) {
      if (status === 'running') return 'current'
      if (status === 'suspended') return 'suspended'
    }
    return 'executed'
  }

  // 没有 $NODE：不假装有数据，直接说明并从定义给参照
  if (resultIds.length === 0) {
    return (
      <Card className="overflow-hidden">
        <div className="px-4 py-3 border-b border-line">
          <h3 className="text-section text-ink-primary">节点时间线</h3>
        </div>
        <div className="p-4 text-data-sm text-ink-muted space-y-1.5">
          <p>本次执行没有留下节点级结果：执行上下文里没有 <span className="font-mono">$NODE</span> 记录。</p>
          <p className="text-caption text-ink-faint">
            {defLoading
              ? '正在加载流程定义…'
              : flowNodes.length > 0
                ? `流程定义共 ${flowNodes.length} 个节点，可在下方「流程可视化」按声明顺序查看。`
                : defError
                  ? `流程定义也不可用（${defError}）。`
                  : '流程定义不可用。'}
          </p>
        </div>
      </Card>
    )
  }

  const routingCount = resultIds.filter((id) => isRoutingNodeId(id) && !metaById.has(id)).length
  const realCount = resultIds.length - routingCount
  const notExecuted = flowNodes.filter((n) => !executed.has(n.id))

  return (
    <Card className="overflow-hidden">
      <div className="px-4 py-3 border-b border-line flex items-center justify-between gap-3 flex-wrap">
        <h3 className="text-section text-ink-primary">节点时间线</h3>
        <span className="text-data-sm text-ink-muted tabular-nums">
          已执行 {realCount}
          {routingCount > 0 && ` · 内部路由 ${routingCount}`}
          {flowNodes.length > 0 && ` · 未执行 ${notExecuted.length}`}
        </span>
      </div>
      <div className="divide-y divide-line">
        {resultIds.map((id, index) => {
          const value = nodeMap ? nodeMap[id] : undefined
          const meta = metaById.get(id)
          const routing = isRoutingNodeId(id) && !meta
          const st = routing ? 'executed' : rowStatus(id)
          const { kind, text } = jsonSummary(value)
          const jumpTarget = routing && typeof value === 'string' ? value : null
          const typeLabel = meta?.type ?? (routing ? 'route' : 'unknown')
          const name = meta?.name ?? id
          return (
            <details key={`${id}-${index}`} className="group px-4 py-2.5">
              <summary className="flex items-center gap-2.5 cursor-pointer select-none list-none">
                <span className="font-mono text-data-sm text-ink-faint tabular-nums w-6 text-right shrink-0">
                  {index + 1}
                </span>
                <span className={cn('w-1.5 h-1.5 rounded-full shrink-0', STATUS_DOT[st])} />
                <span className="text-data-sm text-ink-primary truncate">{name}</span>
                {name !== id && (
                  <span className="font-mono text-caption text-ink-faint truncate shrink-0">{id}</span>
                )}
                <span
                  className={cn(
                    'rounded px-1.5 py-0.5 text-[10px] font-mono shrink-0',
                    routing ? 'bg-inset text-ink-muted' : 'bg-inset text-ink-secondary'
                  )}
                  title={meta?.desc}
                >
                  {routing ? '内部路由' : typeLabel}
                </span>
                <span className={cn('rounded px-1.5 py-0.5 text-[10px] shrink-0', STATUS_CHIP[st])}>
                  {STATUS_LABEL[st]}
                </span>
                <span className="ml-auto flex items-center gap-2 min-w-0">
                  <span className="text-caption text-ink-faint font-mono shrink-0">{kind}</span>
                  <span className="text-caption text-ink-muted truncate max-w-[320px]" title={jumpTarget ? `→ ${jumpTarget}` : text}>
                    {jumpTarget ? `→ ${jumpTarget}` : text}
                  </span>
                </span>
              </summary>
              <div className="mt-2 ml-8">
                {meta?.desc && <p className="text-caption text-ink-faint mb-1.5">{meta.desc}</p>}
                {jumpTarget && (
                  <p className="text-caption text-ink-muted mb-1.5">
                    分支/跳转合成节点：本节点未产生业务输出，随后进入{' '}
                    <span className="font-mono text-ink-secondary">{jumpTarget}</span>
                  </p>
                )}
                <JsonViewer value={value} toolbar={false} defaultExpandDepth={2} maxHeightClass="max-h-72" emptyText="该节点没有返回值（null）" />
              </div>
            </details>
          )
        })}
      </div>
      {notExecuted.length > 0 && (
        <div className="px-4 py-2.5 border-t border-line flex items-start gap-2 flex-wrap">
          <span className="text-caption text-ink-faint shrink-0 pt-0.5">
            未执行节点（{notExecuted.length}）
          </span>
          <div className="flex items-center gap-1 flex-wrap">
            {notExecuted.map((n) => (
              <span
                key={n.id}
                className="rounded px-1.5 py-0.5 text-[11px] font-mono bg-inset text-ink-faint"
                title={n.desc}
              >
                {n.name ?? n.id}
              </span>
            ))}
          </div>
        </div>
      )}
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



function NodeTraceRow({ node }: { node: NonNullable<ExecutionInfo['nodes']>[number] }) {
  const [open, setOpen] = useState(false)
  const tone =
    node.status === 'error'
      ? 'border-status-error/50 bg-status-error/5'
      : node.status === 'running'
        ? 'border-plaita-500/50'
        : 'border-line'
  return (
    <div className={`rounded-lg border ${tone} bg-elevated text-xs`}>
      <button
        className="w-full flex items-center justify-between gap-2 px-3 py-2"
        onClick={() => setOpen((v) => !v)}
      >
        <span className="flex items-center gap-2 min-w-0">
          <span className="font-mono text-ink-primary truncate">{node.name || node.id}</span>
          <span className="text-ink-faint">{node.type}</span>
        </span>
        <StatusBadge status={node.status} />
      </button>
      {open && (
        <div className="px-3 pb-2 space-y-1.5">
          {node.input !== undefined && node.input !== null && (
            <div>
              <p className="text-ink-faint mb-1">input</p>
              <JsonViewer value={node.input} toolbar={false} defaultExpandDepth={2} maxHeightClass="max-h-48" />
            </div>
          )}
          {node.output !== undefined && node.output !== null && (
            <div>
              <p className="text-ink-faint mb-1">output</p>
              <JsonViewer value={node.output} toolbar={false} defaultExpandDepth={2} maxHeightClass="max-h-48" />
            </div>
          )}
          {node.error && <p className="text-status-error">{node.error}</p>}
        </div>
      )}
    </div>
  )
}