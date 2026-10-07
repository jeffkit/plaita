import { useState, useEffect, useCallback, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  ArrowLeft,
  Play,
  Square,
  RefreshCw,
  AlertCircle,
  Loader2,
  X,
  Zap,
  Timer,
  XCircle,
  ChevronRight,
  Radio,
  ExternalLink,
  Workflow,
  User,
  CalendarClock,
} from 'lucide-react'
import { api, API_BASE, authHeaders, ExecutionInfo, type NodeTiming } from '../services/api'
import { Button, Card, StatusBadge, JsonViewer, jsonSummary, cn } from '../components/ui'
import { useFlowDefinition } from '../hooks/useFlowDefinition'
import { nodeOutgoing, type FlowNodeMeta } from '../components/flow/flowDefinition'
import { STATUS_CHIP, STATUS_DOT, STATUS_LABEL } from '../components/flow/nodeStatusStyles'
import { buildNodeDetails, type ExecutedNodeDetail } from '../components/flow/executionNodes'

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

  // 节点详情：执行序、状态、输入来源（按定义表达式解析）、配置、输出。
  // 覆盖全部定义节点（未执行的也有详情），画布点选任何节点都不会「什么也没有」。
  const nodeDetails = useMemo(
    () =>
      buildNodeDetails({
        context: execution?.context,
        status: execution?.status ?? '',
        flowNodes: flowDef.nodes,
        traces: execution?.nodes,
      }),
    [execution?.context, execution?.status, execution?.nodes, flowDef.nodes]
  )
  const executedDetails = useMemo(() => nodeDetails.filter((d) => d.executed), [nodeDetails])

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

      {/* 紧凑信息条：状态 + 流程/版本/调用者/时间/耗时/ID 一行读完。
          取代原来占版面 1/3 却只放四行字的「状态卡 + 时间信息」两张大卡 */}
      <MetaBar
        execution={execution}
        onOpenFlow={() =>
          navigate(
            `/flows/${execution.flow_id}/edit${execution.flow_version ? `?version=${encodeURIComponent(execution.flow_version)}` : ''}`
          )
        }
      />

      {/* 错误信息：人话优先，原始详情折叠（全宽，失败时最该先看到） */}
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

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-5 items-start">
        {/* 主列：节点时间线（执行详情的主要阅读动线，含每节点的输入/配置/输出/后继） */}
        <div className="xl:col-span-2 space-y-5">
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

          {/* 节点时间线：执行先后 + 每节点的输入/配置/输出/后继 */}
          <NodeTimeline
            details={executedDetails}
            flowNodes={flowDef.nodes}
            defLoading={flowDef.isLoading}
            defError={flowDef.errorMessage}
            timings={execution.node_timings}
            executionStart={execution.start_time}
            executionEnd={execution.end_time}
          />
        </div>

        {/* 侧列：流程输出 + 执行上下文（参考型信息，长页面滚动时保持可见） */}
        <div className="xl:col-span-1 space-y-5 xl:sticky xl:top-6">
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

// 紧凑信息条：状态与关键事实一行读完。
// 取代原来「状态卡 + 时间信息 + 流程信息」三张占 1/3 版面、只放不到十行字的卡片。
function MetaBar({ execution, onOpenFlow }: { execution: ExecutionInfo; onOpenFlow: () => void }) {
  const fmt = (t?: string) => (t ? new Date(t).toLocaleString() : '-')
  return (
    <Card className="px-4 py-3">
      <div className="flex items-center gap-x-5 gap-y-2 flex-wrap">
        <StatusBadge status={execution.status} className="text-body px-2.5 py-1" />
        <Fact icon={<Workflow size={12} />} label="流程">
          <button
            onClick={onOpenFlow}
            className="font-mono text-plaita-400 hover:underline truncate max-w-[16rem]"
            title="在编辑器中打开该流程"
          >
            {execution.flow_id}
          </button>
          <span className="text-ink-faint text-caption">
            @{execution.flow_version || 'latest'}
          </span>
        </Fact>
        <Fact icon={<User size={12} />} label="调用者">
          {execution.invoker || '-'}
        </Fact>
        <Fact icon={<CalendarClock size={12} />} label="开始">
          {fmt(execution.start_time)}
        </Fact>
        <Fact label="结束">{fmt(execution.end_time)}</Fact>
        <Fact icon={<Timer size={12} />} label="耗时">
          <span className="tabular-nums">{calculateDuration(execution.start_time, execution.end_time)}</span>
        </Fact>
        <Fact label="更新">{fmt(execution.last_update_time)}</Fact>
      </div>
    </Card>
  )
}

/** 信息条里的「标签 + 值」单元：标签小而静，值可交互 */
function Fact({
  icon,
  label,
  children,
}: {
  icon?: React.ReactNode
  label: string
  children: React.ReactNode
}) {
  return (
    <span className="flex items-center gap-1.5 min-w-0">
      <span className="flex items-center gap-1 text-caption text-ink-muted shrink-0">
        {icon}
        {label}
      </span>
      <span className="flex items-center gap-1 text-data-sm text-ink-primary min-w-0 truncate">
        {children}
      </span>
    </span>
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
  details,
  flowNodes,
  defLoading,
  defError,
  timings,
  executionStart,
  executionEnd,
}: {
  details: ExecutedNodeDetail[]
  flowNodes: FlowNodeMeta[]
  defLoading: boolean
  defError: string | null
  /** 节点级耗时（node_id → 耗时） */
  timings?: Record<string, NodeTiming> | null
  executionStart?: string
  executionEnd?: string
}) {
  const [filter, setFilter] = useState('')

  const timingOf = (id: string): NodeTiming | undefined => timings?.[id]
  const nodeMs = (id: string): number | null => {
    const t = timingOf(id)
    return t?.total_duration_ms ?? t?.duration_ms ?? null
  }

  // 瀑布图基准：以**首个节点的开始**为 0（epoch 毫秒，跨进程/时区都不会错位）
  const timedIds = details.map((d) => d.id).filter((id) => timingOf(id)?.started_ms != null)
  const spanStart = timedIds.length
    ? Math.min(...timedIds.map((id) => timingOf(id)!.started_ms as number))
    : 0
  const spanEnd = timedIds.length
    ? Math.max(...timedIds.map((id) => (timingOf(id)!.ended_ms ?? timingOf(id)!.started_ms) as number))
    : 0
  const span = Math.max(0, spanEnd - spanStart)

  const sumNodeMs = details.reduce((acc, d) => acc + (nodeMs(d.id) ?? 0), 0)
  const execMs = (() => {
    if (!executionStart) return null
    const start = new Date(executionStart).getTime()
    const end = executionEnd ? new Date(executionEnd).getTime() : Date.now()
    return Number.isFinite(start) ? Math.max(0, end - start) : null
  })()
  const slowestId = details.reduce<string | null>((acc, d) => {
    if (nodeMs(d.id) === null) return acc
    if (acc === null) return d.id
    return (nodeMs(d.id) ?? 0) > (nodeMs(acc) ?? 0) ? d.id : acc
  }, null)

  /** 节点间空隙：上一个节点结束到本节点开始之间的时间（挂起等待/调度开销）。
   *  只在前后两节点都有时间戳时才算，避免拿缺时间戳的节点硬凑。 */
  const gapMs = (id: string): number | null => {
    const idx = details.findIndex((d) => d.id === id)
    if (idx <= 0) return null
    const cur = timingOf(id)
    const prev = timingOf(details[idx - 1].id)
    if (cur?.started_ms == null || prev?.ended_ms == null) return null
    const gap = cur.started_ms - prev.ended_ms
    return gap > 20 ? gap : null
  }
  const maxGapMs = details.reduce((acc, d) => Math.max(acc, gapMs(d.id) ?? 0), 0)

  const query = filter.trim().toLowerCase()
  const visible = query
    ? details.filter((d) =>
        [d.id, d.meta?.name, d.meta?.type, d.meta?.desc].some(
          (v) => v && String(v).toLowerCase().includes(query)
        )
      )
    : details
  // 没有 $NODE：不假装有数据，直接说明并从定义给参照
  if (details.length === 0) {
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
                ? `流程定义共 ${flowNodes.length} 个节点，但本次执行没有节点级痕迹，无法还原先后与输入输出。`
                : defError
                  ? `流程定义也不可用（${defError}）。`
                  : '流程定义不可用。'}
          </p>
        </div>
      </Card>
    )
  }

  const routingCount = details.filter((d) => d.isRouting).length
  const realCount = details.length - routingCount
  const executedIds = new Set(details.map((d) => d.id))
  const notExecuted = flowNodes.filter((n) => !executedIds.has(n.id))

  return (
    <Card className="overflow-hidden">
      <div className="px-4 py-3 border-b border-line space-y-2">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <h3 className="text-section text-ink-primary">节点时间线</h3>
          <div className="flex items-center gap-2">
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="筛选节点…"
              className="input py-1 text-data-sm w-40"
            />
            <span className="text-data-sm text-ink-muted tabular-nums whitespace-nowrap">
              已执行 {realCount}
              {routingCount > 0 && ` · 内部路由 ${routingCount}`}
              {flowNodes.length > 0 && ` · 未执行 ${notExecuted.length}`}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-x-4 gap-y-1 flex-wrap text-caption text-ink-muted">
          {sumNodeMs > 0 ? (
            <>
              <span>
                节点耗时合计 <span className="text-ink-primary tabular-nums">{formatDuration(sumNodeMs)}</span>
              </span>
              {execMs !== null && execMs > 0 && (
                <span>
                  占流程总耗时{' '}
                  <span className="text-ink-primary tabular-nums">{formatPercent(sumNodeMs, execMs)}</span>
                  <span className="text-ink-faint">（总 {formatDuration(execMs)}）</span>
                </span>
              )}
              {slowestId && (
                <span>
                  最慢{' '}
                  <span className="text-status-warning">
                    {details.find((d) => d.id === slowestId)?.meta?.name ?? slowestId}{' '}
                    <span className="tabular-nums">{formatDuration(nodeMs(slowestId) ?? 0)}</span>
                  </span>
                </span>
              )}
              {maxGapMs > 100 && (
                <span title="节点之间的时间：挂起等待 / 调度与持久化开销，不计入任何节点的耗时">
                  最大节点间等待{' '}
                  <span className="text-ink-primary tabular-nums">{formatDuration(maxGapMs)}</span>
                </span>
              )}
            </>
          ) : (
            <span className="text-ink-faint">
              本次执行没有节点级耗时（老版本 worker 记录的状态，或采集器未挂载）
            </span>
          )}
          {query && <span className="text-ink-faint">筛选命中 {visible.length}/{details.length}</span>}
          <span className="text-ink-faint">展开看耗时明细与输入/配置/输出/后继</span>
          {defLoading && <span className="text-ink-faint">流程定义加载中…</span>}
        </div>
      </div>
      <div className="divide-y divide-line">
        {visible.map((d) => {
          const { kind, text } = jsonSummary(d.output)
          const jumpTarget = d.isRouting && typeof d.output === 'string' ? d.output : null
          const typeLabel = d.meta?.type ?? (d.isRouting ? 'route' : 'unknown')
          const name = d.meta?.name ?? d.id
          const successors = d.meta ? nodeOutgoing(d.meta) : []
          // 实际走向 = 执行顺序里的下一个节点；用它高亮被选中的分支
          const execIndex = details.findIndex((x) => x.id === d.id)
          const takenNextId = execIndex >= 0 ? details[execIndex + 1]?.id : undefined
          return (
            <details key={d.id} className="group px-4 py-2.5">
              <summary className="flex items-center gap-2.5 cursor-pointer select-none list-none">
                <span className="font-mono text-data-sm text-ink-faint tabular-nums w-6 text-right shrink-0">
                  {d.order}
                </span>
                <span className={cn('w-1.5 h-1.5 rounded-full shrink-0', STATUS_DOT[d.status])} />
                <span className="text-data-sm text-ink-primary truncate">{name}</span>
                {name !== d.id && (
                  <span className="font-mono text-caption text-ink-faint truncate shrink-0">{d.id}</span>
                )}
                <span
                  className={cn(
                    'rounded px-1.5 py-0.5 text-[10px] font-mono shrink-0',
                    d.isRouting ? 'bg-inset text-ink-muted' : 'bg-inset text-ink-secondary'
                  )}
                  title={d.meta?.desc}
                >
                  {d.isRouting ? '内部路由' : typeLabel}
                </span>
                <span className={cn('rounded px-1.5 py-0.5 text-[10px] shrink-0', STATUS_CHIP[d.status])}>
                  {STATUS_LABEL[d.status]}
                </span>
                {timingOf(d.id)?.failed && (
                  <span className="rounded px-1.5 py-0.5 text-[10px] shrink-0 bg-status-error-dim text-status-error">
                    节点失败
                  </span>
                )}
                {slowestId === d.id && (
                  <span className="rounded px-1.5 py-0.5 text-[10px] shrink-0 bg-status-warning-dim text-status-warning">
                    最慢
                  </span>
                )}
                <span className="ml-auto flex items-center gap-2 min-w-0">
                  {timingOf(d.id)?.duration_ms != null && (
                    <span className="flex items-center gap-1.5 shrink-0" title="节点耗时（含相对时间轴的位置）">
                      <span className="text-caption tabular-nums text-ink-secondary w-12 text-right">
                        {formatDuration(timingOf(d.id)!.duration_ms)}
                      </span>
                      {span > 0 && (
                        <span className="relative h-1.5 w-20 rounded-full bg-inset overflow-hidden">
                          <span
                            className={cn(
                              'absolute h-full rounded-full',
                              timingOf(d.id)!.failed
                                ? 'bg-status-error/80'
                                : slowestId === d.id
                                  ? 'bg-status-warning/80'
                                  : 'bg-plaita-500/70'
                            )}
                            style={{
                              left: `${Math.min(100, (((timingOf(d.id)!.started_ms ?? spanStart) - spanStart) / span) * 100)}%`,
                              width: `${Math.max(1.5, (((timingOf(d.id)!.ended_ms ?? 0) - (timingOf(d.id)!.started_ms ?? spanStart)) / span) * 100)}%`,
                            }}
                          />
                        </span>
                      )}
                    </span>
                  )}
                  <span className="text-caption text-ink-faint font-mono shrink-0">{kind}</span>
                  <span className="text-caption text-ink-muted truncate max-w-[320px]" title={jumpTarget ? `→ ${jumpTarget}` : text}>
                    {jumpTarget ? `→ ${jumpTarget}` : text}
                  </span>
                </span>
              </summary>
              <div className="mt-3 ml-8 space-y-3.5">
                {d.meta?.desc && <p className="text-caption text-ink-faint">{d.meta.desc}</p>}
                {jumpTarget && (
                  <p className="text-caption text-ink-muted">
                    分支/跳转合成节点：本节点未产生业务输出，随后进入{' '}
                    <span className="font-mono text-ink-secondary">{jumpTarget}</span>
                  </p>
                )}
                {timingOf(d.id)?.duration_ms != null && (
                  <div className="flex items-center gap-x-4 gap-y-1 flex-wrap text-caption text-ink-muted">
                    <span>
                      耗时{' '}
                      <span className="text-ink-primary tabular-nums">
                        {formatDuration(timingOf(d.id)!.duration_ms)}
                      </span>
                    </span>
                    {timingOf(d.id)!.started_ms != null && (
                      <span>
                        开始{' '}
                        <span className="tabular-nums">
                          +{formatDuration((timingOf(d.id)!.started_ms as number) - spanStart)}
                        </span>
                      </span>
                    )}
                    {timingOf(d.id)!.ended_ms != null && (
                      <span>
                        结束{' '}
                        <span className="tabular-nums">
                          +{formatDuration((timingOf(d.id)!.ended_ms as number) - spanStart)}
                        </span>
                      </span>
                    )}
                    {gapMs(d.id) !== null && (
                      <span title="上一个节点结束 → 本节点开始之间的时间（挂起等待/调度），不属于任何节点的耗时">
                        衔接等待 <span className="tabular-nums">{formatDuration(gapMs(d.id))}</span>
                      </span>
                    )}
                    {(timingOf(d.id)!.attempts ?? 1) > 1 && (
                      <span className="text-status-warning">
                        执行 {timingOf(d.id)!.attempts} 次 · 合计{' '}
                        <span className="tabular-nums">{formatDuration(timingOf(d.id)!.total_duration_ms)}</span>
                      </span>
                    )}
                    {execMs !== null && execMs > 0 && (
                      <span>
                        占流程 <span className="tabular-nums">{formatPercent(timingOf(d.id)!.duration_ms as number, execMs)}</span>
                      </span>
                    )}
                    {timingOf(d.id)!.started_at && (
                      <span className="text-ink-faint font-mono">{timingOf(d.id)!.started_at}</span>
                    )}
                  </div>
                )}
                {successors.length > 0 && (
                  <div className="flex items-center gap-2 flex-wrap text-caption">
                    <span className="text-ink-faint shrink-0">
                      {successors.length > 1 ? '分支走向' : '后继'}
                    </span>
                    {successors.map((s) => {
                      const taken = !!takenNextId && s.target === takenNextId
                      const targetName = flowNodes.find((n) => n.id === s.target)?.name ?? s.target
                      return (
                        <span
                          key={`${s.label ?? ''}-${s.target}`}
                          className={cn(
                            'rounded px-1.5 py-0.5 font-mono',
                            taken ? 'bg-status-success-dim text-status-success' : 'bg-inset text-ink-muted'
                          )}
                        >
                          {s.label ? `${s.label} → ` : '→ '}
                          {targetName}
                          {taken && ' · 实际走向'}
                        </span>
                      )
                    })}
                  </div>
                )}
                <NodeIO detail={d} />
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

/**
 * 节点输入/配置/输出三段式（时间线展开区与画布右侧详情面板共用）。
 *
 * 「输入」必须讲清来源：引擎不持久化解析后的入参，这里按可信度标注——
 * 本地模式回调采集的真实入参 > 定义表达式引用的 $NODE.x/$INPUT.y > 上游返回值（推断）。
 */
function NodeIO({ detail }: { detail: ExecutedNodeDetail }) {
  const hasTraceInput = detail.inputs.some((i) => i.kind === 'trace')
  return (
    <div className="space-y-3.5">
      <IOSection
        title="输入"
        hint={hasTraceInput ? '含本地模式采集的真实入参' : '按流程定义表达式解析（$NODE.x / $INPUT.y）'}
        size={detail.inputs.length > 0 ? formatBytes(jsonBytes(detail.inputs.map((i) => i.value))) : undefined}
      >
        {detail.inputs.length === 0 ? (
          <p className="text-caption text-ink-faint">该节点未引用上游数据，也不是流程入口。</p>
        ) : (
          <div className="space-y-2.5">
            {detail.inputs.map((inp) => (
              <div key={inp.label}>
                <div className="flex items-center gap-2 mb-1 flex-wrap">
                  <span className="font-mono text-caption text-ink-secondary">{inp.label}</span>
                  {!inp.present && <span className="text-caption text-status-warning">未执行/无值</span>}
                  {inp.kind === 'upstream' && <span className="text-caption text-ink-faint">推断</span>}
                  <span className="text-[10px] text-ink-faint tabular-nums">
                    {formatBytes(jsonBytes(inp.value))}
                  </span>
                </div>
                <JsonViewer
                  value={inp.value}
                  toolbar={false}
                  defaultExpandDepth={2}
                  maxHeightClass="max-h-52"
                  emptyText="null（本次执行没有值）"
                />
              </div>
            ))}
          </div>
        )}
      </IOSection>
      {detail.config && (
        <IOSection title="配置" hint="流程定义里的静态字段" size={formatBytes(jsonBytes(detail.config))}>
          <JsonViewer value={detail.config} toolbar={false} defaultExpandDepth={2} maxHeightClass="max-h-56" />
        </IOSection>
      )}
      <IOSection
        title="输出"
        hint={detail.hasOutput ? '$NODE 记录' : undefined}
        size={detail.hasOutput ? formatBytes(jsonBytes(detail.output)) : undefined}
      >
        {detail.hasOutput ? (
          <JsonViewer
            value={detail.output}
            toolbar={false}
            defaultExpandDepth={2}
            maxHeightClass="max-h-64"
            emptyText="该节点没有返回值（null）"
          />
        ) : (
          <p className="text-caption text-ink-faint">本次执行没有该节点的记录。</p>
        )}
      </IOSection>
    </div>
  )
}

function IOSection({
  title,
  hint,
  size,
  children,
}: {
  title: string
  hint?: string
  /** 体量标签（如 "1.2 KB"），帮助识别异常大的 payload */
  size?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
        <h4 className="text-caption font-medium text-ink-secondary">{title}</h4>
        {hint && <span className="text-[10px] text-ink-faint">{hint}</span>}
        {size && <span className="text-[10px] text-ink-faint tabular-nums">· {size}</span>}
      </div>
      {children}
    </div>
  )
}

/** 占比：不足 1% 也如实显示「<1%」，不用 0% 误导 */
function formatPercent(part: number, whole: number): string {
  if (!Number.isFinite(part) || !Number.isFinite(whole) || whole <= 0) return '-'
  const ratio = part / whole
  if (ratio > 0 && ratio < 0.01) return '<1%'
  return `${Math.round(ratio * 100)}%`
}

/** 毫秒 → 人读耗时 */
function formatDuration(ms?: number | null): string {
  if (ms === undefined || ms === null || !Number.isFinite(ms)) return '-'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)}s`
  const minutes = Math.floor(ms / 60_000)
  const seconds = Math.round((ms % 60_000) / 1000)
  if (minutes < 60) return `${minutes}m${seconds ? ` ${seconds}s` : ''}`
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
}

/** JSON 体量（UTF-8 字节）：用于发现异常大的输入/输出 */
function jsonBytes(value: unknown): number | null {
  if (value === undefined) return null
  try {
    const text = typeof value === 'string' ? value : JSON.stringify(value)
    if (text === undefined) return null
    return new TextEncoder().encode(text).length
  } catch {
    return null
  }
}

function formatBytes(bytes: number | null): string {
  if (bytes === null) return '-'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`
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