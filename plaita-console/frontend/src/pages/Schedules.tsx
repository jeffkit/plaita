import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useInfiniteQuery, useQueryClient } from '@tanstack/react-query'
import {
  Plus,
  RefreshCw,
  Trash2,
  Play,
  Pause,
  Zap,
  History,
  Pencil,
  AlertCircle,
  Check,
  Clock,
} from 'lucide-react'
import { api, ScheduleInfo, ScheduleFireRecord, FlowSummaryView } from '../services/api'
import { ServiceHint } from '../components/ServiceHint'
import {
  Page,
  PageHeader,
  Card,
  Button,
  StatusBadge,
  EmptyState,
  ErrorState,
  Table,
  Th,
  Tr,
  Td,
  TdData,
  ConfirmDialog,
} from '../components/ui'

export default function Schedules() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [showDialog, setShowDialog] = useState(false)
  const [editing, setEditing] = useState<ScheduleInfo | null>(null)
  const [historyOf, setHistoryOf] = useState<ScheduleInfo | null>(null)
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['schedules'],
    queryFn: api.getSchedules,
    refetchInterval: 5000,
  })

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['schedules'] })

  const toggleMutation = useMutation({
    mutationFn: ({ id, enable }: { id: string; enable: boolean }) =>
      enable ? api.enableSchedule(id) : api.disableSchedule(id),
    onSuccess: invalidate,
    onError: (e: Error) => setActionError(e.message),
  })

  const triggerMutation = useMutation({
    mutationFn: api.triggerSchedule,
    onSuccess: invalidate,
    onError: (e: Error) => setActionError(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: api.deleteSchedule,
    onSuccess: () => {
      invalidate()
      setConfirmDeleteId(null)
    },
    onError: (e: Error) => setActionError(e.message),
  })

  const schedules = data?.schedules || []

  const openCreate = () => {
    setEditing(null)
    setShowDialog(true)
  }
  const openEdit = (s: ScheduleInfo) => {
    setEditing(s)
    setShowDialog(true)
  }

  return (
    <Page>
      <PageHeader
        title="触发器"
        subtitle="定时触发流程执行（cron，本地时区）"
        actions={
          <>
            <Button variant="ghost" onClick={() => refetch()}>
              <RefreshCw size={14} />
              刷新
            </Button>
            <Button variant="primary" onClick={openCreate}>
              <Plus size={14} />
              新建触发器
            </Button>
          </>
        }
      />

      {/* 调度服务不在线时的提示：触发器只在调度服务运行时才会生效 */}
      <SchedulerHint />

      {actionError && (
        <div className="px-3 py-2 bg-status-error-dim text-status-error text-caption rounded-md">
          {actionError}
        </div>
      )}

      <Card className="overflow-hidden">
        <Table>
          <thead>
            <tr>
              <Th>名称</Th>
              <Th>流程</Th>
              <Th>Cron</Th>
              <Th>状态</Th>
              <Th>下次触发</Th>
              <Th>上次触发</Th>
              <Th className="text-right">操作</Th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr>
                <td colSpan={7}>
                  <EmptyState message="加载中…" />
                </td>
              </tr>
            ) : isError ? (
              <tr>
                <td colSpan={7}>
                  <ErrorState error={error} title="触发器列表加载失败" onRetry={() => refetch()} />
                </td>
              </tr>
            ) : schedules.length === 0 ? (
              <tr>
                <td colSpan={7}>
                  <EmptyState
                    icon={<Clock size={20} />}
                    message="暂无触发器"
                    hint="新建一个触发器，按 cron 周期自动启动流程"
                  />
                </td>
              </tr>
            ) : (
              schedules.map((s) => {
                const enqueueFailed =
                  s.last_fired_at && (s.last_enqueue_ok === false || s.last_enqueue_ok === '0')
                return (
                  <Tr key={s.schedule_id}>
                    <Td className="text-ink-primary">{s.name}</Td>
                    <Td>
                      <button
                        onClick={() => navigate(`/flows/${s.flow_id}/edit${s.version ? `?version=${encodeURIComponent(s.version)}` : ''}`)}
                        className="font-mono text-data-sm text-plaita-400 hover:underline"
                        title="打开流程编辑器"
                      >
                        {s.flow_id}
                        {s.version ? `@${s.version}` : ''}
                      </button>
                    </Td>
                    <TdData>{s.cron}</TdData>
                    <Td>
                      <StatusBadge status={s.enabled ? 'active' : 'paused'} />
                    </Td>
                    <TdData className="text-ink-muted">
                      {s.enabled && s.next_run_at ? formatTime(s.next_run_at) : '—'}
                    </TdData>
                    <Td>
                      {s.last_fired_at ? (
                        <span className="flex items-center gap-1.5">
                          <span className="font-mono text-data-sm text-ink-muted">
                            {formatTime(s.last_fired_at)}
                          </span>
                          {enqueueFailed ? (
                            <span className="text-status-error text-caption" title="上次入队失败">
                              ✕
                            </span>
                          ) : null}
                        </span>
                      ) : (
                        <span className="font-mono text-data-sm text-ink-muted">—</span>
                      )}
                    </Td>
                    <Td>
                      <div className="flex items-center justify-end gap-0.5">
                        <button
                          onClick={() => triggerMutation.mutate(s.schedule_id)}
                          disabled={triggerMutation.isPending}
                          className="p-1.5 rounded-md text-plaita-400 hover:bg-plaita-500/10 transition-colors"
                          title="立即触发一次"
                        >
                          <Zap size={15} />
                        </button>
                        <button
                          onClick={() => setHistoryOf(s)}
                          className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors"
                          title="触发历史"
                        >
                          <History size={15} />
                        </button>
                        {s.enabled ? (
                          <button
                            onClick={() => toggleMutation.mutate({ id: s.schedule_id, enable: false })}
                            disabled={toggleMutation.isPending}
                            className="p-1.5 rounded-md text-ink-muted hover:text-status-warning hover:bg-status-warning-dim transition-colors"
                            title="暂停"
                          >
                            <Pause size={15} />
                          </button>
                        ) : (
                          <button
                            onClick={() => toggleMutation.mutate({ id: s.schedule_id, enable: true })}
                            disabled={toggleMutation.isPending}
                            className="p-1.5 rounded-md text-ink-muted hover:text-status-running hover:bg-status-running-dim transition-colors"
                            title="启用"
                          >
                            <Play size={15} />
                          </button>
                        )}
                        <button
                          onClick={() => openEdit(s)}
                          className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors"
                          title="编辑"
                        >
                          <Pencil size={15} />
                        </button>
                        <button
                          onClick={() => setConfirmDeleteId(s.schedule_id)}
                          disabled={deleteMutation.isPending}
                          className="p-1.5 rounded-md text-ink-muted hover:text-status-error hover:bg-status-error-dim transition-colors"
                          title="删除"
                        >
                          <Trash2 size={15} />
                        </button>
                      </div>
                    </Td>
                  </Tr>
                )
              })
            )}
          </tbody>
        </Table>
      </Card>

      {showDialog && (
        <ScheduleDialog
          editing={editing}
          onClose={() => setShowDialog(false)}
        />
      )}
      {historyOf && <HistoryDialog schedule={historyOf} onClose={() => setHistoryOf(null)} />}

      <ConfirmDialog
        open={!!confirmDeleteId}
        title="删除这个触发器？"
        variant="danger"
        confirmLabel="确认删除"
        busy={deleteMutation.isPending}
        onCancel={() => setConfirmDeleteId(null)}
        onConfirm={() => {
          if (confirmDeleteId) deleteMutation.mutate(confirmDeleteId)
        }}
      >
        删除后不会再按周期触发流程，触发历史一并清除。
      </ConfirmDialog>
    </Page>
  )
}

/** 调度服务（schedule_service）注册状态提示：没有它，触发器只是配置 */
function SchedulerHint() {
  return (
    <ServiceHint
      serviceType="schedule_service"
      message="调度服务（schedule_service）未运行，触发器不会生效——请先在「集群管理」启动调度服务"
    />
  )
}

// ============ 新建 / 编辑对话框 ============

function ScheduleDialog({
  editing,
  onClose,
}: {
  editing: ScheduleInfo | null
  onClose: () => void
}) {
  const queryClient = useQueryClient()
  const [name, setName] = useState(editing?.name || '')
  const [flowId, setFlowId] = useState(editing?.flow_id || '')
  const [flowSearch, setFlowSearch] = useState('')
  const [version, setVersion] = useState(editing?.version || '')
  const [cron, setCron] = useState(editing?.cron || '')
  const [paramsJson, setParamsJson] = useState(
    editing?.params && Object.keys(editing.params).length
      ? JSON.stringify(editing.params, null, 2)
      : '{\n  \n}'
  )
  const [jsonError, setJsonError] = useState<string | null>(null)
  const [cronError, setCronError] = useState<string | null>(null)
  const [submitError, setSubmitError] = useState<string | null>(null)

  const flowsQuery = useQuery({
    queryKey: ['flows'],
    queryFn: api.getFlows,
  })
  const flows = (flowsQuery.data?.flows || []) as FlowSummaryView[]
  const matchedFlows = flows.filter(
    (f) =>
      !flowSearch.trim() ||
      f.flow_id.toLowerCase().includes(flowSearch.trim().toLowerCase()) ||
      (f.desc || '').toLowerCase().includes(flowSearch.trim().toLowerCase())
  )

  const flowDetailQuery = useQuery({
    queryKey: ['flow', flowId],
    queryFn: () => api.getFlow(flowId),
    enabled: !!flowId,
  })
  const versions = (flowDetailQuery.data?.versions || []) as Array<{ version: string; status?: string }>
  const defaultVersion =
    versions.find((v) => v.status === 'published')?.version || versions[versions.length - 1]?.version || ''

  // cron 防抖预览：未来 5 次触发时间
  const [preview, setPreview] = useState<string[]>([])
  const [previewing, setPreviewing] = useState(false)
  useEffect(() => {
    if (!cron.trim()) {
      setPreview([])
      setCronError(null)
      return
    }
    setPreviewing(true)
    const t = setTimeout(async () => {
      try {
        const res = await api.previewCron(cron, 5)
        setPreview(res.next)
        setCronError(null)
      } catch (e) {
        setPreview([])
        setCronError((e as Error).message)
      } finally {
        setPreviewing(false)
      }
    }, 400)
    return () => clearTimeout(t)
  }, [cron])

  const saveMutation = useMutation({
    mutationFn: async () => {
      let params: Record<string, unknown> = {}
      if (paramsJson.trim()) {
        params = JSON.parse(paramsJson)
      }
      const payload = {
        name: name.trim(),
        // A2R57-04：编辑态 version 原样透传——空串=显式解钉（跟随 latest），
        // 不再用 defaultVersion 兜底静默重钉（跟随 latest 行打开编辑时
        // SELECT 显示 defaultVersion，不动版本直接保存曾把「跟随 latest」
        // 静默钉成 defaultVersion）。新建仍兜底 defaultVersion（创建面
        // 既定语义：不选版本=钉当前默认版）。
        version: editing ? version : version || defaultVersion || undefined,
        cron,
        params,
      }
      if (editing) {
        // A2R55-02：带打开对话框时行的 updated_at 作乐观锁基准，并发编辑由
        // 后端 409 schedule_stale 拒绝（不再静默 last-write-wins）。
        // flow_id 只进 create——update 模型 extra=forbid 不吃该字段，此前把
        // 共用 payload 直塞 PUT 必 422 extra_forbidden（r86 探针 F1 实证）。
        return api.updateSchedule(editing.schedule_id, {
          ...payload,
          ...(editing.updated_at ? { expected_updated_at: editing.updated_at } : {}),
        })
      }
      return api.createSchedule({ ...payload, flow_id: flowId })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schedules'] })
      onClose()
    },
    onError: (e: Error) => {
      const detail = (e as Error & { detail?: { conflict?: string } }).detail
      if (detail?.conflict === 'schedule_stale') {
        // A2R55-02：具名提示并发冲突，让用户重读最新配置再改
        setSubmitError(
          '并发冲突：该触发器已被其他编辑更新，本次保存未生效。请关闭对话框后重新打开（自动加载最新配置）再保存'
        )
      } else {
        setSubmitError(e.message)
      }
    },
  })

  const paramsValid = (() => {
    if (!paramsJson.trim()) return true
    try {
      JSON.parse(paramsJson)
      return true
    } catch {
      return false
    }
  })()

  const canSubmit = !!name.trim() && !!flowId && !!cron.trim() && !cronError && paramsValid && !previewing

  const formatJson = () => {
    try {
      setParamsJson(JSON.stringify(JSON.parse(paramsJson), null, 2))
      setJsonError(null)
    } catch (e) {
      setJsonError((e as Error).message)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center animate-fade">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label={editing ? '编辑触发器' : '新建触发器'}
        className="relative bg-elevated border border-line-strong rounded-xl shadow-pop w-full max-w-xl max-h-[90vh] overflow-hidden animate-pop"
      >
        <div className="flex items-center justify-between px-6 py-4 border-b border-line">
          <h2 className="text-section text-ink-primary">{editing ? '编辑触发器' : '新建触发器'}</h2>
          <button
            onClick={onClose}
            aria-label="关闭"
            className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-surface transition-colors"
          >
            ✕
          </button>
        </div>

        <div className="p-6 space-y-4 overflow-y-auto max-h-[60vh]">
          <div>
            <label className="block text-caption text-ink-muted mb-1.5">名称</label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="如：每日内容生产"
              className="input w-full"
            />
          </div>

          <div>
            <label className="block text-caption text-ink-muted mb-1.5">流程</label>
            {editing ? (
              <p className="font-mono text-data-sm text-ink-primary">{editing.flow_id}（不可更改）</p>
            ) : (
              <>
                <input
                  value={flowSearch}
                  onChange={(e) => setFlowSearch(e.target.value)}
                  placeholder="输入 ID 或描述过滤…"
                  className="input w-full mb-2"
                />
                <select
                  value={flowId}
                  onChange={(e) => {
                    setFlowId(e.target.value)
                    setVersion('')
                  }}
                  className="input w-full font-mono"
                  size={Math.min(4, Math.max(2, matchedFlows.length))}
                >
                  {matchedFlows.length === 0 && <option value="">无匹配流程</option>}
                  {matchedFlows.map((f) => (
                    <option key={f.flow_id} value={f.flow_id}>
                      {f.flow_id}
                      {f.desc ? ` — ${f.desc}` : ''}
                    </option>
                  ))}
                </select>
              </>
            )}
          </div>

          <div>
            <label className="block text-caption text-ink-muted mb-1.5">版本</label>
            <select
              value={version || (editing ? '' : defaultVersion)}
              onChange={(e) => setVersion(e.target.value)}
              className="input w-full font-mono"
              disabled={!flowId}
            >
              {/* A2R57-04：编辑态对 version=null（跟随 latest）行如实显示
                  占位，不再借位显示 defaultVersion 诱导静默重钉 */}
              {editing && <option value="">跟随最新（不钉版本）</option>}
              {!flowId && <option value="">先选择流程</option>}
              {!editing && flowId && versions.length === 0 && (
                <option value="">（运行时取最新）</option>
              )}
              {versions.map((v) => (
                <option key={v.version} value={v.version}>
                  {v.version}（{v.status === 'published' ? '已发布' : v.status === 'draft' ? '草稿' : v.status}）
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-caption text-ink-muted mb-1.5">
              Cron 表达式 <span className="text-ink-faint">（分 时 日 月 周，本地时区）</span>
            </label>
            <input
              value={cron}
              onChange={(e) => setCron(e.target.value)}
              placeholder="如：0 9 * * 1-5（工作日每天 09:00）"
              className={`input w-full font-mono ${cronError ? '!border-status-error/50' : ''}`}
            />
            {cronError && (
              <p className="text-caption text-status-error mt-1.5 flex items-center gap-1">
                <AlertCircle size={12} /> {cronError}
              </p>
            )}
            {previewing && <p className="text-caption text-ink-faint mt-1.5">计算触发时间…</p>}
            {preview.length > 0 && (
              <div className="mt-2 px-3 py-2 bg-inset border border-line rounded-md">
                <p className="text-caption text-ink-muted mb-1">接下来 5 次触发</p>
                {preview.map((t, i) => (
                  <p key={i} className="font-mono text-data-sm text-ink-secondary">
                    {formatTime(t)}
                  </p>
                ))}
              </div>
            )}
          </div>

          <div>
            <div className="flex items-center justify-between mb-1.5">
              <label className="text-caption text-ink-muted">输入参数 (JSON)</label>
              <button onClick={formatJson} className="text-caption text-plaita-400 hover:text-plaita-300 transition-colors">
                格式化
              </button>
            </div>
            <textarea
              value={paramsJson}
              onChange={(e) => {
                setParamsJson(e.target.value)
                setJsonError(null)
              }}
              rows={5}
              spellCheck={false}
              className={`input w-full font-mono text-data-sm resize-none ${jsonError ? '!border-status-error/50' : ''}`}
            />
            {jsonError && (
              <p className="text-caption text-status-error mt-1.5 flex items-center gap-1">
                <AlertCircle size={12} /> {jsonError}
              </p>
            )}
            {paramsValid && paramsJson.trim() && (
              <p className="text-caption text-plaita-400 mt-1 flex items-center gap-1">
                <Check size={12} /> JSON 有效
              </p>
            )}
          </div>

          {submitError && (
            <p className="text-caption text-status-error flex items-center gap-1">
              <AlertCircle size={12} /> {submitError}
            </p>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 px-6 py-4 border-t border-line bg-surface">
          <Button variant="secondary" onClick={onClose}>
            取消
          </Button>
          <Button variant="primary" disabled={!canSubmit || saveMutation.isPending} onClick={() => saveMutation.mutate()}>
            {editing ? '保存' : '创建'}
          </Button>
        </div>
      </div>
    </div>
  )
}

// ============ 触发历史对话框 ============

function HistoryDialog({ schedule, onClose }: { schedule: ScheduleInfo; onClose: () => void }) {
  // A2R55-01：truncated/next_offset 必须可消费——第 21 条起的触发史不再不可达
  const {
    data,
    isLoading,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ['schedule-history', schedule.schedule_id],
    queryFn: ({ pageParam }) => api.getScheduleHistory(schedule.schedule_id, 20, pageParam),
    initialPageParam: 0,
    getNextPageParam: (last) =>
      last.truncated && last.next_offset != null ? last.next_offset : undefined,
  })
  const pages = data?.pages ?? []
  const records: ScheduleFireRecord[] = pages.flatMap((p) => p.records)
  const total = pages[0]?.total ?? 0

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center animate-fade">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="触发历史"
        className="relative bg-elevated border border-line-strong rounded-xl shadow-pop w-full max-w-xl overflow-hidden animate-pop"
      >
        <div className="flex items-center justify-between px-6 py-4 border-b border-line">
          <h2 className="text-section text-ink-primary">触发历史 · {schedule.name}</h2>
          <button
            onClick={onClose}
            aria-label="关闭"
            className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-surface transition-colors"
          >
            ✕
          </button>
        </div>
        <div className="p-4 max-h-[55vh] overflow-y-auto">
          {isLoading ? (
            <EmptyState message="加载中…" />
          ) : records.length === 0 ? (
            <EmptyState icon={<Clock size={20} />} message="还没有触发记录" />
          ) : (
            <div className="space-y-1.5">
              {pages.length > 1 && (
                <p className="text-caption text-ink-muted">
                  已显示 <span className="font-mono tabular-nums">{records.length}</span> /{' '}
                  <span className="font-mono tabular-nums">{total}</span> 条
                </p>
              )}
              {records.map((r, i) => (
                <div key={i} className="flex items-center gap-3 px-3 py-2 bg-inset border border-line rounded-md">
                  <span className="font-mono text-data-sm text-ink-secondary tabular-nums">
                    {formatTime(r.fired_at)}
                  </span>
                  <span className="text-caption text-ink-faint">
                    {r.trigger_kind === 'manual' ? '手动' : '周期'}
                  </span>
                  <span
                    className={`ml-auto text-caption ${
                      r.enqueue === 'ok'
                        ? 'text-status-success'
                        : r.enqueue === 'rejected'
                          ? 'text-status-warning'
                          : 'text-status-error'
                    }`}
                  >
                    {r.enqueue === 'ok'
                      ? '已入队'
                      : r.enqueue === 'rejected'
                        ? `已拒绝（${rejectionLabel(r.rejection_reason)}）`
                        : '入队失败'}
                  </span>
                </div>
              ))}
              {hasNextPage && (
                <button
                  onClick={() => fetchNextPage()}
                  disabled={isFetchingNextPage}
                  className="mt-2 w-full py-2 rounded-md border border-line text-caption text-plaita-400 hover:bg-elevated/60 transition-colors disabled:text-ink-faint"
                >
                  {isFetchingNextPage
                    ? '加载中…'
                    : `加载更多（已显示 ${records.length} / 共 ${total} 条）`}
                </button>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

/** 拒绝原因 → 可读标签（B44-01；未知原因原样展示，不吞） */
function rejectionLabel(reason?: string | null): string {
  if (reason === 'tenant_disabled') return '租户已停用'
  if (reason === 'tenant_quota_exhausted') return '租户配额已满'
  return reason || '原因未知'
}

/** 时间格式化：epoch 毫秒（数字或数字字符串）或 ISO 字符串 → 本地可读 */
function formatTime(input: string | number): string {
  // Redis 里的 next_run_at 是数字字符串，先归一成数值
  const asNum = typeof input === 'string' && /^\d+$/.test(input) ? Number(input) : input
  const d = typeof asNum === 'number' ? new Date(asNum) : new Date(input)
  if (Number.isNaN(d.getTime())) return String(input)
  return d.toLocaleString()
}
