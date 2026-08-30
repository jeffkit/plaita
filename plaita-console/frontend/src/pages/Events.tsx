import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Zap,
  Send,
  Trash2,
  RefreshCw,
  Loader2,
  AlertCircle,
  CheckCircle,
  Radio,
  Filter,
  Clock,
} from 'lucide-react'
import { api, EventInfo, SubscriptionInfo } from '../services/api'
import { PageHeader, Card, Button, EmptyState, Table, Th, Tr, Td, TdData, ConfirmDialog, cn } from '../components/ui'

type Tab = 'subscriptions' | 'events' | 'publish'

export default function Events() {
  const [activeTab, setActiveTab] = useState<Tab>('subscriptions')
  const [eventTypeFilter, setEventTypeFilter] = useState('')

  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: 'subscriptions', label: '事件订阅', icon: <Radio size={14} /> },
    { id: 'events', label: '事件记录', icon: <Zap size={14} /> },
    { id: 'publish', label: '发布事件', icon: <Send size={14} /> },
  ]

  return (
    <div className="p-6 h-full flex flex-col gap-4">
      <PageHeader
        title="事件管理"
        subtitle="事件总线订阅、记录与手动发布"
        actions={
          <div className="relative">
            <Filter className="absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-faint" size={14} />
            <input
              type="text"
              placeholder="按事件类型筛选..."
              value={eventTypeFilter}
              onChange={(e) => setEventTypeFilter(e.target.value)}
              className="input w-56 !pl-8"
            />
          </div>
        }
      />

      {/* 分段 Tab：选中项浮起（Linear 式） */}
      <div className="flex gap-1 bg-inset p-1 rounded-lg border border-line self-start">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={cn(
              'flex items-center gap-1.5 px-3.5 h-8 rounded-md text-body font-medium transition-colors duration-150',
              activeTab === tab.id
                ? 'bg-surface text-ink-primary shadow-card'
                : 'text-ink-muted hover:text-ink-secondary',
            )}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      <div className="flex-1 min-h-0 overflow-auto">
        {activeTab === 'subscriptions' && <SubscriptionsPanel eventType={eventTypeFilter} />}
        {activeTab === 'events' && <EventsPanel eventType={eventTypeFilter} />}
        {activeTab === 'publish' && <PublishPanel />}
      </div>
    </div>
  )
}

function SubscriptionsPanel({ eventType }: { eventType: string }) {
  const queryClient = useQueryClient()
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null)

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['subscriptions', eventType],
    queryFn: () => api.getSubscriptions({ event_type: eventType || undefined }),
    refetchInterval: 10000,
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.deleteSubscription(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['subscriptions'] })
      setPendingDeleteId(null)
    },
  })

  const subscriptions = data?.subscriptions || []

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-caption text-ink-muted">
          共 <span className="font-mono tabular-nums">{data?.total ?? 0}</span> 个活跃订阅
        </p>
        <Button variant="ghost" size="sm" onClick={() => refetch()}>
          <RefreshCw size={13} />
          刷新
        </Button>
      </div>

      {isLoading ? (
        <EmptyState message="加载中…" />
      ) : subscriptions.length === 0 ? (
        <EmptyState icon={<Radio size={20} />} message="暂无事件订阅" />
      ) : (
        <div className="grid gap-3">
          {subscriptions.map((sub) => (
            <SubscriptionCard
              key={sub.subscription_id}
              subscription={sub}
              onDelete={() => setPendingDeleteId(sub.subscription_id)}
              isDeleting={deleteMutation.isPending && pendingDeleteId === sub.subscription_id}
            />
          ))}
        </div>
      )}

      <ConfirmDialog
        open={!!pendingDeleteId}
        title="删除这个事件订阅？"
        variant="danger"
        confirmLabel="确认删除"
        busy={deleteMutation.isPending}
        onCancel={() => setPendingDeleteId(null)}
        onConfirm={() => {
          if (pendingDeleteId) deleteMutation.mutate(pendingDeleteId)
        }}
      >
        删除后，等待该订阅恢复的挂起节点将无法被此订阅唤醒。
      </ConfirmDialog>
    </div>
  )
}

function SubscriptionCard({
  subscription,
  onDelete,
  isDeleting,
}: {
  subscription: SubscriptionInfo
  onDelete: () => void
  isDeleting: boolean
}) {
  return (
    <Card className="p-4 hover:border-line-strong transition-colors">
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2.5 mb-2 flex-wrap">
            <span className="bg-plaita-500/10 text-plaita-400 text-caption font-medium px-2 py-0.5 rounded-md border border-plaita-500/20">
              {subscription.event_type}
            </span>
            {subscription.flow_id && (
              <span className="text-caption text-ink-muted font-mono">
                流程: {subscription.flow_id}
              </span>
            )}
            {subscription.node_id && (
              <span className="text-caption text-ink-muted font-mono">
                节点: {subscription.node_id}
              </span>
            )}
          </div>
          <p className="font-mono text-data-sm text-ink-faint truncate">
            {subscription.subscription_id}
          </p>
          <div className="flex items-center gap-4 mt-2 text-caption text-ink-muted">
            {subscription.created_at && (
              <span className="flex items-center gap-1">
                <Clock size={12} />
                {new Date(subscription.created_at * 1000).toLocaleString()}
              </span>
            )}
            {subscription.timeout && (
              <span>超时: <span className="font-mono tabular-nums">{subscription.timeout}</span>s</span>
            )}
            {subscription.correlation_id && (
              <span className="font-mono">关联: {subscription.correlation_id}</span>
            )}
          </div>
          {subscription.filter_condition && Object.keys(subscription.filter_condition).length > 0 && (
            <div className="mt-2">
              <pre className="text-data-sm text-ink-secondary bg-inset border border-line rounded-md px-2 py-1 overflow-x-auto">
                {JSON.stringify(subscription.filter_condition, null, 2)}
              </pre>
            </div>
          )}
        </div>
        <button
          onClick={onDelete}
          disabled={isDeleting}
          className="p-1.5 rounded-md text-ink-faint hover:text-status-error hover:bg-status-error-dim transition-colors shrink-0"
          title="删除订阅"
        >
          <Trash2 size={15} />
        </button>
      </div>
    </Card>
  )
}

function EventsPanel({ eventType }: { eventType: string }) {
  const { data, isLoading, refetch } = useQuery({
    queryKey: ['events', eventType],
    queryFn: () => api.getEvents({ event_type: eventType || undefined, limit: 100 }),
    refetchInterval: 10000,
  })

  const events = data?.events || []

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-caption text-ink-muted">
          共 <span className="font-mono tabular-nums">{data?.total ?? 0}</span> 条事件记录
        </p>
        <Button variant="ghost" size="sm" onClick={() => refetch()}>
          <RefreshCw size={13} />
          刷新
        </Button>
      </div>

      {isLoading ? (
        <EmptyState message="加载中…" />
      ) : events.length === 0 ? (
        <EmptyState icon={<Zap size={20} />} message="暂无事件记录" />
      ) : (
        <Card className="overflow-hidden">
          <Table>
            <thead>
              <tr>
                <Th>事件类型</Th>
                <Th>事件 ID</Th>
                <Th>来源</Th>
                <Th>时间</Th>
                <Th>数据</Th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <EventRow key={event.event_id} event={event} />
              ))}
            </tbody>
          </Table>
        </Card>
      )}
    </div>
  )
}

function EventRow({ event }: { event: EventInfo }) {
  const [expanded, setExpanded] = useState(false)

  return (
    <>
      <Tr className="cursor-pointer" onClick={() => setExpanded(!expanded)}>
        <Td>
          <span className="bg-plaita-500/10 text-plaita-400 text-caption font-medium px-2 py-0.5 rounded-md border border-plaita-500/20">
            {event.event_type}
          </span>
        </Td>
        <TdData className="max-w-[200px] truncate text-ink-muted">{event.event_id}</TdData>
        <Td>{event.source || '-'}</Td>
        <TdData className="text-ink-muted">
          {event.timestamp ? new Date(event.timestamp * 1000).toLocaleString() : '-'}
        </TdData>
        <TdData className="max-w-[300px] truncate text-ink-faint">
          {JSON.stringify(event.data)}
        </TdData>
      </Tr>
      {expanded && (
        <tr className="bg-inset">
          <td colSpan={5} className="px-4 py-3">
            <pre className="text-data-sm text-ink-secondary whitespace-pre-wrap font-mono">
              {JSON.stringify(event, null, 2)}
            </pre>
          </td>
        </tr>
      )}
    </>
  )
}

function PublishPanel() {
  const queryClient = useQueryClient()
  const [eventType, setEventType] = useState('')
  const [eventData, setEventData] = useState('{\n  \n}')
  const [correlationId, setCorrelationId] = useState('')
  const [jsonError, setJsonError] = useState('')
  const [publishResult, setPublishResult] = useState<{ success: boolean; message: string; event_id?: string } | null>(null)

  const publishMutation = useMutation({
    mutationFn: (params: { event_type: string; data: Record<string, unknown>; correlation_id?: string }) =>
      api.publishEvent(params),
    onSuccess: (result) => {
      setPublishResult({ success: true, message: result.message, event_id: result.event_id })
      queryClient.invalidateQueries({ queryKey: ['events'] })
    },
    onError: (error: Error) => {
      setPublishResult({ success: false, message: error.message })
    },
  })

  const handlePublish = () => {
    if (!eventType.trim()) return
    let data: Record<string, unknown> = {}
    if (eventData.trim()) {
      try {
        data = JSON.parse(eventData)
        setJsonError('')
      } catch {
        setJsonError('JSON 格式错误')
        return
      }
    }
    setPublishResult(null)
    publishMutation.mutate({
      event_type: eventType,
      data,
      correlation_id: correlationId || undefined,
    })
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
    <div className="max-w-2xl space-y-4">
      <Card className="p-5 space-y-4">
        <div>
          <label className="block text-body font-medium text-ink-secondary mb-1.5">
            事件类型 <span className="text-status-error">*</span>
          </label>
          <input
            type="text"
            value={eventType}
            onChange={(e) => setEventType(e.target.value)}
            placeholder="例如: approval_completed, order_created"
            className="input"
          />
        </div>

        <div>
          <label className="block text-body font-medium text-ink-secondary mb-1.5">
            关联 ID (可选)
          </label>
          <input
            type="text"
            value={correlationId}
            onChange={(e) => setCorrelationId(e.target.value)}
            placeholder="用于关联订阅，例如 execution_id 或 flow_id"
            className="input"
          />
        </div>

        <div>
          <div className="flex items-center justify-between mb-1.5">
            <label className="text-body font-medium text-ink-secondary">
              事件数据 (JSON)
            </label>
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
            rows={8}
            className="input font-mono text-data-sm resize-none"
            placeholder='{"approved": true, "approver": "admin"}'
          />
          {jsonError && (
            <p className="text-caption text-status-error mt-1 flex items-center gap-1">
              <AlertCircle size={12} /> {jsonError}
            </p>
          )}
        </div>

        <Button
          variant="primary"
          onClick={handlePublish}
          disabled={!eventType.trim() || publishMutation.isPending}
        >
          {publishMutation.isPending ? (
            <Loader2 size={14} className="animate-spin" />
          ) : (
            <Send size={14} />
          )}
          发布事件
        </Button>
      </Card>

      {publishResult && (
        <div
          className={cn(
            'flex items-start gap-3 p-4 rounded-xl border',
            publishResult.success
              ? 'bg-status-success-dim border-status-success/30 text-status-success'
              : 'bg-status-error-dim border-status-error/30 text-status-error',
          )}
        >
          {publishResult.success ? <CheckCircle size={16} /> : <AlertCircle size={16} />}
          <div>
            <p className="text-body font-medium">{publishResult.message}</p>
            {publishResult.event_id && (
              <p className="text-caption opacity-70 mt-1 font-mono">
                Event ID: {publishResult.event_id}
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
