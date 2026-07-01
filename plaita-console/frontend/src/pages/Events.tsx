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

type Tab = 'subscriptions' | 'events' | 'publish'

export default function Events() {
  const [activeTab, setActiveTab] = useState<Tab>('subscriptions')
  const [eventTypeFilter, setEventTypeFilter] = useState('')

  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: 'subscriptions', label: '事件订阅', icon: <Radio size={16} /> },
    { id: 'events', label: '事件记录', icon: <Zap size={16} /> },
    { id: 'publish', label: '发布事件', icon: <Send size={16} /> },
  ]

  return (
    <div className="p-8 h-full flex flex-col">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-3xl font-bold">事件管理</h1>
        <div className="relative">
          <Filter className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-400" size={16} />
          <input
            type="text"
            placeholder="按事件类型筛选..."
            value={eventTypeFilter}
            onChange={(e) => setEventTypeFilter(e.target.value)}
            className="bg-dark-700 border border-dark-600 rounded-lg pl-10 pr-4 py-2 text-sm w-56 focus:outline-none focus:ring-2 focus:ring-plaita-500"
          />
        </div>
      </div>

      <div className="flex gap-1 mb-6 bg-dark-800/50 p-1 rounded-xl border border-dark-700 self-start">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeTab === tab.id
                ? 'bg-plaita-500/20 text-plaita-400'
                : 'text-dark-400 hover:text-dark-200 hover:bg-dark-700'
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      <div className="flex-1 min-h-0">
        {activeTab === 'subscriptions' && <SubscriptionsPanel eventType={eventTypeFilter} />}
        {activeTab === 'events' && <EventsPanel eventType={eventTypeFilter} />}
        {activeTab === 'publish' && <PublishPanel />}
      </div>
    </div>
  )
}

function SubscriptionsPanel({ eventType }: { eventType: string }) {
  const queryClient = useQueryClient()

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['subscriptions', eventType],
    queryFn: () => api.getSubscriptions({ event_type: eventType || undefined }),
    refetchInterval: 10000,
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.deleteSubscription(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['subscriptions'] }),
  })

  const subscriptions = data?.subscriptions || []

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-dark-400">
          共 {data?.total ?? 0} 个活跃订阅
        </p>
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 bg-dark-700 hover:bg-dark-600 px-3 py-1.5 rounded-lg text-sm transition-colors"
        >
          <RefreshCw size={14} />
          刷新
        </button>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-20 text-dark-400">
          <Loader2 className="animate-spin mr-2" size={20} />
          加载中...
        </div>
      ) : subscriptions.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-dark-400">
          <Radio size={48} className="mb-4 opacity-30" />
          <p>暂无事件订阅</p>
        </div>
      ) : (
        <div className="grid gap-3">
          {subscriptions.map((sub) => (
            <SubscriptionCard
              key={sub.subscription_id}
              subscription={sub}
              onDelete={() => deleteMutation.mutate(sub.subscription_id)}
              isDeleting={deleteMutation.isPending}
            />
          ))}
        </div>
      )}
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
    <div className="bg-dark-800/50 border border-dark-700 rounded-xl p-4 hover:border-dark-600 transition-colors">
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3 mb-2">
            <span className="bg-plaita-500/20 text-plaita-400 text-xs font-medium px-2.5 py-0.5 rounded-full">
              {subscription.event_type}
            </span>
            {subscription.flow_id && (
              <span className="text-xs text-dark-400">
                流程: {subscription.flow_id}
              </span>
            )}
            {subscription.node_id && (
              <span className="text-xs text-dark-400">
                节点: {subscription.node_id}
              </span>
            )}
          </div>
          <p className="font-mono text-xs text-dark-500 truncate">
            {subscription.subscription_id}
          </p>
          <div className="flex items-center gap-4 mt-2 text-xs text-dark-400">
            {subscription.created_at && (
              <span className="flex items-center gap-1">
                <Clock size={12} />
                {new Date(subscription.created_at * 1000).toLocaleString()}
              </span>
            )}
            {subscription.timeout && (
              <span>超时: {subscription.timeout}s</span>
            )}
            {subscription.correlation_id && (
              <span>关联: {subscription.correlation_id}</span>
            )}
          </div>
          {subscription.filter_condition && Object.keys(subscription.filter_condition).length > 0 && (
            <div className="mt-2">
              <pre className="text-xs text-dark-400 bg-dark-900/50 rounded px-2 py-1 overflow-x-auto">
                {JSON.stringify(subscription.filter_condition, null, 2)}
              </pre>
            </div>
          )}
        </div>
        <button
          onClick={onDelete}
          disabled={isDeleting}
          className="p-2 text-dark-500 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-colors"
          title="删除订阅"
        >
          <Trash2 size={16} />
        </button>
      </div>
    </div>
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
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-dark-400">
          共 {data?.total ?? 0} 条事件记录
        </p>
        <button
          onClick={() => refetch()}
          className="flex items-center gap-2 bg-dark-700 hover:bg-dark-600 px-3 py-1.5 rounded-lg text-sm transition-colors"
        >
          <RefreshCw size={14} />
          刷新
        </button>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-20 text-dark-400">
          <Loader2 className="animate-spin mr-2" size={20} />
          加载中...
        </div>
      ) : events.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-dark-400">
          <Zap size={48} className="mb-4 opacity-30" />
          <p>暂无事件记录</p>
        </div>
      ) : (
        <div className="bg-dark-800/50 border border-dark-700 rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-dark-700 text-dark-400 text-left">
                <th className="px-4 py-3 font-medium">事件类型</th>
                <th className="px-4 py-3 font-medium">事件 ID</th>
                <th className="px-4 py-3 font-medium">来源</th>
                <th className="px-4 py-3 font-medium">时间</th>
                <th className="px-4 py-3 font-medium">数据</th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <EventRow key={event.event_id} event={event} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function EventRow({ event }: { event: EventInfo }) {
  const [expanded, setExpanded] = useState(false)

  return (
    <>
      <tr
        className="border-b border-dark-700/50 hover:bg-dark-700/30 cursor-pointer transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <td className="px-4 py-3">
          <span className="bg-blue-500/15 text-blue-400 text-xs font-medium px-2 py-0.5 rounded-full">
            {event.event_type}
          </span>
        </td>
        <td className="px-4 py-3 font-mono text-xs text-dark-400 max-w-[200px] truncate">
          {event.event_id}
        </td>
        <td className="px-4 py-3 text-dark-300">
          {event.source || '-'}
        </td>
        <td className="px-4 py-3 text-dark-400">
          {event.timestamp ? new Date(event.timestamp * 1000).toLocaleString() : '-'}
        </td>
        <td className="px-4 py-3 text-dark-400 text-xs font-mono max-w-[300px] truncate">
          {JSON.stringify(event.data)}
        </td>
      </tr>
      {expanded && (
        <tr className="bg-dark-900/50">
          <td colSpan={5} className="px-4 py-3">
            <pre className="text-xs text-dark-300 whitespace-pre-wrap font-mono">
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
    <div className="max-w-2xl space-y-6">
      <div className="bg-dark-800/50 border border-dark-700 rounded-xl p-6 space-y-5">
        <div>
          <label className="block text-sm font-medium text-dark-300 mb-2">
            事件类型 <span className="text-red-400">*</span>
          </label>
          <input
            type="text"
            value={eventType}
            onChange={(e) => setEventType(e.target.value)}
            placeholder="例如: approval_completed, order_created"
            className="w-full bg-dark-900 border border-dark-600 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-dark-300 mb-2">
            关联 ID (可选)
          </label>
          <input
            type="text"
            value={correlationId}
            onChange={(e) => setCorrelationId(e.target.value)}
            placeholder="用于关联订阅，例如 execution_id 或 flow_id"
            className="w-full bg-dark-900 border border-dark-600 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500"
          />
        </div>

        <div>
          <div className="flex items-center justify-between mb-2">
            <label className="text-sm font-medium text-dark-300">
              事件数据 (JSON)
            </label>
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
            rows={8}
            className="w-full bg-dark-900 border border-dark-600 rounded-lg px-4 py-3 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500 resize-none"
            placeholder='{"approved": true, "approver": "admin"}'
          />
          {jsonError && (
            <p className="text-xs text-red-400 mt-1 flex items-center gap-1">
              <AlertCircle size={12} /> {jsonError}
            </p>
          )}
        </div>

        <button
          onClick={handlePublish}
          disabled={!eventType.trim() || publishMutation.isPending}
          className="flex items-center gap-2 bg-plaita-500 hover:bg-plaita-600 disabled:opacity-50 px-6 py-2.5 rounded-lg text-sm font-medium transition-colors"
        >
          {publishMutation.isPending ? (
            <Loader2 size={16} className="animate-spin" />
          ) : (
            <Send size={16} />
          )}
          发布事件
        </button>
      </div>

      {publishResult && (
        <div
          className={`flex items-start gap-3 p-4 rounded-xl border ${
            publishResult.success
              ? 'bg-green-500/10 border-green-500/30 text-green-400'
              : 'bg-red-500/10 border-red-500/30 text-red-400'
          }`}
        >
          {publishResult.success ? <CheckCircle size={18} /> : <AlertCircle size={18} />}
          <div>
            <p className="text-sm font-medium">{publishResult.message}</p>
            {publishResult.event_id && (
              <p className="text-xs opacity-70 mt-1 font-mono">
                Event ID: {publishResult.event_id}
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
