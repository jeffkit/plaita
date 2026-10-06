import { useQuery } from '@tanstack/react-query'
import { Activity, Cpu, Inbox, Server, Zap } from 'lucide-react'
import { api } from '../services/api'
import {
  Page,
  PageHeader,
  Card,
  StatCard,
  StatusBadge,
  EmptyState,
} from '../components/ui'

/**
 * Worker 效能看板：两台 worker（本机 + 远端）的忙闲、任务在跑数、心跳新鲜度
 * 与队列积压。数据源复用既有 API（/services + /queues），无独立后端接口。
 *
 * 「忙闲」= active_tasks：worker 是单条串行消费（flow_worker 主循环一次一条），
 * 故 >0 即占用满档；多任务改造（并发提案 S2）后可 >1，此页据此显示多档占用。
 */

const FLOW_QUEUE = 'plaita:flow:queue'
const STREAM_QUEUE = 'plaita:flow:queue:v2'

// 心跳新鲜度：<60s 新鲜 / <180s 滞后 / 其余失联（registry-ttl=30s，两跳即异常）
function hbState(lastHeartbeat?: string) {
  if (!lastHeartbeat) return { text: '无心跳', tone: 'error' as const, age: null }
  const age = (Date.now() - new Date(lastHeartbeat).getTime()) / 1000
  if (age < 60) return { text: `${Math.round(age)}s 前`, tone: 'success' as const, age }
  if (age < 180) return { text: `${Math.round(age)}s 前`, tone: 'warning' as const, age }
  return { text: `${Math.round(age / 60)}m 前`, tone: 'error' as const, age }
}

const TONE_TEXT: Record<string, string> = {
  success: 'text-status-success',
  warning: 'text-status-warning',
  error: 'text-status-error',
}

export default function Workers() {
  const { data: svcData, isLoading: svcLoading } = useQuery({
    queryKey: ['services', 'flow_worker'],
    queryFn: () => api.getServices('flow_worker'),
    refetchInterval: 5000,
  })

  const { data: queueData } = useQuery({
    queryKey: ['queues'],
    queryFn: api.getQueues,
    refetchInterval: 5000,
  })

  const workers = (svcData?.services || []).filter(
    (s) => s.service_type === 'flow_worker',
  )
  // 队列积压以 stream（v2，worker 实消费）为准
  const queues = queueData?.queues || []
  const streamQueue = queues.find((q) => q.name === STREAM_QUEUE)
  const pendingTasks = streamQueue?.length ?? 0

  const online = workers.filter((w) => w.status === 'running')
  const activeTasks = workers.reduce((sum, w) => sum + (w.active_tasks || 0), 0)
  // 忙闲占用率：以「在线实例数」为分母（每实例当前串行=1 档），多任务后可超 100%
  const utilization = online.length ? Math.round((activeTasks / online.length) * 100) : 0

  return (
    <Page>
      <PageHeader
        title="Worker 效能"
        subtitle="流程执行器（flow_worker）忙闲与队列积压 · 每 5 秒刷新"
      />

      {/* 概要卡 */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard
          icon={<Server size={15} className="text-ink-muted" />}
          title="在线 Worker"
          value={online.length}
          total={workers.length}
        />
        <StatCard
          icon={<Cpu size={15} className="text-ink-muted" />}
          title="在跑任务"
          value={activeTasks}
        />
        <StatCard
          icon={<Activity size={15} className="text-ink-muted" />}
          title="队列积压"
          value={pendingTasks}
        />
        <StatCard
          icon={<Zap size={15} className="text-ink-muted" />}
          title="占用率"
          value={`${utilization}%`}
        />
      </div>

      {/* Worker 列表 */}
      {svcLoading ? (
        <EmptyState message="加载中…" />
      ) : workers.length === 0 ? (
        <EmptyState
          icon={<Server size={20} />}
          message="暂无 Worker 实例"
          hint="worker 需注册到服务注册表（plaita:registry）才会出现在此处；经隧道连远端 console 时可看到两台（VM + MacBookPro）"
        />
      ) : (
        <div className="space-y-3">
          {workers.map((w) => {
            const hb = hbState(w.last_heartbeat)
            const busy = (w.active_tasks || 0) > 0
            const qname = String(
              (w.metadata as Record<string, unknown>)?.queue_name ?? '—',
            )
            return (
              <Card key={w.instance_id} className="p-4">
                <div className="flex items-start justify-between gap-4 flex-wrap">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <h3 className="font-mono text-data font-medium text-ink-primary">
                        {w.instance_id}
                      </h3>
                      <StatusBadge status={w.status} />
                      {/* 忙闲徽章：>0 忙、=0 闲 */}
                      <span
                        className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md border border-line text-caption ${
                          busy
                            ? 'text-status-running bg-status-running-dim'
                            : 'text-status-success bg-status-success-dim'
                        }`}
                      >
                        <span
                          className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                            busy ? 'bg-status-running animate-breathe' : 'bg-status-success'
                          }`}
                        />
                        {busy ? `忙 · ${w.active_tasks}` : '闲'}
                      </span>
                    </div>
                    <p className="mt-1.5 text-caption text-ink-muted break-all">
                      主机 <span className="font-mono">{w.host}</span> · 队列{' '}
                      <span className="font-mono">{qname}</span>
                    </p>
                    {w.start_time && (
                      <p className="mt-0.5 text-caption text-ink-faint">
                        启动于 {new Date(w.start_time).toLocaleString()}
                      </p>
                    )}
                  </div>

                  <div className="text-right shrink-0">
                    <p className="text-micro uppercase text-ink-muted">心跳</p>
                    <p className={`mt-1 font-mono text-data tabular-nums ${TONE_TEXT[hb.tone]}`}>
                      {hb.text}
                    </p>
                  </div>
                </div>
              </Card>
            )
          })}
        </div>
      )}

      {/* 队列积压明细 */}
      <Card className="p-4">
        <div className="flex items-center gap-2 mb-3">
          <Inbox size={15} className="text-ink-muted" />
          <h3 className="text-section text-ink-primary">队列积压</h3>
        </div>
        <div className="space-y-2">
          {queues.length === 0 ? (
            <p className="text-caption text-ink-muted">暂无队列</p>
          ) : (
            queues.map((q) => (
              <div
                key={q.name}
                className="flex items-center justify-between bg-inset border border-line rounded-lg px-3 py-2"
              >
                <span className="font-mono text-data-sm text-ink-secondary break-all">
                  {q.name}
                </span>
                <span
                  className={`font-mono text-data tabular-nums ${
                    q.length === 0
                      ? 'text-status-success'
                      : q.length < 10
                        ? 'text-status-warning'
                        : 'text-status-error'
                  }`}
                >
                  {q.length}
                </span>
              </div>
            ))
          )}
        </div>
        <p className="mt-3 text-caption text-ink-faint">
          {STREAM_QUEUE} 为 worker 实消费流（XLEN）；{FLOW_QUEUE} 为兼容视图。
          积压持续增长 = 工人产能不足，需要放大并发（见并发提案 S1-S5）。
        </p>
      </Card>
    </Page>
  )
}
