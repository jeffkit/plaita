import { useQuery, useInfiniteQuery } from '@tanstack/react-query'
import { Inbox, ChevronDown, ChevronUp } from 'lucide-react'
import { useState } from 'react'
import { api } from '../services/api'
import { Page, PageHeader, Card, EmptyState, ErrorState } from '../components/ui'

export default function Queues() {
  const [expandedQueue, setExpandedQueue] = useState<string | null>(null)

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['queues'],
    queryFn: api.getQueues,
    refetchInterval: 5000,
  })

  const queues = data?.queues || []

  return (
    <Page>
      <PageHeader title="任务队列" subtitle="各队列积压与任务明细" />

      {isLoading ? (
        <EmptyState message="加载中…" />
      ) : isError ? (
        <ErrorState error={error} title="队列列表加载失败" onRetry={() => refetch()} />
      ) : queues.length === 0 ? (
        <EmptyState icon={<Inbox size={20} />} message="暂无队列" />
      ) : (
        <div className="space-y-3">
          {queues.map((queue) => (
            <QueueCard
              key={queue.name}
              queue={queue}
              isExpanded={expandedQueue === queue.name}
              onToggle={() => setExpandedQueue(
                expandedQueue === queue.name ? null : queue.name
              )}
            />
          ))}
        </div>
      )}
    </Page>
  )
}

// 队列卡片
function QueueCard({
  queue,
  isExpanded,
  onToggle,
}: {
  queue: { name: string; length: number }
  isExpanded: boolean
  onToggle: () => void
}) {
  // A2R55-01：truncated/next_start_id 必须可消费——深队列（含 DLQ 死信）
  // 不能静默截在前 20 条。Stream 队列用排他游标 start_id 续读，list 队列
  // 按索引切片（start=已取条数）。
  const {
    data: detailData,
    isLoading,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ['queue', queue.name],
    queryFn: ({ pageParam }) => api.getQueueDetail(queue.name, pageParam || undefined),
    initialPageParam: undefined as { start?: number; startId?: string } | undefined,
    getNextPageParam: (last, allPages) => {
      if (!last.truncated) return undefined
      if (last.next_start_id) return { startId: last.next_start_id }
      return { start: allPages.reduce((n, p) => n + p.tasks.length, 0) }
    },
    enabled: isExpanded,
  })

  const detailPages = detailData?.pages ?? []
  // stream 游标续读的 task.index 每页都从 0 计，拼上页号做稳定 key
  const detailTasks = detailPages.flatMap((page, pi) =>
    page.tasks.map((task) => ({ task, rowKey: `${pi}-${task.index}` }))
  )
  const totalInQueue = detailPages[0]?.length ?? 0

  // 积压语义：0 空 / <10 正常 / ≥10 需要关注
  const getLengthColor = (length: number) => {
    if (length === 0) return 'text-status-success'
    if (length < 10) return 'text-status-warning'
    return 'text-status-error'
  }

  return (
    <Card className="overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-4 hover:bg-elevated/40 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-inset border border-line">
            <Inbox className="text-ink-muted" size={18} />
          </div>
          <div className="text-left">
            <h3 className="font-mono text-data font-medium text-ink-primary">{queue.name}</h3>
            <p className="text-caption text-ink-muted">
              {queue.length === 0 ? '队列为空' : `${queue.length} 个任务等待处理`}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className={`font-mono text-2xl font-semibold tabular-nums ${getLengthColor(queue.length)}`}>
            {queue.length}
          </span>
          <span className="text-ink-faint">
            {isExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </span>
        </div>
      </button>

      {isExpanded && (
        <div className="border-t border-line p-4">
          {isLoading ? (
            <div className="text-center text-ink-muted py-4 text-caption">加载中…</div>
          ) : detailTasks.length === 0 ? (
            <div className="text-center text-ink-muted py-4 text-caption">队列为空</div>
          ) : (
            <div className="space-y-2">
              <p className="text-caption text-ink-muted mb-3">
                已显示 <span className="font-mono tabular-nums">{detailTasks.length}</span> /{' '}
                <span className="font-mono tabular-nums">{totalInQueue}</span> 个任务
                {hasNextPage && <span className="text-status-warning">（仅前几页，可加载更多）</span>}
              </p>
              {detailTasks.map(({ task, rowKey }) => (
                <div key={rowKey} className="bg-inset border border-line rounded-lg p-3 font-mono text-data-sm">
                  <div className="flex items-center justify-between mb-1.5">
                    <span className="text-ink-faint tabular-nums">#{task.index}</span>
                    {!!task.data.type && (
                      <span className="px-1.5 py-0.5 bg-elevated border border-line rounded text-caption text-ink-secondary">
                        {String(task.data.type)}
                      </span>
                    )}
                  </div>
                  <pre className="text-caption text-ink-secondary overflow-x-auto">
                    {JSON.stringify(task.data, null, 2)}
                  </pre>
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
                    : `加载更多（已显示 ${detailTasks.length} / 共 ${totalInQueue} 个）`}
                </button>
              )}
            </div>
          )}
        </div>
      )}
    </Card>
  )
}
