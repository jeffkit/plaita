import { useQuery } from '@tanstack/react-query'
import { Inbox, ChevronDown, ChevronUp } from 'lucide-react'
import { useState } from 'react'
import { api } from '../services/api'

export default function Queues() {
  const [expandedQueue, setExpandedQueue] = useState<string | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['queues'],
    queryFn: api.getQueues,
    refetchInterval: 5000,
  })

  const queues = data?.queues || []

  return (
    <div className="p-8">
      <h1 className="text-3xl font-bold mb-6">任务队列</h1>

      {isLoading ? (
        <div className="flex items-center justify-center py-12 text-dark-400">
          加载中...
        </div>
      ) : queues.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-12 text-dark-400">
          <Inbox size={48} className="mb-4" />
          <p>暂无队列</p>
        </div>
      ) : (
        <div className="space-y-4">
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
    </div>
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
  const { data: detailData, isLoading } = useQuery({
    queryKey: ['queue', queue.name],
    queryFn: () => api.getQueueDetail(queue.name),
    enabled: isExpanded,
  })

  const getLengthColor = (length: number) => {
    if (length === 0) return 'text-plaita-400'
    if (length < 10) return 'text-yellow-400'
    return 'text-red-400'
  }

  return (
    <div className="bg-dark-800/50 rounded-xl border border-dark-700 overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-6 hover:bg-dark-700/30 transition-colors"
      >
        <div className="flex items-center gap-4">
          <div className="p-3 rounded-lg bg-dark-700">
            <Inbox className="text-dark-400" size={24} />
          </div>
          <div className="text-left">
            <h3 className="font-medium">{queue.name}</h3>
            <p className="text-dark-400 text-sm">
              {queue.length === 0 ? '队列为空' : `${queue.length} 个任务等待处理`}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <span className={`text-3xl font-bold ${getLengthColor(queue.length)}`}>
            {queue.length}
          </span>
          {isExpanded ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
        </div>
      </button>

      {isExpanded && (
        <div className="border-t border-dark-700 p-6">
          {isLoading ? (
            <div className="text-center text-dark-400 py-4">加载中...</div>
          ) : detailData?.tasks.length === 0 ? (
            <div className="text-center text-dark-400 py-4">队列为空</div>
          ) : (
            <div className="space-y-2">
              <p className="text-sm text-dark-400 mb-4">
                显示前 {detailData?.tasks.length} 个任务
              </p>
              {detailData?.tasks.map((task) => (
                <div
                  key={task.index}
                  className="bg-dark-700/50 rounded-lg p-4 font-mono text-sm"
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-dark-400">#{task.index}</span>
                    {!!task.data.type && (
                      <span className="px-2 py-1 bg-dark-600 rounded text-xs">
                        {String(task.data.type)}
                      </span>
                    )}
                  </div>
                  <pre className="text-xs text-dark-300 overflow-x-auto">
                    {JSON.stringify(task.data, null, 2)}
                  </pre>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

