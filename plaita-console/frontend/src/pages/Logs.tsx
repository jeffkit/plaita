import { useState, useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Play, Pause, RefreshCw, Search, Radio } from 'lucide-react'
import { api, LogEntry } from '../services/api'

export default function Logs() {
  const [isStreaming, setIsStreaming] = useState(false)
  const [useSSE, setUseSSE] = useState(false)
  const [levelFilter, setLevelFilter] = useState<string>('')
  const [searchText, setSearchText] = useState('')
  const [autoScroll, setAutoScroll] = useState(true)
  const [sseLogs, setSseLogs] = useState<LogEntry[]>([])
  const logsContainerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!isStreaming || !useSSE) return

    const params = new URLSearchParams()
    if (levelFilter) params.set('level', levelFilter)
    const evtSource = new EventSource(`/api/logs/stream?${params}`)

    evtSource.addEventListener('log', (e) => {
      try {
        const logData = JSON.parse(e.data) as LogEntry
        setSseLogs((prev) => [...prev.slice(-499), logData])
      } catch { /* ignore */ }
    })

    evtSource.onerror = () => {
      evtSource.close()
      setUseSSE(false)
    }

    return () => evtSource.close()
  }, [isStreaming, useSSE, levelFilter])

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['logs', levelFilter],
    queryFn: () => api.getLogs({ level: levelFilter || undefined, limit: 200 }),
    refetchInterval: isStreaming && !useSSE ? 2000 : false,
  })

  const baseLogs = useSSE && isStreaming ? sseLogs : (data?.logs || [])

  const filteredLogs = baseLogs.filter((log) => {
    if (searchText && !log.message.toLowerCase().includes(searchText.toLowerCase())) {
      return false
    }
    return true
  })

  useEffect(() => {
    if (autoScroll && logsContainerRef.current) {
      logsContainerRef.current.scrollTop = logsContainerRef.current.scrollHeight
    }
  }, [filteredLogs, autoScroll])

  return (
    <div className="p-8 h-full flex flex-col">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-3xl font-bold">日志查看</h1>
        <div className="flex items-center gap-4">
          {/* 搜索 */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-400" size={16} />
            <input
              type="text"
              placeholder="搜索日志..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              className="bg-dark-700 border border-dark-600 rounded-lg pl-10 pr-4 py-2 text-sm w-64 focus:outline-none focus:ring-2 focus:ring-plaita-500"
            />
          </div>

          {/* 级别筛选 */}
          <select
            value={levelFilter}
            onChange={(e) => setLevelFilter(e.target.value)}
            className="bg-dark-700 border border-dark-600 rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500"
          >
            <option value="">全部级别</option>
            <option value="DEBUG">DEBUG</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
          </select>

          {/* SSE/轮询切换 */}
          <button
            onClick={() => { setUseSSE(!useSSE); if (!isStreaming) setIsStreaming(true) }}
            className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors ${
              useSSE
                ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30'
                : 'bg-dark-700 hover:bg-dark-600 text-dark-300'
            }`}
            title={useSSE ? 'SSE 实时推送' : '轮询模式'}
          >
            <Radio size={14} />
            {useSSE ? 'SSE' : '轮询'}
          </button>

          {/* 实时开关 */}
          <button
            onClick={() => setIsStreaming(!isStreaming)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
              isStreaming
                ? 'bg-plaita-500/20 text-plaita-400 border border-plaita-500/30'
                : 'bg-dark-700 hover:bg-dark-600'
            }`}
          >
            {isStreaming ? <Pause size={16} /> : <Play size={16} />}
            {isStreaming ? '暂停' : '实时'}
          </button>

          {/* 刷新 */}
          <button
            onClick={() => refetch()}
            className="flex items-center gap-2 bg-dark-700 hover:bg-dark-600 px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw size={16} />
          </button>
        </div>
      </div>

      {/* 日志列表 */}
      <div
        ref={logsContainerRef}
        className="flex-1 bg-dark-900 rounded-xl border border-dark-700 overflow-auto font-mono text-sm"
      >
        {isLoading ? (
          <div className="flex items-center justify-center h-full text-dark-400">
            加载中...
          </div>
        ) : filteredLogs.length === 0 ? (
          <div className="flex items-center justify-center h-full text-dark-400">
            暂无日志
          </div>
        ) : (
          <div className="p-4 space-y-1">
            {filteredLogs.map((log, index) => (
              <LogLine key={index} log={log} />
            ))}
          </div>
        )}
      </div>

      {/* 底部状态栏 */}
      <div className="flex items-center justify-between mt-4 text-sm text-dark-400">
        <span>共 {filteredLogs.length} 条日志</span>
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
            className="rounded bg-dark-700 border-dark-600"
          />
          自动滚动
        </label>
      </div>
    </div>
  )
}

// 日志行组件
function LogLine({ log }: { log: { timestamp: string; level: string; message: string; service_type?: string } }) {
  const levelColors: Record<string, string> = {
    DEBUG: 'text-dark-400',
    INFO: 'text-blue-400',
    WARNING: 'text-yellow-400',
    ERROR: 'text-red-400',
  }

  return (
    <div className="flex gap-4 hover:bg-dark-800/50 px-2 py-1 rounded">
      <span className="text-dark-500 whitespace-nowrap">
        {new Date(log.timestamp).toLocaleTimeString()}
      </span>
      <span className={`w-16 ${levelColors[log.level] || 'text-dark-400'}`}>
        [{log.level}]
      </span>
      {log.service_type && (
        <span className="text-dark-500 w-24 truncate">
          {log.service_type}
        </span>
      )}
      <span className="flex-1 text-dark-200">{log.message}</span>
    </div>
  )
}

