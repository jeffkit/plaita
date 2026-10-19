import { useState, useEffect, useRef } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Play, Pause, RefreshCw, Search, Radio, ScrollText, X, AlertTriangle } from 'lucide-react'
import { api, API_BASE, apiFetch, errorUserMessage, LogEntry } from '../services/api'
import { PageHeader, Button, EmptyState, ErrorState } from '../components/ui'

export default function Logs() {
  // 筛选进 URL：Cluster 实例页的「查看更多」深链可以直达了
  const [searchParams, setSearchParams] = useSearchParams()
  const levelFilter = searchParams.get('level') || ''
  const instanceFilter = searchParams.get('instance_id') || ''
  // #102：Cluster 实例卡深链带 service_type+instance_id；后端对只有
  // instance_id 的历史链接同样按条目收窄，这里两者都透传
  const serviceTypeFilter = searchParams.get('service_type') || ''

  const setParam = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams)
    if (value) next.set(key, value)
    else next.delete(key)
    setSearchParams(next)
  }

  const [isStreaming, setIsStreaming] = useState(false)
  const [useSSE, setUseSSE] = useState(false)
  const [sseLost, setSseLost] = useState(false)
  const [searchText, setSearchText] = useState('')
  const [autoScroll, setAutoScroll] = useState(true)
  const [sseLogs, setSseLogs] = useState<LogEntry[]>([])
  const logsContainerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!isStreaming || !useSSE) return

    const params = new URLSearchParams()
    if (levelFilter) params.set('level', levelFilter)
    if (instanceFilter) params.set('instance_id', instanceFilter)
    if (serviceTypeFilter) params.set('service_type', serviceTypeFilter)

    // C4-4：EventSource 无法携带 Authorization 头，鉴权部署下 SSE 连接
    // 必然 401。/logs/stream 无一次性票据端点，故用 fetch 流式读替代：
    // apiFetch 在每次建流时注入当前凭证；断流回落轮询。
    const controller = new AbortController()
    let cancelled = false

    const handleLogEvent = (rawData: string) => {
      try {
        const logData = JSON.parse(rawData) as LogEntry
        setSseLogs((prev) => [...prev.slice(-499), logData])
      } catch { /* ignore */ }
    }

    void (async () => {
      try {
        const resp = await apiFetch(`${API_BASE}/logs/stream?${params}`, {
          signal: controller.signal,
        }, { stream: true })
        if (!resp.ok || !resp.body) throw new Error(`logs/stream ${resp.status}`)
        const reader = resp.body.getReader()
        const decoder = new TextDecoder()
        let buf = ''
        for (;;) {
          const { done, value } = await reader.read()
          if (done) break
          buf += decoder.decode(value, { stream: true })
          // 尾部悬空的 \r 可能与下一块的 \n 组成 \r\n，先扣下不归一化
          let normalizedLen = buf.length
          if (buf.endsWith('\r')) normalizedLen -= 1
          buf = buf.slice(0, normalizedLen).replace(/\r\n?/g, '\n') + buf.slice(normalizedLen)
          // SSE 事件以空行分隔
          let sep: number
          while ((sep = buf.indexOf('\n\n')) >= 0) {
            const block = buf.slice(0, sep)
            buf = buf.slice(sep + 2)
            let eventName = 'message'
            const dataLines: string[] = []
            for (const line of block.split('\n')) {
              if (line.startsWith('event:')) eventName = line.slice(6).trim()
              else if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart())
              // ": ping" 注释行与 retry: 行忽略
            }
            if (eventName === 'log' && dataLines.length > 0) {
              handleLogEvent(dataLines.join('\n'))
            }
          }
        }
        // 服务端正常收尾也视为断流：回落轮询，不允许静默停更
        throw new Error('logs/stream ended')
      } catch (e) {
        if (!cancelled && !controller.signal.aborted) {
          console.warn('[logs-stream] 流式读取结束/失败，回落轮询:', errorUserMessage(e), e)
          setUseSSE(false)
          setSseLost(true)
        }
      }
    })()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [isStreaming, useSSE, levelFilter, instanceFilter, serviceTypeFilter])

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['logs', levelFilter, instanceFilter, serviceTypeFilter],
    queryFn: () =>
      api.getLogs({
        level: levelFilter || undefined,
        instance_id: instanceFilter || undefined,
        service_type: serviceTypeFilter || undefined,
        limit: 200,
      }),
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
    <div className="p-6 h-full flex flex-col gap-4">
      <PageHeader
        title="日志查看"
        subtitle="集群运行日志检索与实时跟踪"
        actions={
          <>
            {/* 搜索 */}
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-faint" size={14} />
              <input
                type="text"
                placeholder="搜索日志…"
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                className="input w-56 !pl-8"
              />
            </div>

            {/* 级别筛选 */}
            <select
              value={levelFilter}
              onChange={(e) => setParam('level', e.target.value)}
              className="input w-32"
            >
              <option value="">全部级别</option>
              <option value="DEBUG">DEBUG</option>
              <option value="INFO">INFO</option>
              <option value="WARNING">WARNING</option>
              <option value="ERROR">ERROR</option>
            </select>

            {/* SSE/轮询切换 */}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                setSseLost(false)
                setUseSSE((v) => !v)
                if (!isStreaming) setIsStreaming(true)
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
                  ? 'SSE 实时推送'
                  : sseLost
                    ? '实时连接已断开，自动回落轮询（点击重试 SSE）'
                    : '轮询模式'
              }
            >
              <Radio size={13} />
              {useSSE ? 'SSE' : sseLost ? '轮询（已断开）' : '轮询'}
            </Button>

            {/* 实时开关 */}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsStreaming(!isStreaming)}
              className={isStreaming ? 'bg-plaita-500/10 text-plaita-400 hover:text-plaita-400' : undefined}
            >
              {isStreaming ? <Pause size={13} /> : <Play size={13} />}
              {isStreaming ? '暂停' : '实时'}
            </Button>

            {/* 刷新 */}
            <Button variant="ghost" size="sm" onClick={() => refetch()} aria-label="刷新" title="刷新">
              <RefreshCw size={13} />
            </Button>
          </>
        }
      />

      {/* 活跃筛选 chips：来源可见、可移除 */}
      {(instanceFilter || levelFilter || serviceTypeFilter) && (
        <div className="flex items-center gap-2">
          {serviceTypeFilter && (
            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md border border-line bg-plaita-500/10 text-caption text-plaita-400 font-mono">
              {serviceTypeFilter}
              <button onClick={() => setParam('service_type', '')} aria-label="移除服务类型筛选" title="移除服务类型筛选">
                <X size={12} />
              </button>
            </span>
          )}
          {instanceFilter && (
            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md border border-line bg-plaita-500/10 text-caption text-plaita-400 font-mono">
              {instanceFilter}
              <button onClick={() => setParam('instance_id', '')} aria-label="移除实例筛选" title="移除实例筛选">
                <X size={12} />
              </button>
            </span>
          )}
          {levelFilter && (
            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md border border-line bg-plaita-500/10 text-caption text-plaita-400">
              {levelFilter}
              <button onClick={() => setParam('level', '')} aria-label="移除级别筛选" title="移除级别筛选">
                <X size={12} />
              </button>
            </span>
          )}
        </div>
      )}

      {/* 日志列表：下沉区（inset）+ 数据声道 */}
      <div
        ref={logsContainerRef}
        className="flex-1 min-h-0 bg-inset border border-line rounded-xl overflow-auto font-mono text-data-sm"
      >
        {isLoading ? (
          <EmptyState message="加载中…" />
        ) : isError && !(useSSE && isStreaming) ? (
          <ErrorState error={error} title="日志加载失败" onRetry={() => refetch()} />
        ) : filteredLogs.length === 0 ? (
          <EmptyState
            icon={<ScrollText size={20} />}
            message={instanceFilter || levelFilter || serviceTypeFilter || searchText ? '没有匹配的日志' : '暂无日志'}
            hint={instanceFilter ? '该实例可能尚未产生日志；可移除筛选查看全部' : undefined}
          />
        ) : (
          <div className="p-3 space-y-0.5">
            {filteredLogs.map((log, index) => (
              <LogLine key={index} log={log} />
            ))}
          </div>
        )}
      </div>

      {/* 底部状态栏 */}
      <div className="flex items-center justify-between text-caption text-ink-muted">
        <span className="flex items-center gap-3">
          <span>共 <span className="font-mono tabular-nums">{filteredLogs.length}</span> 条日志</span>
          {sseLost && (
            <span className="flex items-center gap-1 text-status-warning">
              <AlertTriangle size={12} />
              实时连接已断开，已回落轮询
            </span>
          )}
        </span>
        <label className="flex items-center gap-2 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
            className="rounded bg-inset border-line accent-plaita-500"
          />
          自动滚动
        </label>
      </div>
    </div>
  )
}

// 日志行组件：级别色只做语义点缀，正文保持中性（DESIGN.md §6-5）。
// #102：行内渲染 instance_id——实例筛选下混入行（同 service_type 其他实例）
// 必须可与目标实例的行区分。
function LogLine({ log }: { log: { timestamp: string; level: string; message: string; service_type?: string; instance_id?: string } }) {
  const levelColors: Record<string, string> = {
    DEBUG: 'text-ink-faint',
    INFO: 'text-ink-secondary',
    WARNING: 'text-status-warning',
    ERROR: 'text-status-error',
  }

  return (
    <div className="flex gap-4 hover:bg-elevated/50 px-2 py-1 rounded">
      <span className="text-ink-faint whitespace-nowrap tabular-nums">
        {new Date(log.timestamp).toLocaleTimeString()}
      </span>
      <span className={`w-16 shrink-0 ${levelColors[log.level] || 'text-ink-muted'}`}>
        [{log.level}]
      </span>
      {log.service_type && (
        <span className="text-ink-faint w-24 truncate shrink-0">
          {log.service_type}
        </span>
      )}
      {log.instance_id && (
        <span className="text-ink-faint w-40 truncate shrink-0">
          {log.instance_id}
        </span>
      )}
      <span className="flex-1 text-ink-secondary">{log.message}</span>
    </div>
  )
}
