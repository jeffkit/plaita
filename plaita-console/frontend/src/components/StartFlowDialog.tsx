import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { X, Play, AlertCircle, Check, ChevronRight } from 'lucide-react'
import { api, FlowSummaryView } from '../services/api'
import { Button } from './ui'

interface StartFlowDialogProps {
  isOpen: boolean
  onClose: () => void
}

export default function StartFlowDialog({ isOpen, onClose }: StartFlowDialogProps) {
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [flowId, setFlowId] = useState('')
  const [flowSearch, setFlowSearch] = useState('')
  const [version, setVersion] = useState('')
  const [paramsJson, setParamsJson] = useState('{\n  \n}')
  const [jsonError, setJsonError] = useState<string | null>(null)
  const [startedFlowId, setStartedFlowId] = useState('')

  // 流程与版本不再靠手输记忆：打开时拉全量流程列表
  const flowsQuery = useQuery({
    queryKey: ['flows'],
    queryFn: api.getFlows,
    enabled: isOpen,
  })
  const flows = (flowsQuery.data?.flows || []) as FlowSummaryView[]
  const matchedFlows = flows.filter((f) =>
    !flowSearch.trim() ||
    f.flow_id.toLowerCase().includes(flowSearch.trim().toLowerCase()) ||
    (f.desc || '').toLowerCase().includes(flowSearch.trim().toLowerCase())
  )

  // 选中流程后拉版本列表；默认选最新已发布版本
  const flowDetailQuery = useQuery({
    queryKey: ['flow', flowId],
    queryFn: () => api.getFlow(flowId),
    enabled: isOpen && !!flowId,
  })
  const versions = (flowDetailQuery.data?.versions || []) as Array<{ version: string; status?: string }>
  const defaultVersion = versions.find((v) => v.status === 'published')?.version || versions[versions.length - 1]?.version || ''

  const startMutation = useMutation({
    mutationFn: api.startExecution,
    onSuccess: (_res, vars) => {
      queryClient.invalidateQueries({ queryKey: ['executions'] })
      // 发起即失联的修复：留在对话框内给出「查看执行」出口
      setStartedFlowId(vars.flow_id)
    },
  })

  const resetForm = () => {
    setFlowId('')
    setFlowSearch('')
    setVersion('')
    setParamsJson('{\n  \n}')
    setJsonError(null)
    setStartedFlowId('')
  }

  const handleClose = () => {
    onClose()
    // 成功后再关闭时才清表单；失败/取消保留输入
    if (startMutation.isSuccess || !flowId) resetForm()
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') handleClose()
  }

  const validateJson = (json: string): boolean => {
    try {
      JSON.parse(json)
      setJsonError(null)
      return true
    } catch (e) {
      setJsonError((e as Error).message)
      return false
    }
  }

  const handleParamsChange = (value: string) => {
    setParamsJson(value)
    if (value.trim()) {
      validateJson(value)
    } else {
      setJsonError(null)
    }
  }

  const formatJson = () => {
    try {
      const parsed = JSON.parse(paramsJson)
      setParamsJson(JSON.stringify(parsed, null, 2))
      setJsonError(null)
    } catch (e) {
      setJsonError((e as Error).message)
    }
  }

  const handleSubmit = () => {
    if (!flowId.trim()) return
    let params = {}
    if (paramsJson.trim()) {
      try {
        params = JSON.parse(paramsJson)
      } catch (e) {
        setJsonError((e as Error).message)
        return
      }
    }
    startMutation.mutate({
      flow_id: flowId,
      version: version || defaultVersion || undefined,
      params,
    })
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 animate-fade z-50 flex items-center justify-center" onKeyDown={handleKeyDown}>
      {/* 背景遮罩 */}
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={handleClose}
      />

      {/* 对话框：elevated 浮层 + line-strong（DESIGN.md §2.1/§5） */}
      <div
        role="dialog"
        aria-modal="true"
        aria-label="启动流程"
        className="relative bg-elevated border border-line-strong rounded-xl shadow-pop w-full max-w-2xl max-h-[90vh] overflow-hidden animate-pop"
      >
        {/* 标题栏 */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-line">
          <h2 className="text-section text-ink-primary flex items-center gap-2">
            <Play size={16} className="text-plaita-400" />
            启动流程
          </h2>
          <button
            onClick={handleClose}
            aria-label="关闭"
            className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-surface transition-colors"
          >
            <X size={16} />
          </button>
        </div>

        {startMutation.isSuccess ? (
          /* 成功态：给出下钻出口，不再「发起即失联」 */
          <div className="p-8 flex flex-col items-center text-center gap-3">
            <span className="w-10 h-10 rounded-full bg-plaita-500/10 flex items-center justify-center">
              <Check size={20} className="text-plaita-400" />
            </span>
            <p className="text-body text-ink-primary">流程已加入执行队列</p>
            <p className="text-caption text-ink-muted font-mono">
              {startedFlowId}
              {version || defaultVersion ? `@${version || defaultVersion}` : ''}
            </p>
            <p className="text-caption text-ink-faint">
              执行由 FlowWorker 异步消费，稍候片刻即可在列表中看到
            </p>
            <div className="flex gap-2 mt-2">
              <Button
                variant="primary"
                size="sm"
                onClick={() => {
                  const target = `/executions?flow_id=${encodeURIComponent(startedFlowId)}`
                  handleClose()
                  navigate(target)
                }}
              >
                查看执行
                <ChevronRight size={13} />
              </Button>
              <Button variant="secondary" size="sm" onClick={() => resetForm()}>
                再启动一个
              </Button>
            </div>
          </div>
        ) : (
          <>
            {/* 表单内容 */}
            <div className="p-6 space-y-5 overflow-y-auto max-h-[60vh]">
              {/* 流程选择：过滤 + 下拉，替代手输 flow_id */}
              <div>
                <label className="block text-caption text-ink-muted mb-1.5">
                  流程 <span className="text-status-error">*</span>
                </label>
                <input
                  type="text"
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
                    startMutation.reset()
                  }}
                  className="input w-full font-mono"
                  size={Math.min(6, Math.max(3, matchedFlows.length))}
                >
                  {flowsQuery.isLoading && <option value="">加载流程列表…</option>}
                  {!flowsQuery.isLoading && flows.length === 0 && (
                    <option value="">（还没有流程——先到「流程编排」新建并发布）</option>
                  )}
                  {!flowsQuery.isLoading && flows.length > 0 && matchedFlows.length === 0 && (
                    <option value="">无匹配流程</option>
                  )}
                  {matchedFlows.map((f) => (
                    <option key={f.flow_id} value={f.flow_id}>
                      {f.flow_id}
                      {f.desc ? ` — ${f.desc}` : ''}
                    </option>
                  ))}
                </select>
                {flowsQuery.isError && (
                  <p className="text-caption text-status-error mt-1.5 flex items-center gap-1">
                    <AlertCircle size={12} /> 流程列表加载失败：{(flowsQuery.error as Error).message}
                  </p>
                )}
              </div>

              {/* 版本：从流程详情拉取，默认最新已发布 */}
              <div>
                <label className="block text-caption text-ink-muted mb-1.5">版本</label>
                <select
                  value={version || defaultVersion}
                  onChange={(e) => setVersion(e.target.value)}
                  className="input w-full font-mono"
                  disabled={!flowId}
                >
                  {!flowId && <option value="">先选择流程</option>}
                  {flowId && versions.length === 0 && <option value="">（无版本，运行时取最新）</option>}
                  {versions.map((v) => (
                    <option key={v.version} value={v.version}>
                      {v.version}（{v.status === 'published' ? '已发布' : v.status === 'draft' ? '草稿' : v.status}）
                    </option>
                  ))}
                </select>
              </div>

              {/* 输入参数 */}
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <label className="text-caption text-ink-muted">输入参数 (JSON)</label>
                  <button
                    onClick={formatJson}
                    className="text-caption text-plaita-400 hover:text-plaita-300 transition-colors"
                  >
                    格式化
                  </button>
                </div>
                <div className="relative">
                  <textarea
                    value={paramsJson}
                    onChange={(e) => handleParamsChange(e.target.value)}
                    rows={8}
                    spellCheck={false}
                    className={`input w-full font-mono text-data-sm resize-none ${
                      jsonError ? '!border-status-error/50' : ''
                    }`}
                  />
                  {/* JSON 状态指示器 */}
                  <div className="absolute top-3 right-3">
                    {paramsJson.trim() && !jsonError && (
                      <Check size={16} className="text-plaita-400" />
                    )}
                    {jsonError && (
                      <AlertCircle size={16} className="text-status-error" />
                    )}
                  </div>
                </div>
                {jsonError && (
                  <p className="text-status-error text-caption mt-2 flex items-center gap-2">
                    <AlertCircle size={14} />
                    JSON 格式错误: {jsonError}
                  </p>
                )}
              </div>
            </div>

            {/* 底部按钮 */}
            <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-line bg-surface">
              <Button variant="secondary" onClick={handleClose}>
                取消
              </Button>
              <Button
                variant="primary"
                onClick={handleSubmit}
                disabled={!flowId || !!jsonError || startMutation.isPending}
              >
                {startMutation.isPending ? (
                  <>
                    <span className="w-3.5 h-3.5 border-2 border-on-accent/30 border-t-on-accent rounded-full animate-spin" />
                    启动中…
                  </>
                ) : (
                  <>
                    <Play size={14} />
                    启动
                  </>
                )}
              </Button>
            </div>
          </>
        )}

        {/* 错误提示 */}
        {startMutation.isError && (
          <div className="px-6 py-3 bg-status-error-dim border-t border-status-error/30 text-status-error text-caption">
            启动失败: {(startMutation.error as Error).message}
          </div>
        )}
      </div>
    </div>
  )
}
