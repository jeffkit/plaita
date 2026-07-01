import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { X, Play, AlertCircle, Check } from 'lucide-react'
import { api } from '../services/api'

interface StartFlowDialogProps {
  isOpen: boolean
  onClose: () => void
}

export default function StartFlowDialog({ isOpen, onClose }: StartFlowDialogProps) {
  const queryClient = useQueryClient()
  const [flowId, setFlowId] = useState('')
  const [version, setVersion] = useState('')
  const [paramsJson, setParamsJson] = useState('{\n  \n}')
  const [jsonError, setJsonError] = useState<string | null>(null)

  const startMutation = useMutation({
    mutationFn: api.startExecution,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['executions'] })
      onClose()
      resetForm()
    },
  })

  const resetForm = () => {
    setFlowId('')
    setVersion('')
    setParamsJson('{\n  \n}')
    setJsonError(null)
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
    if (!flowId.trim()) {
      return
    }

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
      version: version || undefined,
      params,
    })
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* 背景遮罩 */}
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* 对话框 */}
      <div className="relative bg-dark-800 rounded-xl border border-dark-600 w-full max-w-2xl max-h-[90vh] overflow-hidden shadow-2xl">
        {/* 标题栏 */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-dark-700">
          <h2 className="text-xl font-semibold flex items-center gap-2">
            <Play size={20} className="text-plaita-400" />
            启动流程
          </h2>
          <button
            onClick={onClose}
            className="p-2 hover:bg-dark-700 rounded-lg transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        {/* 表单内容 */}
        <div className="p-6 space-y-6 overflow-y-auto max-h-[60vh]">
          {/* 流程 ID */}
          <div>
            <label className="block text-sm font-medium mb-2">
              流程 ID <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              value={flowId}
              onChange={(e) => setFlowId(e.target.value)}
              placeholder="例如: my-workflow"
              className="w-full bg-dark-700 border border-dark-600 rounded-lg px-4 py-3 focus:outline-none focus:ring-2 focus:ring-plaita-500 focus:border-transparent"
            />
          </div>

          {/* 版本 */}
          <div>
            <label className="block text-sm font-medium mb-2">
              版本 <span className="text-dark-400">(可选，留空使用最新版本)</span>
            </label>
            <input
              type="text"
              value={version}
              onChange={(e) => setVersion(e.target.value)}
              placeholder="例如: 1.0.0"
              className="w-full bg-dark-700 border border-dark-600 rounded-lg px-4 py-3 focus:outline-none focus:ring-2 focus:ring-plaita-500 focus:border-transparent"
            />
          </div>

          {/* 输入参数 */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-sm font-medium">输入参数 (JSON)</label>
              <button
                onClick={formatJson}
                className="text-xs px-2 py-1 bg-dark-600 hover:bg-dark-500 rounded transition-colors"
              >
                格式化
              </button>
            </div>
            <div className="relative">
              <textarea
                value={paramsJson}
                onChange={(e) => handleParamsChange(e.target.value)}
                rows={10}
                spellCheck={false}
                className={`w-full bg-dark-900 border rounded-lg px-4 py-3 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-plaita-500 ${
                  jsonError
                    ? 'border-red-500/50 focus:ring-red-500'
                    : 'border-dark-600'
                }`}
              />
              {/* JSON 状态指示器 */}
              <div className="absolute top-3 right-3">
                {paramsJson.trim() && !jsonError && (
                  <Check size={16} className="text-plaita-400" />
                )}
                {jsonError && (
                  <AlertCircle size={16} className="text-red-400" />
                )}
              </div>
            </div>
            {jsonError && (
              <p className="text-red-400 text-sm mt-2 flex items-center gap-2">
                <AlertCircle size={14} />
                JSON 格式错误: {jsonError}
              </p>
            )}
          </div>
        </div>

        {/* 底部按钮 */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-dark-700 bg-dark-800/50">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-dark-600 hover:bg-dark-500 rounded-lg transition-colors"
          >
            取消
          </button>
          <button
            onClick={handleSubmit}
            disabled={!flowId.trim() || !!jsonError || startMutation.isPending}
            className="flex items-center gap-2 px-6 py-2 bg-plaita-500 hover:bg-plaita-600 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg transition-colors"
          >
            {startMutation.isPending ? (
              <>
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                启动中...
              </>
            ) : (
              <>
                <Play size={16} />
                启动
              </>
            )}
          </button>
        </div>

        {/* 错误提示 */}
        {startMutation.isError && (
          <div className="px-6 py-3 bg-red-500/10 border-t border-red-500/30 text-red-400 text-sm">
            启动失败: {(startMutation.error as Error).message}
          </div>
        )}

        {/* 成功提示 */}
        {startMutation.isSuccess && (
          <div className="px-6 py-3 bg-plaita-500/10 border-t border-plaita-500/30 text-plaita-400 text-sm">
            流程已加入队列
          </div>
        )}
      </div>
    </div>
  )
}

