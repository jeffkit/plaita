import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { api, type DryRunNodeResult } from '../../services/api'

interface DryRunPanelProps {
  flowJson: string
  onClose: () => void
}

// 试跑面板：输入 JSON → 调 /api/flows/dry-run → 节点级结果时间线
export default function DryRunPanel({ flowJson, onClose }: DryRunPanelProps) {
  const [inputJson, setInputJson] = useState('{\n  "name": "plaita"\n}')
  const [inputError, setInputError] = useState<string | null>(null)

  const mut = useMutation({
    mutationFn: async () => {
      let input: Record<string, unknown> = {}
      try {
        input = inputJson.trim() ? JSON.parse(inputJson) : {}
        setInputError(null)
      } catch (e) {
        throw new Error(`输入 JSON 非法: ${(e as Error).message}`)
      }
      return api.dryRun({ flowJson, input })
    },
  })

  const result = mut.data
  const nodes: DryRunNodeResult[] = result?.nodes || []

  return (
    <div className="w-96 bg-dark-900/95 border-l border-dark-700 p-4 overflow-y-auto text-sm flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-dark-100">试跑</h3>
        <button onClick={onClose} className="text-dark-400 hover:text-dark-100">✕</button>
      </div>

      <label className="text-xs text-dark-400 mb-1">输入参数（JSON）</label>
      <textarea
        value={inputJson}
        onChange={(e) => setInputJson(e.target.value)}
        rows={5}
        className="input w-full font-mono text-xs mb-2"
      />
      <button
        onClick={() => mut.mutate()}
        disabled={mut.isPending}
        className="bg-plaita-600 hover:bg-plaita-500 disabled:opacity-50 text-white py-1.5 rounded mb-3"
      >
        {mut.isPending ? '执行中…' : '开始试跑'}
      </button>

      {inputError && <p className="text-xs text-red-400 mb-2">{inputError}</p>}
      {mut.isError && <p className="text-xs text-red-400 mb-2">{(mut.error as Error).message}</p>}
      {result?.error && <p className="text-xs text-red-400 mb-2">{result.error}</p>}

      {result && !result.error && (
        <div className="mb-3 p-2 rounded bg-dark-800 border border-dark-700">
          <div className="text-xs text-dark-400">最终结果</div>
          <pre className="text-xs text-green-300 mt-1 whitespace-pre-wrap break-all">
            {JSON.stringify(result.result, null, 2)}
          </pre>
        </div>
      )}

      <div className="text-xs text-dark-400 mb-1">节点执行时间线</div>
      <div className="space-y-2 flex-1">
        {nodes.map((n, i) => (
          <div
            key={(n.id || `n${i}`)}
            className={`p-2 rounded border text-xs ${
              n.status === 'error'
                ? 'bg-red-600/15 border-red-600/50'
                : 'bg-dark-800 border-dark-700'
            }`}
          >
            <div className="flex items-center justify-between">
              <span className="font-mono text-dark-100">{n.name || n.id}</span>
              <span className="text-dark-400">{n.type}</span>
            </div>
            {n.input !== undefined && n.input !== null && (
              <div className="mt-1 text-dark-400">in: <span className="text-dark-200">{short(n.input)}</span></div>
            )}
            {n.output !== undefined && n.output !== null && (
              <div className="mt-0.5 text-dark-400">out: <span className="text-green-300">{short(n.output)}</span></div>
            )}
            {n.error && <div className="mt-1 text-red-300">{n.error}</div>}
          </div>
        ))}
        {nodes.length === 0 && <p className="text-dark-400">无节点结果</p>}
      </div>
      <style>{`.input{background:#1e293b;border:1px solid #334155;border-radius:6px;padding:6px 8px;color:#e2e8f0}`}</style>
    </div>
  )
}

function short(v: unknown): string {
  const s = typeof v === 'string' ? v : JSON.stringify(v)
  return s.length > 80 ? s.slice(0, 80) + '…' : s
}
