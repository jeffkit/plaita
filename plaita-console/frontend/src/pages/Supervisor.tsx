/**
 * Supervisor 面板：对一条 flow 跑自迭代（基线评测 → LLM 提案 → 候选评测 →
 * 对比 → 闸门），展示 promotion ticket；发布复用既有 publishFlow（人工动作）。
 *
 * 后端：/api/flows/{id}/supervisor/*（懒加载 plaita-ai；未安装返回 503 指引）。
 */
import { useCallback, useEffect, useState } from 'react'
import { RefreshCw, Sparkles, CheckCircle2, XCircle } from 'lucide-react'
import { api } from '../services/api'

interface IterationOutcome {
  status: string
  candidate_version?: string
  baseline_version?: string
  rationale?: string
  promoted?: boolean
  comparison?: {
    pass_rate?: { base?: number; candidate?: number; delta?: number | null }
    regressions?: string[]
    improvements?: string[]
  }
  promotion_ticket?: { version: string; command: string; reason: string }
  error?: string
}

interface LoopResult {
  final_status: string
  iterations_run: number
  iterations: IterationOutcome[]
}

interface DatasetInfo {
  name: string
  kind: string
  case_files: number
}

interface IterationOutcomeBoxProps {
  outcome: IterationOutcome
  onApply: (version: string) => void
  applying: boolean
}

function OutcomeBox({ outcome, onApply, applying }: IterationOutcomeBoxProps) {
  const ticket = outcome.promotion_ticket
  const pr = outcome.comparison?.pass_rate
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-4 text-sm">
      <div className="flex items-center gap-2">
        {outcome.status === 'improved'
          ? <CheckCircle2 className="text-green-600" size={16} />
          : <XCircle className="text-gray-400" size={16} />}
        <span className="font-medium">{outcome.status}</span>
        {outcome.candidate_version && (
          <span className="text-gray-500">
            {outcome.baseline_version} → {outcome.candidate_version}
          </span>
        )}
        {pr && (
          <span className="ml-auto font-mono text-xs">
            pass_rate {pr.base ?? '-'} → {pr.candidate ?? '-'} (Δ {pr.delta ?? '-'})
          </span>
        )}
      </div>
      {outcome.rationale && <p className="mt-2 text-gray-600">{outcome.rationale}</p>}
      {outcome.comparison && (outcome.comparison.improvements?.length || outcome.comparison.regressions?.length) ? (
        <p className="mt-1 text-xs text-gray-500">
          改善: {outcome.comparison.improvements?.join(', ') || '无'} · 回归:{' '}
          {outcome.comparison.regressions?.join(', ') || '无'}
        </p>
      ) : null}
      {ticket && (
        <div className="mt-3 rounded-md border border-amber-300 bg-amber-50 p-3">
          <p className="font-medium text-amber-800">
            Promotion ticket: v{ticket.version}
          </p>
          <p className="mt-1 font-mono text-xs text-amber-700">{ticket.command}</p>
          <button
            className="mt-2 rounded bg-amber-600 px-3 py-1 text-xs font-medium text-white hover:bg-amber-700 disabled:opacity-50"
            disabled={applying}
            onClick={() => onApply(ticket.version)}
          >
            {applying ? '发布中…' : '确认并发布（人工闸门）'}
          </button>
        </div>
      )}
    </div>
  )
}

export default function Supervisor() {
  const [flows, setFlows] = useState<Array<{ flow_id: string }>>([])
  const [flowId, setFlowId] = useState('')
  const [datasets, setDatasets] = useState<DatasetInfo[]>([])
  const [dataset, setDataset] = useState('')
  const [maxIterations, setMaxIterations] = useState(1)
  const [config, setConfig] = useState<{ plaita_ai: string; proposer: string; dataset_root: string } | null>(null)
  const [running, setRunning] = useState(false)
  const [applying, setApplying] = useState(false)
  const [result, setResult] = useState<LoopResult | null>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    api.getFlows().then((r) => setFlows(r.flows)).catch(() => setFlows([]))
    fetch('/api/supervisor/config').then((r) => r.json()).then(setConfig).catch(() => setConfig(null))
  }, [])

  useEffect(() => {
    if (!flowId) return
    setDatasets([])
    fetch(`/api/flows/${flowId}/supervisor/datasets`)
      .then((r) => r.json())
      .then((r) => setDatasets(r.datasets || []))
      .catch(() => setDatasets([]))
  }, [flowId])

  const run = useCallback(async () => {
    if (!flowId || !dataset) return
    setRunning(true)
    setError('')
    setResult(null)
    try {
      const resp = await fetch(`/api/flows/${flowId}/supervisor/iterate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dataset, max_iterations: maxIterations }),
      })
      const body = await resp.json()
      if (!resp.ok) {
        setError(typeof body.detail === 'string' ? body.detail : JSON.stringify(body.detail))
      } else {
        setResult(body)
      }
    } catch (e) {
      setError(String(e))
    } finally {
      setRunning(false)
    }
  }, [flowId, dataset, maxIterations])

  const applyTicket = useCallback(
    async (version: string) => {
      if (!flowId) return
      setApplying(true)
      try {
        await api.publishFlow(flowId, version)
        window.alert(`已发布 ${flowId}@${version}`)
      } catch (e) {
        window.alert(`发布失败: ${String(e)}`)
      } finally {
        setApplying(false)
      }
    },
    [flowId],
  )

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-2">
        <Sparkles size={18} className="text-indigo-600" />
        <h1 className="text-lg font-semibold">Supervisor · 自迭代工作流</h1>
      </div>

      {config && (config.plaita_ai !== 'ok' || !config.proposer.startsWith('ok')) && (
        <div className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          plaita-ai: {config.plaita_ai} · proposer: {config.proposer} · 数据集根目录: {config.dataset_root}
        </div>
      )}

      <div className="flex flex-wrap items-end gap-3 rounded-lg border border-gray-200 bg-white p-4">
        <label className="text-sm">
          <span className="mb-1 block text-gray-600">Flow</span>
          <select
            className="w-56 rounded border border-gray-300 px-2 py-1.5"
            value={flowId}
            onChange={(e) => { setFlowId(e.target.value); setResult(null) }}
          >
            <option value="">选择…</option>
            {flows.map((f) => (
              <option key={f.flow_id} value={f.flow_id}>{f.flow_id}</option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-gray-600">评测集</span>
          <select
            className="w-56 rounded border border-gray-300 px-2 py-1.5"
            value={dataset}
            onChange={(e) => setDataset(e.target.value)}
            disabled={!flowId}
          >
            <option value="">{datasets.length ? '选择…' : '（无可用数据集）'}</option>
            {datasets.map((d) => (
              <option key={d.name} value={d.name}>{d.name}（{d.case_files} 文件）</option>
            ))}
          </select>
        </label>
        <label className="text-sm">
          <span className="mb-1 block text-gray-600">迭代轮数</span>
          <input
            type="number" min={1} max={5} value={maxIterations}
            onChange={(e) => setMaxIterations(Number(e.target.value) || 1)}
            className="w-20 rounded border border-gray-300 px-2 py-1.5"
          />
        </label>
        <button
          className="flex items-center gap-1 rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
          disabled={running || !flowId || !dataset}
          onClick={run}
        >
          <RefreshCw size={14} className={running ? 'animate-spin' : ''} />
          {running ? '迭代中…' : '跑一轮自迭代'}
        </button>
      </div>

      {error && (
        <div className="rounded-md border border-red-300 bg-red-50 p-3 text-sm text-red-700">{error}</div>
      )}

      {result && (
        <div className="space-y-3">
          <p className="text-sm text-gray-600">
            final_status=<span className="font-mono">{result.final_status}</span> · 共 {result.iterations_run} 轮
          </p>
          {result.iterations.map((it, i) => (
            <OutcomeBox key={i} outcome={it} onApply={applyTicket} applying={applying} />
          ))}
        </div>
      )}
    </div>
  )
}
