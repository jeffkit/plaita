import { useState } from 'react'

// 后端 agent 宿主的 SSE 事件（AG-UI 风格最小集）
interface AgentEvent {
  type: string
  attempt?: number
  text?: string
  errors?: string[]
  ok?: boolean
  source?: string
  ir?: Record<string, unknown>
  reason?: string
}

// AI 流程生成对话框：后端经 agentproc 跑真实编码 Agent（事件流），
// 编译校验失败自动回喂自纠；完成后可导入画布。
export default function AiGenerateDialog({
  onImport,
  onClose,
}: {
  onImport: (ir: Record<string, unknown>, info: { rounds: number }) => void
  onClose: () => void
}) {
  const [prompt, setPrompt] = useState('')
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [lines, setLines] = useState<string[]>([])
  const [errorMsg, setErrorMsg] = useState('')
  const [ir, setIr] = useState<Record<string, unknown> | null>(null)

  const push = (line: string) => setLines((prev) => [...prev.slice(-200), line])

  const consumeEvent = (ev: AgentEvent) => {
    switch (ev.type) {
      case 'run_started':
        push('▶ 生成开始（agent 宿主：agentproc）')
        break
      case 'turn_started':
        push(`── 第 ${ev.attempt} 轮 ──`)
        break
      case 'line':
        if (ev.text && ev.text.trim()) push(ev.text)
        break
      case 'compile_failed':
        for (const e of ev.errors || []) push(`✗ 编译失败：${e}`)
        break
      case 'finished':
        if (ev.ok && ev.ir) {
          setIr(ev.ir)
          setStatus('done')
          push('✓ 编译通过，可导入画布')
        } else {
          setStatus('error')
          setErrorMsg(ev.reason || '多轮自纠失败')
        }
        break
    }
  }

  const generate = async () => {
    if (!prompt.trim()) return
    setStatus('running'); setLines([]); setErrorMsg(''); setIr(null)
    try {
      const resp = await fetch('/api/flows/ai-generate/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt }),
      })
      if (!resp.ok || !resp.body) {
        const detail = await resp.text()
        throw new Error(detail.slice(0, 300) || `HTTP ${resp.status}`)
      }
      const reader = resp.body.getReader()
      const dec = new TextDecoder()
      let buf = ''
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        let idx: number
        while ((idx = buf.indexOf('\n\n')) >= 0) {
          const chunk = buf.slice(0, idx)
          buf = buf.slice(idx + 2)
          const dataLine = chunk.split('\n').find((l) => l.startsWith('data: '))
          if (!dataLine) continue
          try {
            consumeEvent(JSON.parse(dataLine.slice(6)) as AgentEvent)
          } catch { /* 忽略无法解析的事件块 */ }
        }
      }
      setStatus((s) => (s === 'running' ? 'error' : s))
    } catch (e: unknown) {
      setErrorMsg(e instanceof Error ? e.message : String(e))
      setStatus((s) => (s === 'done' ? s : 'error'))
    }
  }

  const doImport = () => {
    if (!ir) return
    const rounds = lines.filter((l) => l.startsWith('──')).length || 1
    onImport(ir, { rounds })
  }

  return (
    <div className="fixed inset-0 animate-fade z-50 flex items-center justify-center bg-black/60" onClick={onClose}>
      <div
        className="w-[720px] max-h-[85vh] overflow-y-auto bg-dark-800 border border-dark-600 rounded-xl p-5 space-y-4"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-dark-100">✨ AI 生成流程</h3>
          <button onClick={onClose} className="text-dark-400 hover:text-dark-200">✕</button>
        </div>
        <div>
          <label className="text-xs text-dark-400">需求描述（自然语言；Agent 为 recursive/GLM，经 agentproc 运行）</label>
          <textarea
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            rows={3}
            placeholder={'例：做一个互动闭环流程：采集 twitter 提及，逐条安全分类起草，微信逐条确认后网页回复并销账'}
            className="input w-full mt-1"
          />
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={generate}
            disabled={status === 'running' || !prompt.trim()}
            className="bg-plaita-600 hover:bg-plaita-500 disabled:opacity-50 px-4 py-1.5 rounded text-white text-sm"
          >
            {status === 'running' ? '🤖 Agent 执行中…' : '✨ 生成流程'}
          </button>
          {status === 'done' && <span className="text-xs text-green-400">编译通过</span>}
        </div>
        {errorMsg && (
          <pre className="text-xs text-red-300 bg-red-600/10 rounded p-3 whitespace-pre-wrap">{errorMsg}</pre>
        )}
        {lines.length > 0 && (
          <div>
            <div className="text-xs text-dark-400 mb-1">Agent 执行过程</div>
            <pre className="text-xs bg-dark-900 rounded p-3 overflow-auto max-h-56 text-plaita-200 whitespace-pre-wrap">
              {lines.join('\n')}
            </pre>
          </div>
        )}
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="px-3 py-1.5 rounded bg-dark-700 text-dark-200 text-sm">关闭</button>
          <button
            disabled={status !== 'done' || !ir}
            onClick={doImport}
            className="px-3 py-1.5 rounded bg-plaita-600 hover:bg-plaita-500 text-white text-sm disabled:opacity-40"
          >
            导入画布（覆盖当前内容）
          </button>
        </div>
      </div>
    </div>
  )
}
