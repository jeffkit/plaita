import { useEffect, useState } from 'react'
import { useFlowEditor } from '../../stores/flowEditor'
import type { FlowNodeData } from './flowConverter'

// 节点配置抽屉：编辑选中节点的公共字段 + 类型特定字段（JSON 编辑器）。
// 公共字段：name / output / timeout / desc。
export default function NodeConfigDrawer() {
  const selectedId = useFlowEditor((s) => s.selectedNodeId)
  const node = useFlowEditor((s) => s.nodes.find((n) => n.id === s.selectedNodeId))
  const updateNodeData = useFlowEditor((s) => s.updateNodeData)
  const removeNode = useFlowEditor((s) => s.removeNode)

  const [name, setName] = useState('')
  const [output, setOutput] = useState('')
  const [timeout, setTimeout_] = useState('')
  const [fieldsJson, setFieldsJson] = useState('{}')
  const [jsonError, setJsonError] = useState<string | null>(null)

  useEffect(() => {
    if (!node) return
    const d = node.data as FlowNodeData
    setName(d.name || '')
    setOutput((d.fields.output as string) || '')
    setTimeout_((d.fields.timeout as string) || '')
    const { output: _o, timeout: _t, ...rest } = d.fields
    void _o; void _t
    setFieldsJson(JSON.stringify(rest, null, 2))
    setJsonError(null)
  }, [node])

  if (!selectedId || !node) return null
  const d = node.data as FlowNodeData

  const commit = () => {
    let parsed: Record<string, unknown> = {}
    try {
      parsed = fieldsJson.trim() ? JSON.parse(fieldsJson) : {}
      setJsonError(null)
    } catch (e) {
      setJsonError(`字段 JSON 非法: ${(e as Error).message}`)
      return
    }
    const fields: Record<string, unknown> = { ...parsed }
    if (output) fields.output = output
    if (timeout) fields.timeout = timeout
    updateNodeData(node.id, { name, fields })
  }

  return (
    <div className="w-80 bg-dark-900/90 border-l border-dark-700 p-4 overflow-y-auto text-sm">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-dark-100">节点配置</h3>
        <button
          onClick={() => removeNode(node.id)}
          className="text-xs text-red-400 hover:text-red-300"
        >
          删除节点
        </button>
      </div>

      <div className="space-y-3">
        <Field label="节点 ID">
          <input value={node.id} disabled className="input w-full opacity-60" />
        </Field>
        <Field label="类型">
          <input value={d.type} disabled className="input w-full opacity-60" />
        </Field>
        <Field label="名称 name">
          <input value={name} onChange={(e) => setName(e.target.value)} className="input w-full" />
        </Field>
        <Field label="输出 output（表达式）">
          <input
            value={output}
            onChange={(e) => setOutput(e.target.value)}
            placeholder="$INPUT.name"
            className="input w-full"
          />
        </Field>
        <Field label="超时 timeout（ms）">
          <input
            value={timeout}
            onChange={(e) => setTimeout_(e.target.value)}
            placeholder="3000"
            className="input w-full"
          />
        </Field>
        <Field label="类型特定字段（JSON）">
          <textarea
            value={fieldsJson}
            onChange={(e) => setFieldsJson(e.target.value)}
            rows={8}
            className="input w-full font-mono text-xs"
          />
          {jsonError && <p className="text-xs text-red-400 mt-1">{jsonError}</p>}
        </Field>

        <button
          onClick={commit}
          className="w-full bg-plaita-600 hover:bg-plaita-500 text-white py-2 rounded-md"
        >
          应用
        </button>
      </div>
      <style>{`.input{background:#1e293b;border:1px solid #334155;border-radius:6px;padding:6px 8px;color:#e2e8f0}`}</style>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs text-dark-400 mb-1">{label}</label>
      {children}
    </div>
  )
}
