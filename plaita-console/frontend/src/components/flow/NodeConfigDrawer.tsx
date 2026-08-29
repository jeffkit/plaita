import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useFlowEditor } from '../../stores/flowEditor'
import { api } from '../../services/api'
import type { FlowNodeData } from './flowConverter'
import SchemaForm from './schemaForm/SchemaForm'
import { normalizeFieldKeys, type JsonSchema } from './schemaForm/schemaUtils'

// 节点配置抽屉：通用字段（name/output/timeout）+ schema 驱动的类型特定字段表单。
// 表单变更即时写回 store（与画布交互一致，自动置 dirty）；
// 无 schema 的自定义/未知类型退化为整段 JSON 编辑。
export default function NodeConfigDrawer() {
  const selectedId = useFlowEditor((s) => s.selectedNodeId)
  const node = useFlowEditor((s) => s.nodes.find((n) => n.id === s.selectedNodeId))
  const updateNodeData = useFlowEditor((s) => s.updateNodeData)
  const removeNode = useFlowEditor((s) => s.removeNode)

  const [name, setName] = useState('')
  const [output, setOutput] = useState('')
  const [timeout, setTimeout_] = useState('')

  // 节点类型 schema：与节点面板共用 ['nodes'] 缓存，不额外请求
  const nodesQuery = useQuery({
    queryKey: ['nodes'],
    queryFn: () => api.getNodes(),
    staleTime: 5 * 60_000,
  })
  const schemaByType = useMemo(() => {
    const map = new Map<string, JsonSchema>()
    for (const d of nodesQuery.data?.nodes || []) {
      try {
        map.set(d.node_type, JSON.parse(d.schema_json || '{}') as JsonSchema)
      } catch {
        // 坏 schema 的类型按无 schema 处理
      }
    }
    return map
  }, [nodesQuery.data])

  useEffect(() => {
    if (!node) return
    const d = node.data as FlowNodeData
    setName(d.name || '')
    setOutput((d.fields.output as string) || '')
    setTimeout_((d.fields.timeout as string) || '')
  }, [node])

  // 归一化必须在 early-return 前（hooks 顺序）
  const schema = node ? (schemaByType.get((node.data as FlowNodeData).type) ?? null) : null
  const typeFields = useMemo(() => {
    const raw = node ? (node.data as FlowNodeData).fields : {}
    const { output: _o, timeout: _t, ...rest } = raw
    void _o
    void _t
    return schema ? normalizeFieldKeys(rest, schema) : rest
  }, [node, schema])

  if (!selectedId || !node) return null
  const d = node.data as FlowNodeData

  /** 写回类型字段（保留通用字段），空值键剔除 */
  const writeTypeFields = (next: Record<string, unknown>) => {
    const fields: Record<string, unknown> = { ...next }
    if (output) fields.output = output
    if (timeout) fields.timeout = timeout
    updateNodeData(node.id, { fields })
  }

  /** 通用字段（output/timeout）即时写回 */
  const writeCommon = (key: 'output' | 'timeout', v: string) => {
    const fields = { ...d.fields }
    if (v === '') delete fields[key]
    else fields[key] = v
    updateNodeData(node.id, { fields })
  }

  return (
    <div className="w-80 shrink-0 bg-surface border-l border-line p-4 overflow-y-auto text-sm">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-section text-ink-primary">节点配置</h3>
        <button
          onClick={() => removeNode(node.id)}
          className="text-caption text-status-error hover:opacity-80"
        >
          删除节点
        </button>
      </div>

      <div className="space-y-3">
        <Field label="节点 ID">
          <input value={node.id} disabled className="input w-full opacity-60 font-mono text-[12px]" />
        </Field>
        <Field label="类型">
          <input value={d.type} disabled className="input w-full opacity-60 font-mono text-[12px]" />
        </Field>
        <Field label="名称">
          <input
            value={name}
            onChange={(e) => {
              setName(e.target.value)
              updateNodeData(node.id, { name: e.target.value })
            }}
            className="input w-full"
          />
        </Field>
        <Field label="输出 output（表达式）">
          <input
            value={output}
            onChange={(e) => {
              setOutput(e.target.value)
              writeCommon('output', e.target.value)
            }}
            placeholder="$INPUT.name"
            className="input w-full font-mono text-[12px]"
          />
        </Field>
        <Field label="超时 timeout（ms）">
          <input
            value={timeout}
            onChange={(e) => {
              setTimeout_(e.target.value)
              writeCommon('timeout', e.target.value)
            }}
            placeholder="3000"
            className="input w-full font-mono text-[12px]"
          />
        </Field>

        <div className="border-t border-line pt-3">
          <p className="text-caption text-ink-muted mb-2">
            类型特定字段
            {schema && <span className="ml-1.5 text-[10px] text-ink-faint">schema 驱动</span>}
          </p>
          {schema ? (
            <SchemaForm fields={typeFields} schema={schema} onChange={writeTypeFields} />
          ) : (
            <FallbackJson
              fields={typeFields}
              onApply={(next) => writeTypeFields(next)}
            />
          )}
        </div>
      </div>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-caption text-ink-muted mb-1">{label}</label>
      {children}
    </div>
  )
}

/** 无 schema 时的整段 JSON 编辑（fallback，保持原能力） */
function FallbackJson({
  fields,
  onApply,
}: {
  fields: Record<string, unknown>
  onApply: (next: Record<string, unknown>) => void
}) {
  const [text, setText] = useState(() => JSON.stringify(fields, null, 2))
  const [error, setError] = useState<string | null>(null)
  const dirty = text !== JSON.stringify(fields, null, 2)
  const apply = () => {
    try {
      const parsed = text.trim() ? JSON.parse(text) : {}
      setError(null)
      onApply(parsed)
    } catch (e) {
      setError(`字段 JSON 非法: ${(e as Error).message}`)
    }
  }
  return (
    <div>
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        rows={10}
        spellCheck={false}
        className={`input w-full font-mono text-[11px] leading-4 ${error ? 'border-status-error/60' : ''}`}
      />
      {error && <p className="mt-1 text-[11px] text-status-error">{error}</p>}
      {dirty && (
        <button
          onClick={apply}
          className="mt-2 w-full bg-plaita-500 hover:bg-plaita-600 text-on-accent py-1.5 rounded-md text-caption"
        >
          应用 JSON
        </button>
      )}
    </div>
  )
}
