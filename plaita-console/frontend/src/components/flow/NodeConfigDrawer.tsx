import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { GitBranch, Plus, Trash2 } from 'lucide-react'
import { useFlowEditor } from '../../stores/flowEditor'
import { api } from '../../services/api'
import type { FlowNodeData } from './flowConverter'
import SchemaForm, { JsonField } from './schemaForm/SchemaForm'
import { normalizeFieldKeys, type JsonSchema } from './schemaForm/schemaUtils'

// 内嵌 child_flow 子流程的节点类型（reference 仅有内嵌子图时也可编辑）
const SUBFLOW_TYPES = new Set(['map', 'loop', 'filter', 'find', 'reduce', 'child'])

// 节点配置抽屉：通用字段（name/output/timeout）+ schema 驱动的类型特定字段表单。
// 表单变更即时写回 store（与画布交互一致，自动置 dirty）；
// 无 schema 的自定义/未知类型退化为整段 JSON 编辑。
export default function NodeConfigDrawer() {
  const selectedId = useFlowEditor((s) => s.selectedNodeId)
  const node = useFlowEditor((s) => s.nodes.find((n) => n.id === s.selectedNodeId))
  const updateNodeData = useFlowEditor((s) => s.updateNodeData)
  const removeNode = useFlowEditor((s) => s.removeNode)
  const enterSubgraph = useFlowEditor((s) => s.enterSubgraph)

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

  /** 由专门 UI 接管、不进通用表单的键：child_flow 走子图编辑，branches 走分支列表 */
  const formExcludeKeys = new Set<string>(
    d.type === 'parallel'
      ? ['branches']
      : SUBFLOW_TYPES.has(d.type) || d.type === 'reference'
        ? ['child_flow']
        : []
  )

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
            <SchemaForm
              fields={typeFields}
              schema={schema}
              onChange={writeTypeFields}
              excludeKeys={formExcludeKeys}
            />
          ) : (
            <FallbackJson
              fields={typeFields}
              onApply={(next) => writeTypeFields(next)}
            />
          )}
        </div>

        {/* 子流程编辑入口：进入子画布（面包屑返回） */}
        {(SUBFLOW_TYPES.has(d.type) || (d.type === 'reference' && Boolean(typeFields.child_flow))) && (
          <div className="border-t border-line pt-3">
            <button
              onClick={() => enterSubgraph(node.id, 'child_flow')}
              className="w-full flex items-center justify-center gap-1.5 bg-elevated border border-line hover:bg-dark-700 text-ink-primary py-1.5 rounded-md text-caption"
            >
              <GitBranch size={13} />
              编辑子流程
            </button>
            <p className="mt-1 text-[11px] leading-4 text-ink-faint">
              {d.type === 'reference'
                ? '内嵌子流程（与外部 flowID 引用互斥，引擎按调度器注入优先）'
                : '每个元素以 item / index 注入子流程（$INPUT.item / $INPUT.index）'}
            </p>
          </div>
        )}

        {/* parallel：分支列表 + 每分支子图入口 */}
        {d.type === 'parallel' && (
          <ParallelBranches
            fields={typeFields}
            onChange={writeTypeFields}
            onEditBranch={(i) => enterSubgraph(node.id, 'branch', i)}
          />
        )}
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

/** parallel 分支列表：增删、命名、每分支子图入口；条件等细节走 JSON 折叠 */
function ParallelBranches({
  fields,
  onChange,
  onEditBranch,
}: {
  fields: Record<string, unknown>
  onChange: (next: Record<string, unknown>) => void
  onEditBranch: (index: number) => void
}) {
  const [detailOpen, setDetailOpen] = useState(false)
  const branches = (fields.branches as Array<Record<string, unknown>>) || []
  const setBranches = (next: Array<Record<string, unknown>>) =>
    onChange({ ...fields, branches: next })

  return (
    <div className="border-t border-line pt-3">
      <p className="text-caption text-ink-muted mb-2">并行分支</p>
      <div className="space-y-1.5">
        {branches.map((b, i) => (
          <div
            key={i}
            className="flex items-center gap-1.5 px-2 py-1.5 rounded-md bg-elevated border border-line"
          >
            <input
              value={(b.name as string) || ''}
              onChange={(e) => {
                const next = branches.map((x, j) => (j === i ? { ...x, name: e.target.value } : x))
                setBranches(next)
              }}
              placeholder={`分支 ${i + 1}`}
              className="bg-transparent text-caption text-ink-primary outline-none min-w-0 flex-1"
            />
            <button
              onClick={() => onEditBranch(i)}
              className="flex items-center gap-1 text-[11px] text-plaita-400 hover:text-plaita-300 shrink-0"
              title="编辑该分支的子流程"
            >
              <GitBranch size={12} />
              子图
            </button>
            <button
              onClick={() => setBranches(branches.filter((_, j) => j !== i))}
              className="text-ink-faint hover:text-status-error shrink-0"
              title="删除分支"
            >
              <Trash2 size={12} />
            </button>
          </div>
        ))}
      </div>
      <button
        onClick={() =>
          setBranches([...branches, { name: `branch_${branches.length + 1}` }])
        }
        className="mt-1.5 flex items-center gap-1 text-caption text-plaita-400 hover:text-plaita-300"
      >
        <Plus size={12} />
        添加分支
      </button>
      <button
        onClick={() => setDetailOpen((v) => !v)}
        className="mt-2 text-[11px] text-ink-faint hover:text-ink-secondary"
      >
        {detailOpen ? '收起分支配置 JSON' : '分支详细配置（JSON：condition / join 等）'}
      </button>
      {detailOpen && (
        <div className="mt-1.5">
          <JsonField
            value={branches}
            onChange={(v) =>
              onChange({ ...fields, branches: (v as Array<Record<string, unknown>>) ?? [] })
            }
          />
        </div>
      )}
    </div>
  )
}

/** 无 schema 时的整段 JSON 编辑（fallback，保持原能力） */function FallbackJson({
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
