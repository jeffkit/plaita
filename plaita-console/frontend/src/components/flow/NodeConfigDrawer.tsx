import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { GitBranch, Plus, Trash2 } from 'lucide-react'
import { useFlowEditor } from '../../stores/flowEditor'
import { api } from '../../services/api'
import type { FlowNodeData } from './flowConverter'
import SchemaForm, { JsonField } from './schemaForm/SchemaForm'
import { coreFieldsOf } from './schemaForm/coreFields'
import { normalizeFieldKeys, type JsonSchema } from './schemaForm/schemaUtils'

// 内嵌 child_flow 子流程的节点类型（reference 仅有内嵌子图时也可编辑）
const SUBFLOW_TYPES = new Set(['map', 'loop', 'filter', 'find', 'reduce', 'child'])

type DrawerTab = 'config' | 'basic' | 'fault'
const DRAWER_TABS: Array<{ key: DrawerTab; label: string }> = [
  { key: 'config', label: '配置' },
  { key: 'basic', label: '基础' },
  { key: 'fault', label: '容错' },
]

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
  const [desc, setDesc] = useState('')
  const [output, setOutput] = useState('')
  const [timeout, setTimeout_] = useState('')
  const [tab, setTab] = useState<DrawerTab>('config')

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
    setDesc((d.fields.desc as string) || '')
    setOutput((d.fields.output as string) || '')
    setTimeout_((d.fields.timeout as string) || '')
    setTab('config') // 切换节点时回到「配置」Tab
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

  /** 通用单值字段（desc/output/timeout）即时写回，空值删键 */
  const writeField = (key: string, v: string) => {
    const fields = { ...d.fields }
    if (v === '') delete fields[key]
    else fields[key] = v
    updateNodeData(node.id, { fields })
  }

  /** 结构化通用字段（timeout_handler/error_handler）写回 */
  const writeHandler = (key: 'timeout_handler' | 'error_handler', v: unknown) => {
    const fields = { ...d.fields }
    if (v === undefined) delete fields[key]
    else fields[key] = v
    updateNodeData(node.id, { fields })
  }

  return (
    <div className="w-96 shrink-0 bg-surface border-l border-line flex flex-col text-sm">
      <div className="flex items-center justify-between pl-4 pr-3 pt-3">
        <h3 className="text-section text-ink-primary">节点配置</h3>
        <button
          onClick={() => removeNode(node.id)}
          className="flex items-center gap-1 text-caption text-status-error hover:opacity-80"
        >
          <Trash2 size={12} />
          删除
        </button>
      </div>

      {/* Tab 栏 */}
      <div className="flex gap-1 px-3 pt-1.5 border-b border-line">
        {DRAWER_TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-3 pb-2 pt-1.5 text-caption transition-colors border-b-2 -mb-px ${
              tab === t.key
                ? 'text-ink-primary border-plaita-400 font-medium'
                : 'text-ink-muted border-transparent hover:text-ink-secondary'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-3">
        {tab === 'config' && (
          <>
            {/* 子流程编辑入口：进入子画布（面包屑返回） */}
            {(SUBFLOW_TYPES.has(d.type) ||
              (d.type === 'reference' && Boolean(typeFields.child_flow))) && (
              <div>
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

            <div>
              <p className="text-caption text-ink-muted mb-2">
                类型特定字段
                {schema && (
                  <span className="ml-1.5 text-[10px] text-ink-faint">
                    schema 驱动 · 核心参数置顶
                  </span>
                )}
              </p>
              {schema ? (
                <SchemaForm
                  fields={typeFields}
                  schema={schema}
                  onChange={writeTypeFields}
                  excludeKeys={formExcludeKeys}
                  coreFields={coreFieldsOf(d.type)}
                />
              ) : (
                <FallbackJson
                  fields={typeFields}
                  onApply={(next) => writeTypeFields(next)}
                />
              )}
            </div>

            {/* parallel：分支列表 + 每分支子图入口 */}
            {d.type === 'parallel' && (
              <ParallelBranches
                fields={typeFields}
                onChange={writeTypeFields}
                onEditBranch={(i) => enterSubgraph(node.id, 'branch', i)}
              />
            )}
          </>
        )}

        {tab === 'basic' && (
          <>
            <Field label="节点 ID">
              <input
                value={node.id}
                disabled
                className="input w-full opacity-60 font-mono text-[12px]"
              />
            </Field>
            <Field label="类型">
              <input
                value={d.type}
                disabled
                className="input w-full opacity-60 font-mono text-[12px]"
              />
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
            <Field label="描述 desc">
              <input
                value={desc}
                onChange={(e) => {
                  setDesc(e.target.value)
                  writeField('desc', e.target.value)
                }}
                placeholder="节点用途说明"
                className="input w-full"
              />
            </Field>
            <Field label="输出 output（表达式）">
              <input
                value={output}
                onChange={(e) => {
                  setOutput(e.target.value)
                  writeField('output', e.target.value)
                }}
                placeholder="$INPUT.name"
                className="input w-full font-mono text-[12px]"
              />
            </Field>
          </>
        )}

        {tab === 'fault' && (
          <>
            <Field label="执行超时 timeout（ms）">
              <input
                value={timeout}
                onChange={(e) => {
                  setTimeout_(e.target.value)
                  writeField('timeout', e.target.value)
                }}
                placeholder="3000"
                className="input w-full font-mono text-[12px]"
              />
            </Field>
            <HandlerEditor
              label="超时处理 timeout_handler"
              hint="节点执行超时后的处理策略"
              value={d.fields.timeout_handler}
              onChange={(v) => writeHandler('timeout_handler', v)}
            />
            <HandlerEditor
              label="失败处理 error_handler"
              hint="节点执行抛错后的处理策略"
              value={d.fields.error_handler}
              recoverable
              onChange={(v) => writeHandler('error_handler', v)}
            />
          </>
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

/** 错误处理策略选项（与引擎 ErrorStrategy 对齐） */
const STRATEGY_OPTIONS = [
  { value: '', label: '（未启用）' },
  { value: 'abort', label: 'abort · 中止流程' },
  { value: 'continue', label: 'continue · 忽略并继续' },
  { value: 'continue-with', label: 'continue-with · 返回默认值' },
]

/**
 * 超时/失败处理的固定表单（Node 基类共有字段，不走 SchemaForm）。
 * 键名用引擎序列化别名：strategy / code / message / defaultValue / retryTimes。
 */
function HandlerEditor({
  label,
  hint,
  value,
  onChange,
  recoverable = false,
}: {
  label: string
  hint?: string
  value: unknown
  onChange: (v: unknown) => void
  recoverable?: boolean
}) {
  const obj =
    value && typeof value === 'object' && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null
  const strategy = (obj?.strategy as string) || ''

  // 切换策略时重建干净对象：只保留与新策略相关的字段
  const pick = (s: string) => {
    if (!s) {
      onChange(undefined)
      return
    }
    const next: Record<string, unknown> = { strategy: s }
    if (recoverable && obj?.retryTimes != null) next.retryTimes = obj.retryTimes
    if (s === 'abort') {
      if (obj?.code != null) next.code = obj.code
      if (obj?.message != null) next.message = obj.message
    }
    if (s === 'continue-with' && obj?.defaultValue != null) next.defaultValue = obj.defaultValue
    onChange(next)
  }
  const merge = (patch: Record<string, unknown>) => onChange({ ...obj, strategy, ...patch })

  return (
    <div>
      <label className="block text-caption text-ink-muted mb-1">{label}</label>
      {hint && <p className="mb-1.5 text-[11px] leading-4 text-ink-faint">{hint}</p>}
      <select value={strategy} onChange={(e) => pick(e.target.value)} className="input w-full">
        {STRATEGY_OPTIONS.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
      {strategy === 'abort' && (
        <div className="mt-1.5 grid grid-cols-2 gap-1.5">
          <input
            type="number"
            value={obj?.code != null ? String(obj.code) : ''}
            placeholder="错误码（默认 -9527）"
            onChange={(e) => merge(e.target.value === '' ? {} : { code: Number(e.target.value) })}
            className="input w-full font-mono text-[12px]"
          />
          <input
            value={(obj?.message as string) || ''}
            placeholder="错误消息"
            onChange={(e) => merge(e.target.value === '' ? {} : { message: e.target.value })}
            className="input w-full"
          />
        </div>
      )}
      {strategy === 'continue-with' && (
        <div className="mt-1.5">
          <label className="mb-1 block text-[11px] text-ink-faint">
            默认返回值 defaultValue（JSON 对象）
          </label>
          <JsonField
            value={obj?.defaultValue}
            onChange={(v) => merge({ defaultValue: v === undefined ? null : v })}
            compact
          />
        </div>
      )}
      {recoverable && strategy && (
        <div className="mt-1.5 flex items-center gap-2">
          <label className="shrink-0 text-[11px] text-ink-faint">失败重试次数 retryTimes</label>
          <input
            type="number"
            min={0}
            value={obj?.retryTimes != null ? String(obj.retryTimes) : '0'}
            onChange={(e) => merge({ retryTimes: Math.max(0, Number(e.target.value) || 0) })}
            className="input w-20 font-mono text-[12px]"
          />
        </div>
      )}
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
