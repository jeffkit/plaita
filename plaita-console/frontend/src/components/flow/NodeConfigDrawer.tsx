import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { GitBranch, Plus, Trash2 } from 'lucide-react'
import { useFlowEditor } from '../../stores/flowEditor'
import { api } from '../../services/api'
import type { FlowNodeData } from './flowConverter'
import SchemaForm, { JsonField } from './schemaForm/SchemaForm'
import ConditionEditor from './schemaForm/ConditionEditor'
import ExpressionInput from './schemaForm/ExpressionInput'
import type { VarGroup } from './schemaForm/ExpressionInput'
import { coreFieldsOf } from './schemaForm/coreFields'
import { conditionOperators, normalizeFieldKeys, type JsonSchema } from './schemaForm/schemaUtils'

// 内嵌 child_flow 子流程的节点类型（reference 仅有内嵌子图时也可编辑）
const SUBFLOW_TYPES = new Set(['map', 'loop', 'filter', 'find', 'reduce', 'while', 'child'])
// 顶层 condition 字段的节点类型：条件走三段式构造器（2026-09 表单评审 B4）
const CONDITION_TYPES = new Set(['if', 'loop', 'while', 'filter', 'find'])
const CONDITION_HINT: Record<string, string> = {
  if: '不满足时走 else 分支',
  while: '条件成立时继续循环',
  loop: '满足时继续重复，不满足时结束',
  filter: '对集合元素逐个判断，命中才保留',
  find: '返回第一个命中的元素',
}

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
  const allNodes = useFlowEditor((s) => s.nodes)
  const allEdges = useFlowEditor((s) => s.edges)
  const flowMeta = useFlowEditor((s) => s.meta)

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
  }, [node])

  // Tab 重置只在**切换选中节点**时发生——历史上依赖 [node]，而节点对象在
  // 每次字段编辑后都会换新引用，导致「容错 Tab 里选个超时就被踢回配置」
  // （2026-09 UI 旅程评审用户实测）。
  const prevSelectedId = useRef(selectedId)
  useEffect(() => {
    if (prevSelectedId.current !== selectedId) {
      prevSelectedId.current = selectedId
      setTab('config')
    }
  }, [selectedId])

  // 归一化必须在 early-return 前（hooks 顺序）
  const schema = node ? (schemaByType.get((node.data as FlowNodeData).type) ?? null) : null
  const typeFields = useMemo(() => {
    const raw = node ? (node.data as FlowNodeData).fields : {}
    const { output: _o, timeout: _t, ...rest } = raw
    void _o
    void _t
    return schema ? normalizeFieldKeys(rest, schema) : rest
  }, [node, schema])

  // 引用流程下拉数据源（B6）：与启动弹窗共用 ['flows'] 缓存，仅 reference 节点启用
  const flowsQuery = useQuery({
    queryKey: ['flows'],
    queryFn: () => api.getFlows(),
    enabled: node?.data.type === 'reference',
    staleTime: 30_000,
  })

  // 真上游节点（沿入边反向遍历）：变量目录与「声明上游依赖」下拉共用——
  // 声明上游只能选当前节点之前可达的节点，不能是任意节点（2026-09 用户反馈）
  const upstreamIds = useMemo(() => {
    const ids = new Set<string>()
    if (!node) return ids
    const walk = (id: string, depth: number) => {
      if (depth > 6) return
      for (const e of allEdges) {
        if (e.target !== id || ids.has(e.source)) continue
        ids.add(e.source)
        walk(e.source, depth + 1)
      }
    }
    walk(node.id, 0)
    return ids
  }, [node, allEdges])

  // 变量目录：$INPUT 流程入参 / $NODE 上游结果（沿入边反推）/ $GLOBAL 全局上下文
  const variableGroups = useMemo<VarGroup[]>(() => {
    if (!node) return []
    const groups: VarGroup[] = []
    const inputProps = (flowMeta.inputType as Record<string, unknown> | undefined)?.properties
    if (inputProps && typeof inputProps === 'object') {
      groups.push({
        label: '$INPUT · 流程入参',
        items: Object.keys(inputProps).map((k) => ({ expr: `$INPUT.${k}` })),
      })
    }
    const upstreamItems = allNodes
      .filter((n) => upstreamIds.has(n.id))
      .map((n) => {
        const d = n.data as FlowNodeData
        const out = d.fields.output
        return {
          expr: `$NODE.${n.id}`,
          desc: `${d.type} · ${d.name}${typeof out === 'string' && out ? ` → ${out}` : ''}`,
        }
      })
    if (upstreamItems.length > 0) groups.push({ label: '$NODE · 上游节点结果', items: upstreamItems })
    const gc = flowMeta.globalContext
    if (gc && typeof gc === 'object' && Object.keys(gc).length > 0) {
      groups.push({
        label: '$GLOBAL · 全局上下文',
        items: Object.keys(gc).map((k) => ({ expr: `$GLOBAL.${k}` })),
      })
    }
    return groups
  }, [node, allNodes, allEdges, upstreamIds, flowMeta])

  if (!selectedId || !node) return null
  const d = node.data as FlowNodeData

  /** 由专门 UI 接管、不进通用表单的键：child_flow 走子图编辑，branches 走分支
   *  列表，condition 走三段式构造器。一个节点可同时命中多类（如 loop =
   *  子图入口 + 条件构造器），逐类累加 */
  const formExcludeKeys = new Set<string>()
  if (d.type === 'parallel') formExcludeKeys.add('branches')
  if (d.type === 'assignment') {
    formExcludeKeys.add('output_type')
    formExcludeKeys.add('outputType')
  }
  if (SUBFLOW_TYPES.has(d.type) || d.type === 'reference') formExcludeKeys.add('child_flow')
  if (CONDITION_TYPES.has(d.type)) formExcludeKeys.add('condition')
  if (d.type === 'reference') formExcludeKeys.add('flow_id') // B6：流程下拉接管
  if (d.type === 'assignment') formExcludeKeys.add('upstream_output') // B6：行编辑器接管

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

      {/* key=node.id：切换选中节点时强制重挂载内容区。JsonField/HandlerEditor
          等内部 useState 只在挂载时取值，不重挂载会把上一节点的内容串写进
          当前节点（2026-09 表单评审 P1-7） */}
      <div key={node.id} className="flex-1 overflow-y-auto p-4 space-y-3">
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

            {d.type === 'reference' && (
              <div>
                <label className="block text-caption text-ink-muted mb-1">
                  引用流程
                  <span className="ml-1.5 text-[10px] text-ink-faint">
                    版本留空取最新；入参经下方 input 字段注入（引用流程不共享父上下文）
                  </span>
                </label>
                <select
                  value={String(typeFields.flow_id ?? '')}
                  onChange={(e) =>
                    writeTypeFields({ ...typeFields, flow_id: e.target.value || undefined })
                  }
                  className="input w-full font-mono text-data-sm"
                >
                  <option value="">（选择要引用的流程）</option>
                  {(flowsQuery.data?.flows ?? []).map((f) => (
                    <option key={f.flow_id} value={f.flow_id}>
                      {f.flow_id}
                      {f.desc ? ` — ${f.desc}` : ''}
                    </option>
                  ))}
                  {/* 当前值不在流程列表（如已删除）时保留显示，避免静默丢值 */}
                  {String(typeFields.flow_id ?? '') &&
                    !(flowsQuery.data?.flows ?? []).some((f) => f.flow_id === typeFields.flow_id) && (
                      <option value={String(typeFields.flow_id)}>
                        {String(typeFields.flow_id)}（不在流程列表中）
                      </option>
                    )}
                </select>
              </div>
            )}

            {d.type === 'assignment' && (
              <UpstreamOutputEditor
                value={typeFields.upstream_output}
                nodeIds={allNodes.filter((n) => upstreamIds.has(n.id)).map((n) => n.id)}
                variableGroups={variableGroups}
                onChange={(v) => writeTypeFields({ ...typeFields, upstream_output: v })}
              />
            )}

            {CONDITION_TYPES.has(d.type) && (
              <div>
                <label className="block text-caption text-ink-muted mb-1">
                  执行条件
                  <span className="ml-1.5 text-[10px] text-ink-faint">
                    {CONDITION_HINT[d.type]}
                  </span>
                </label>
                <ConditionEditor
                  value={typeFields.condition}
                  onChange={(v) => writeTypeFields({ ...typeFields, condition: v })}
                  operatorEnum={schema ? conditionOperators(schema) : undefined}
                  variableGroups={variableGroups}
                />
              </div>
            )}

            {d.type === 'assignment' && (
              <div>
                <label className="block text-caption text-ink-muted mb-1">
                  输出类型
                <span className="ml-1.5 text-[10px] text-ink-faint">
                  输出值的类型闸：不匹配时引擎静默返回 None，务必与实际输出一致
                </span>
                </label>
                <OutputTypeEditor
                  value={typeFields.output_type ?? typeFields.outputType}
                  onChange={(v) => {
                    const fields = { ...d.fields }
                    // 两种历史键都清掉，只写 schema 规范键 output_type，
                    // 避免「高级字段」里 output_type / outputType 双写重复
                    delete fields.output_type
                    delete fields.outputType
                    if (v !== undefined) fields.output_type = v
                    updateNodeData(node.id, { fields })
                  }}
                />
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
                  variableGroups={variableGroups}
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

/** assignment 上游依赖声明（B6）：upstream 只能选**真上游**（沿入边可达的
 *  前置节点），value 为取值表达式。引擎语义（plaita/node/assignment.py）：
 *  单条声明=执行时对该表达式求值作为输出；多条=按实际执行到的上一节点匹配
 *  对应声明（分支汇聚场景）。历史上只能在高级 JSON 里手写数组。 */
function UpstreamOutputEditor({
  value,
  nodeIds,
  variableGroups,
  onChange,
}: {
  value: unknown
  nodeIds: string[]
  variableGroups: VarGroup[]
  onChange: (v: unknown) => void
}) {
  const rows = (Array.isArray(value) ? value : []) as Array<Record<string, unknown>>
  const setRows = (next: Array<Record<string, unknown>>) => onChange(next.length ? next : undefined)
  if (nodeIds.length === 0) {
    return (
      <div className="space-y-1.5">
        <p className="text-caption text-ink-muted">上游依赖</p>
        <p className="text-caption text-ink-faint">
          当前节点没有上游连线——先用连线接入前置节点，才能在这里声明从哪个上游取值。
        </p>
      </div>
    )
  }
  return (
    <div className="space-y-1.5">
      <p className="text-caption text-ink-muted">
        上游依赖
        <span className="ml-1.5 text-[10px] text-ink-faint">
          只能声明真实上游（沿连线可达的前置节点）。单条=执行时对该表达式求值作为输出；多条=按实际执行到的上游节点匹配（分支汇聚场景）
        </span>
      </p>
      {rows.map((r, i) => (
        <div key={i} className="flex items-center gap-1.5">
          <select
            value={String(r.upstream ?? '')}
            onChange={(e) =>
              setRows(rows.map((x, j) => (j === i ? { ...x, upstream: e.target.value } : x)))
            }
            className="input w-40 shrink-0 font-mono text-[12px]"
            title="上游节点"
          >
            <option value="">（选择上游节点）</option>
            {nodeIds.map((id) => (
              <option key={id} value={id}>{id}</option>
            ))}
            {String(r.upstream ?? '') && !nodeIds.includes(String(r.upstream)) && (
              <option value={String(r.upstream)}>{String(r.upstream)}（已不是上游）</option>
            )}
          </select>
          <div className="flex-1 min-w-0">
            <UpstreamValueInput
              row={r}
              variableGroups={variableGroups}
              onChange={(n) => setRows(rows.map((x, j) => (j === i ? n : x)))}
            />
          </div>
          <button
            onClick={() => setRows(rows.filter((_, j) => j !== i))}
            className="text-ink-faint hover:text-status-error shrink-0"
            title="移除"
          >
            <Trash2 size={13} />
          </button>
        </div>
      ))}
      <button
        onClick={() => setRows([...rows, { upstream: nodeIds[0] ?? '', value: '' }])}
        className="flex items-center gap-1 text-caption text-plaita-400 hover:text-plaita-300"
      >
        <Plus size={12} />
        声明上游依赖
      </button>
      <p className="text-[11px] leading-4 text-ink-faint">
        取值表达式示例：<span className="font-mono">$NODE.http1.data</span>
        （取 http 节点输出里的 data 字段）、<span className="font-mono">$INPUT.name</span>
        （流程入参）、字面量如 <span className="font-mono">42</span>。结果经类型闸校验后作为本节点输出，下游以{' '}
        <span className="font-mono">$NODE.&lt;本节点id&gt;</span> 引用。
      </p>
    </div>
  )
}

/** 上游取值输入：本地文本态保留输入中间过程（如 "1."），$ 菜单插变量 */
function UpstreamValueInput({
  row,
  variableGroups,
  onChange,
}: {
  row: Record<string, unknown>
  variableGroups: VarGroup[]
  onChange: (v: Record<string, unknown>) => void
}) {
  const [text, setText] = useState(() =>
    typeof row.value === 'string' ? row.value : row.value == null ? '' : JSON.stringify(row.value)
  )
  return (
    <ExpressionInput
      value={text}
      onChange={(v) => {
        setText(v)
        onChange({ ...row, value: v === '' ? undefined : v })
      }}
      groups={variableGroups}
      placeholder="取值表达式，如 $NODE.http1.data"
    />
  )
}

/** assignment 输出类型结构化编辑（Property 结构，2026-09 用户反馈）：
 *  object 定义 children 字段结构、array 定义元素类型、标量直接选——
 *  替代只表达顶层类型名的下拉。语义：输出值经 Property.match 校验，
 *  不匹配时引擎静默返回 None，所以「未设置」= 不校验、原样返回。 */
const OUTPUT_SCALAR_TYPES = ['string', 'integer', 'number', 'boolean', 'any']

function OutputTypeEditor({
  value,
  onChange,
}: {
  value: unknown
  onChange: (v: unknown) => void
}) {
  const prop = (typeof value === 'object' && value !== null ? value : {}) as Record<string, unknown>
  const dataType = String(prop.data_type ?? prop.dataType ?? '')
  const itemTypeRaw = (prop.item_type ?? prop.itemType) as Record<string, unknown> | undefined
  const itemType = String(itemTypeRaw?.data_type ?? itemTypeRaw?.dataType ?? 'string')
  const childrenRaw = prop.children
  const childRows: Array<{ name: string; type: string }> = Array.isArray(childrenRaw)
    ? (childrenRaw as Array<Record<string, unknown>>).map((c) => ({
        name: String(c?.name ?? ''),
        type: String(c?.data_type ?? c?.dataType ?? 'string'),
      }))
    : childrenRaw && typeof childrenRaw === 'object'
      ? Object.entries(childrenRaw as Record<string, unknown>).map(([k, v]) => ({
          name: k,
          type: String((v as Record<string, unknown>)?.data_type ?? (v as Record<string, unknown>)?.dataType ?? 'string'),
        }))
      : []

  const emit = (next: Record<string, unknown> | null) => {
    if (next === null || Object.keys(next).length === 0) {
      onChange(undefined)
      return
    }
    onChange(next)
  }
  const setDataType = (t: string) => {
    if (!t) return emit(null)
    if (t === 'object') {
      const children = Object.fromEntries(
        childRows.filter((r) => r.name.trim()).map((r) => [r.name.trim(), { data_type: r.type }])
      )
      return emit({ data_type: 'object', children })
    }
    if (t === 'array') return emit({ data_type: 'array', item_type: { data_type: itemType || 'string' } })
    return emit({ data_type: t })
  }
  const setChildren = (rows: Array<{ name: string; type: string }>) =>
    emit({
      data_type: 'object',
      children: Object.fromEntries(
        rows.filter((r) => r.name.trim()).map((r) => [r.name.trim(), { data_type: r.type }])
      ),
    })

  return (
    <div className="space-y-1.5">
      <select value={dataType} onChange={(e) => setDataType(e.target.value)} className="input w-full">
        <option value="">未设置（不做类型校验，输出原样返回）</option>
        <option value="object">对象（dict）— 可定义字段结构</option>
        <option value="array">数组（list）— 可定义元素类型</option>
        {OUTPUT_SCALAR_TYPES.filter((t) => t !== 'any').map((t) => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>
      {dataType === 'array' && (
        <div className="flex items-center gap-1.5">
          <span className="text-caption text-ink-muted shrink-0">元素类型</span>
          <select
            value={itemType}
            onChange={(e) => emit({ data_type: 'array', item_type: { data_type: e.target.value } })}
            className="input w-32"
          >
            {OUTPUT_SCALAR_TYPES.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
      )}
      {dataType === 'object' && (
        <div className="border-l border-line pl-3 space-y-1.5">
          <p className="text-[11px] text-ink-faint">字段结构（输出 dict 应包含的键及其类型）：</p>
          {childRows.map((r, i) => (
            <div key={i} className="flex items-center gap-1.5">
              <input
                value={r.name}
                onChange={(e) =>
                  setChildren(childRows.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))
                }
                placeholder="字段名"
                className="input w-36 font-mono text-data-sm"
              />
              <select
                value={OUTPUT_SCALAR_TYPES.includes(r.type) ? r.type : 'any'}
                onChange={(e) =>
                  setChildren(childRows.map((x, j) => (j === i ? { ...x, type: e.target.value } : x)))
                }
                className="input w-28"
              >
                {OUTPUT_SCALAR_TYPES.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <button
                onClick={() => setChildren(childRows.filter((_, j) => j !== i))}
                className="text-ink-faint hover:text-status-error shrink-0"
                title="移除字段"
              >
                <Trash2 size={13} />
              </button>
            </div>
          ))}
          <button
            onClick={() => setChildren([...childRows, { name: '', type: 'string' }])}
            className="flex items-center gap-1 text-caption text-plaita-400 hover:text-plaita-300"
          >
            <Plus size={12} />
            添加输出字段
          </button>
          <p className="text-[10px] text-ink-faint">更深的嵌套结构请切「源码」直接编辑定义。</p>
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
