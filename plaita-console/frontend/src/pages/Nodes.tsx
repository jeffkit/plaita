import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2, X } from 'lucide-react'
import { api } from '../services/api'
import type { NodeDescriptorView, PropertyTypeView, UpsertPropertyTypeRequest } from '../services/api'
import { Page, PageHeader, Button, EmptyState, Table, Th, Tr, Td, TdData } from '../components/ui'

/**
 * 节点管理（2026-09 C 线重构）：
 * - 列表纯表格（类型/展示名/分类/来源/字段数/代码位置/操作）+ 搜索与筛选
 * - 注册/编辑走右侧抽屉；参数 schema 支持「字段列表 ⇄ JSON 源码」双视图，
 *   公共基础字段自动排除，只露出业务需要配置的字段（含默认值）
 * - 自定义类型 Tab：console 侧命名别名（base_type + enum/default），保存节点
 *   schema 时展开为内置基础类型，运行时永不接触自定义类型名
 */

// Node 基类公共实例字段（plaita/node/basic.py）：注册表单只编辑业务字段，
// 保存时原样保留这些键（编辑内置节点时尤其重要）
const BASE_KEYS = new Set([
  'id', 'name', 'desc', 'output', 'next', 'timeout',
  'source_line', 'timeout_handler', 'error_handler',
])

const BASIC_TYPES = ['string', 'number', 'integer', 'boolean', 'array', 'object'] as const

/** 字段列表的一行（编辑态） */
interface FieldRow {
  key: string
  title: string
  /** 基础类型名 | 'enum' | 自定义类型名 */
  type: string
  /** type=enum 时的选项（逗号分隔文本态） */
  enumText: string
  /** type=array 的元素类型 */
  itemsType: string
  required: boolean
  defaultText: string
  desc: string
}

function emptyRow(): FieldRow {
  return { key: '', title: '', type: 'string', enumText: '', itemsType: 'string', required: false, defaultText: '', desc: '' }
}

/** 自定义类型 → 展开后的 JSON Schema 片段（别名展开：运行时只见内置类型） */
function expandCustomType(t: PropertyTypeView, overrideEnum?: string[]): Record<string, unknown> {
  const s: Record<string, unknown> = { type: t.base_type }
  const enumOptions = overrideEnum ?? t.enum_options
  if (Array.isArray(enumOptions) && enumOptions.length) s.enum = enumOptions
  else if (t.default_value !== null && t.default_value !== undefined) s.default = t.default_value
  if (t.default_value !== null && t.default_value !== undefined && Array.isArray(enumOptions) && enumOptions.length) {
    s.default = t.default_value
  }
  if (t.desc) s.description = s.description ?? t.desc
  return s
}

/** 行 → JSON Schema 属性片段；自定义类型经类型注册表展开 */
function rowToSchema(row: FieldRow, typeRegistry: Map<string, PropertyTypeView>): Record<string, unknown> | null {
  if (!row.key.trim()) return null
  const s: Record<string, unknown> = {}
  const custom = typeRegistry.get(row.type)
  if (custom) {
    Object.assign(s, expandCustomType(custom, row.enumText ? row.enumText.split(',').map((x) => x.trim()).filter(Boolean) : undefined))
  } else if (row.type === 'enum') {
    s.type = 'string'
    s.enum = row.enumText.split(',').map((x) => x.trim()).filter(Boolean)
  } else if (row.type === 'array') {
    s.type = 'array'
    s.items = { type: row.itemsType || 'string' }
  } else {
    s.type = row.type
  }
  if (row.title.trim()) s.title = row.title.trim()
  if (row.desc.trim()) s.description = row.desc.trim()
  const d = coerceDefault(row)
  if (d !== undefined) s.default = d
  return s
}

function coerceDefault(row: FieldRow): unknown {
  if (row.defaultText === '') return undefined
  if (row.type === 'number' || row.type === 'integer') {
    const n = Number(row.defaultText)
    return Number.isFinite(n) ? n : undefined
  }
  if (row.type === 'boolean') return row.defaultText === 'true' ? true : row.defaultText === 'false' ? false : undefined
  return row.defaultText
}

/** schema_json → 编辑行（跳过公共基础字段；枚举识别为一等类型展示） */
function schemaToRows(schemaJson: string, typeRegistry: Map<string, PropertyTypeView>): FieldRow[] {
  let parsed: Record<string, unknown>
  try {
    parsed = JSON.parse(schemaJson || '{}')
  } catch {
    return []
  }
  const props = (parsed.properties ?? {}) as Record<string, Record<string, unknown>>
  const required = (parsed.required as string[]) ?? []
  const rows: FieldRow[] = []
  for (const [key, v] of Object.entries(props)) {
    if (BASE_KEYS.has(key)) continue
    if (!v || typeof v !== 'object') continue
    const type = Array.isArray(v.type) ? (v.type.find((x) => x !== 'null') as string) : (v.type as string)
    const enumArr = v.enum as unknown[] | undefined
    // 反向识别自定义类型：base/enum/default 全部吻合才回显类型名（尽力而为）
    let displayType = enumArr?.length ? 'enum' : (type ?? 'object')
    for (const t of typeRegistry.values()) {
      if (
        t.base_type === displayType &&
        JSON.stringify(t.enum_options ?? []) === JSON.stringify(enumArr ?? []) &&
        ('default' in v ? JSON.stringify(t.default_value) === JSON.stringify(v.default) : t.default_value == null)
      ) {
        displayType = t.name
        break
      }
    }
    const items = (v.items ?? {}) as Record<string, unknown>
    rows.push({
      key,
      title: (v.title as string) ?? '',
      type: displayType,
      enumText: enumArr?.length ? enumArr.map(String).join(', ') : '',
      itemsType: (items.type as string) ?? 'string',
      required: required.includes(key),
      defaultText: 'default' in v ? String(v.default) : '',
      desc: (v.description as string) ?? '',
    })
  }
  return rows
}

export default function Nodes() {
  const [tab, setTab] = useState<'nodes' | 'types'>('nodes')
  const [drawer, setDrawer] = useState<{ mode: 'create' } | { mode: 'edit'; node: NodeDescriptorView } | null>(null)

  const nodesQuery = useQuery({ queryKey: ['nodes'], queryFn: () => api.getNodes() })
  const typesQuery = useQuery({ queryKey: ['property-types'], queryFn: () => api.getPropertyTypes() })
  const nodes = nodesQuery.data?.nodes ?? []
  const typeRegistry = useMemo(
    () => new Map((typesQuery.data?.types ?? []).map((t) => [t.name, t])),
    [typesQuery.data]
  )
  const categories = useMemo(
    () => Array.from(new Set(nodes.map((n) => n.category).filter(Boolean))).sort(),
    [nodes]
  )

  return (
    <Page>
      <PageHeader
        title="节点管理"
        subtitle="内置节点与自定义节点描述；自定义描述驱动编排表单，执行需运行时存在同类型可执行节点"
      />
      <div className="mb-3 flex gap-1">
        {(
          [
            { k: 'nodes', label: `节点（${nodes.length}）` },
            { k: 'types', label: `自定义类型（${typeRegistry.size}）` },
          ] as const
        ).map((t) => (
          <button
            key={t.k}
            onClick={() => setTab(t.k)}
            className={`px-2.5 py-1 rounded-md text-caption transition-colors border ${
              tab === t.k
                ? 'bg-plaita-500/10 text-plaita-400 border-plaita-500/40'
                : 'text-ink-muted border-line hover:text-ink-primary hover:bg-elevated'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'nodes' ? (
        <NodesTab
          nodes={nodes}
          loading={nodesQuery.isLoading}
          categories={categories}
          onOpenDrawer={setDrawer}
        />
      ) : (
        <TypesTab types={typesQuery.data?.types ?? []} />
      )}

      {drawer && (
        <NodeDrawer
          mode={drawer.mode}
          node={drawer.mode === 'edit' ? drawer.node : null}
          typeRegistry={typeRegistry}
          categories={categories}
          onClose={() => setDrawer(null)}
        />
      )}
    </Page>
  )
}

// ── 节点 Tab：搜索/筛选 + 表格 ───────────────────────────────────────────

function NodesTab({
  nodes,
  loading,
  categories,
  onOpenDrawer,
}: {
  nodes: NodeDescriptorView[]
  loading: boolean
  categories: string[]
  onOpenDrawer: (d: { mode: 'create' } | { mode: 'edit'; node: NodeDescriptorView }) => void
}) {
  const qc = useQueryClient()
  const [keyword, setKeyword] = useState('')
  const [category, setCategory] = useState('')
  const [source, setSource] = useState('')

  const del = useMutation({
    mutationFn: (nodeType: string) => api.deleteNode(nodeType),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['nodes'] }),
  })

  const filtered = nodes.filter((n) => {
    if (keyword && !`${n.node_type} ${n.node_name}`.toLowerCase().includes(keyword.toLowerCase())) return false
    if (category && n.category !== category) return false
    if (source === 'builtin' && !n.is_builtin) return false
    if (source === 'custom' && n.is_builtin) return false
    return true
  })

  const fieldCount = (n: NodeDescriptorView): number => {
    try {
      const props = (JSON.parse(n.schema_json || '{}').properties ?? {}) as Record<string, unknown>
      return Object.keys(props).filter((k) => !BASE_KEYS.has(k)).length
    } catch {
      return 0
    }
  }

  return (
    <>
      <div className="mb-3 flex items-center gap-2 flex-wrap">
        <input
          value={keyword}
          onChange={(e) => setKeyword(e.target.value)}
          placeholder="搜索类型 / 展示名…"
          className="input w-56"
        />
        <select value={category} onChange={(e) => setCategory(e.target.value)} className="input w-32">
          <option value="">全部分类</option>
          {categories.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        <select value={source} onChange={(e) => setSource(e.target.value)} className="input w-28">
          <option value="">全部来源</option>
          <option value="builtin">内置</option>
          <option value="custom">自定义</option>
        </select>
        <div className="flex-1" />
        <Button variant="primary" onClick={() => onOpenDrawer({ mode: 'create' })}>
          <Plus size={13} />
          注册节点
        </Button>
      </div>

      <div className="rounded-lg border border-line overflow-hidden bg-surface">
        <Table>
          <thead>
            <tr>
              <Th>节点类型</Th>
              <Th>展示名</Th>
              <Th>分类</Th>
              <Th>来源</Th>
              <Th className="text-right">字段数</Th>
              <Th>代码位置</Th>
              <Th className="text-right">操作</Th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((n) => (
              <Tr key={n.node_type}>
                <TdData className="text-ink-primary">{n.node_type}</TdData>
                <Td>{n.node_name || <span className="text-ink-faint">—</span>}</Td>
                <Td>{n.category || <span className="text-ink-faint">—</span>}</Td>
                <Td>
                  <span
                    className={`text-[11px] px-1.5 py-0.5 rounded-md border ${
                      n.is_builtin ? 'bg-elevated text-ink-muted border-line' : 'text-ink-secondary border-line'
                    }`}
                  >
                    {n.is_builtin ? '内置' : '自定义'}
                  </span>
                </Td>
                <TdData className="text-right tabular-nums">{fieldCount(n)}</TdData>
                <TdData className="max-w-56 truncate" title={n.is_builtin ? `${n.source_module}.${n.source_class}` : '控制台元数据（无可执行代码）'}>
                  {n.is_builtin ? `${n.source_module}.${n.source_class}` : 'console://metadata'}
                </TdData>
                <Td className="text-right whitespace-nowrap">
                  <button
                    onClick={() => onOpenDrawer({ mode: 'edit', node: n })}
                    className="text-caption text-plaita-400 hover:text-plaita-300 mr-3"
                  >
                    {n.is_builtin ? '查看' : '编辑'}
                  </button>
                  {!n.is_builtin && (
                    <button
                      onClick={() => {
                        if (window.confirm(`删除自定义节点 ${n.node_type}？`)) del.mutate(n.node_type)
                      }}
                      className="text-caption text-ink-faint hover:text-status-error"
                      title="删除"
                    >
                      <Trash2 size={13} className="inline" />
                    </button>
                  )}
                </Td>
              </Tr>
            ))}
            {!loading && filtered.length === 0 && (
              <tr>
                <td colSpan={7} className="py-8">
                  <EmptyState message="没有匹配的节点" hint="调整搜索/筛选，或注册自定义节点描述" />
                </td>
              </tr>
            )}
          </tbody>
        </Table>
      </div>
    </>
  )
}

// ── 注册/编辑抽屉 ────────────────────────────────────────────────────────

function NodeDrawer({
  mode,
  node,
  typeRegistry,
  categories,
  onClose,
}: {
  mode: 'create' | 'edit'
  node: NodeDescriptorView | null
  typeRegistry: Map<string, PropertyTypeView>
  categories: string[]
  onClose: () => void
}) {
  const qc = useQueryClient()
  const isEdit = mode === 'edit' && node !== null
  const [nodeType, setNodeType] = useState(node?.node_type ?? '')
  const [nodeName, setNodeName] = useState(node?.node_name ?? '')
  const [category, setCategory] = useState(node?.category ?? '')
  const [schemaText, setSchemaText] = useState(node?.schema_json ?? '{\n  "properties": {}\n}')
  const [schemaTab, setSchemaTab] = useState<'fields' | 'json'>('fields')
  const [rows, setRows] = useState<FieldRow[]>(() => schemaToRows(node?.schema_json ?? '', typeRegistry))
  const [error, setError] = useState<string | null>(null)

  const jsonValid = useMemo(() => {
    try {
      JSON.parse(schemaText)
      return true
    } catch {
      return false
    }
  }, [schemaText])

  // JSON ⇄ 字段列表：切到字段列表时从文本重建行（保留顶层未知键由保存时合并）
  const switchTab = (t: 'fields' | 'json') => {
    if (t === 'fields' && jsonValid) setRows(schemaToRows(schemaText, typeRegistry))
    setSchemaTab(t)
  }

  const save = useMutation({
    mutationFn: async () => {
      if (!nodeType.trim()) throw new Error('node_type 不能为空')
      let parsed: Record<string, unknown>
      try {
        parsed = JSON.parse(schemaText || '{}')
      } catch (e) {
        throw new Error(`schema JSON 非法: ${(e as Error).message}`)
      }
      if (schemaTab === 'fields') {
        // 字段列表为源：重建 properties/required，其余顶层键原样保留
        const properties: Record<string, unknown> = {}
        const required: string[] = []
        const seen = new Set<string>()
        for (const row of rows) {
          if (!row.key.trim()) continue
          if (seen.has(row.key.trim())) throw new Error(`字段 key 重复: ${row.key}`)
          seen.add(row.key.trim())
          const s = rowToSchema(row, typeRegistry)
          if (s) properties[row.key.trim()] = s
          if (row.required) required.push(row.key.trim())
        }
        parsed = { ...parsed, properties }
        if (required.length) parsed.required = required
        else delete parsed.required
        setSchemaText(JSON.stringify(parsed, null, 2))
      }
      return api.registerNode({
        node_type: nodeType.trim(),
        node_name: nodeName.trim(),
        category: category.trim(),
        schema_json: JSON.stringify(parsed, null, 2),
      })
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['nodes'] })
      onClose()
    },
    onError: (e: Error) => setError(e.message),
  })

  return (
    <div className="fixed inset-0 z-40 flex justify-end">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative w-[560px] max-w-[92vw] h-full bg-elevated border-l border-line-strong shadow-pop overflow-y-auto p-5 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-section text-ink-primary">{isEdit ? `编辑节点 · ${node!.node_type}` : '注册自定义节点'}</h2>
          <button onClick={onClose} className="text-ink-muted hover:text-ink-primary" aria-label="关闭">
            <X size={16} />
          </button>
        </div>

        {/* 基本信息 */}
        <div className="space-y-2">
          <label className="block">
            <span className="text-caption text-ink-muted">节点类型 <span className="text-status-error">*</span></span>
            <input
              value={nodeType}
              onChange={(e) => setNodeType(e.target.value)}
              disabled={isEdit}
              placeholder="如 lark_notify（不可与内置冲突）"
              className="input w-full mt-1 font-mono text-data-sm disabled:opacity-60"
            />
          </label>
          <div className="flex gap-2">
            <label className="flex-1">
              <span className="text-caption text-ink-muted">展示名</span>
              <input value={nodeName} onChange={(e) => setNodeName(e.target.value)} className="input w-full mt-1" placeholder="飞书通知" />
            </label>
            <label className="w-36">
              <span className="text-caption text-ink-muted">分类</span>
              <input
                value={category}
                onChange={(e) => setCategory(e.target.value)}
                list="node-category-options"
                className="input w-full mt-1"
                placeholder="通知"
              />
              <datalist id="node-category-options">
                {categories.map((c) => <option key={c} value={c} />)}
              </datalist>
            </label>
          </div>
        </div>

        {/* 来源与代码位置（只读） */}
        <div className="p-3 rounded-lg bg-inset border border-line">
          {isEdit && node!.is_builtin ? (
            <>
              <p className="text-caption text-ink-secondary">
                内置节点 · 代码位置
                <span className="ml-2 font-mono text-data-sm text-ink-primary">
                  {node!.source_module}.{node!.source_class}
                </span>
              </p>
              <p className="mt-1 text-[11px] text-ink-faint">内置节点不可修改/删除，此处仅查看其参数 schema。</p>
            </>
          ) : (
            <>
              <p className="text-caption text-ink-secondary">来源：控制台注册（仅编排表单元数据）</p>
              <p className="mt-1 text-[11px] text-ink-faint">
                该描述驱动画布节点面板与配置表单；试跑/执行需运行时存在同 node_type
                的可执行节点，否则报「未注册节点类型」。
              </p>
            </>
          )}
        </div>

        {/* 参数 schema：字段列表 ⇄ JSON 源码 */}
        <div>
          <div className="flex items-center gap-1 mb-2">
            {(
              [
                { k: 'fields', label: `字段列表（${rows.length}）` },
                { k: 'json', label: 'JSON 源码' },
              ] as const
            ).map((t) => {
              const disabled = t.k === 'fields' && !jsonValid
              return (
                <button
                  key={t.k}
                  onClick={() => !disabled && switchTab(t.k)}
                  disabled={disabled}
                  className={`px-2 py-0.5 rounded-md text-caption border transition-colors ${
                    schemaTab === t.k
                      ? 'bg-plaita-500/10 text-plaita-400 border-plaita-500/40'
                      : 'text-ink-muted border-line hover:text-ink-primary hover:bg-elevated disabled:opacity-40'
                  }`}
                >
                  {t.label}
                </button>
              )
            })}
            {!jsonValid && <span className="text-[11px] text-status-error ml-1">JSON 非法，修正后可切回字段列表</span>}
          </div>

          {schemaTab === 'json' ? (
            <textarea
              value={schemaText}
              onChange={(e) => setSchemaText(e.target.value)}
              rows={16}
              spellCheck={false}
              className={`input w-full font-mono text-data-sm ${jsonValid ? '' : 'border-status-error/60'}`}
            />
          ) : (
            <FieldListEditor
              rows={rows}
              setRows={setRows}
              typeRegistry={typeRegistry}
            />
          )}
        </div>

        {error && <p className="text-caption text-status-error">{error}</p>}

        <div className="flex gap-2 justify-end pt-2 border-t border-line">
          <Button variant="secondary" onClick={onClose}>取消</Button>
          <Button
            variant="primary"
            onClick={() => save.mutate()}
            disabled={save.isPending || !nodeType.trim() || !jsonValid}
          >
            {save.isPending ? '保存中…' : isEdit ? '保存修改' : '注册'}
          </Button>
        </div>
      </div>
    </div>
  )
}

// ── 字段列表编辑器 ───────────────────────────────────────────────────────

function FieldListEditor({
  rows,
  setRows,
  typeRegistry,
}: {
  rows: FieldRow[]
  setRows: (rows: FieldRow[]) => void
  typeRegistry: Map<string, PropertyTypeView>
}) {
  const update = (i: number, patch: Partial<FieldRow>) =>
    setRows(rows.map((r, j) => (j === i ? { ...r, ...patch } : r)))
  return (
    <div className="space-y-2">
      <p className="text-[11px] text-ink-faint">
        只列业务字段——公共基础属性（id/name/desc/output/next/timeout/容错策略）由引擎基类提供，编辑器不展示、保存时原样保留。
      </p>
      {rows.map((row, i) => (
        <div key={i} className="p-2.5 rounded-lg bg-surface border border-line space-y-2">
          <div className="flex items-center gap-1.5">
            <input
              value={row.key}
              onChange={(e) => update(i, { key: e.target.value })}
              placeholder="字段 key（如 url）"
              className="input flex-1 font-mono text-data-sm"
            />
            <input
              value={row.title}
              onChange={(e) => update(i, { title: e.target.value })}
              placeholder="展示名"
              className="input w-32"
            />
            <select
              value={row.type}
              onChange={(e) => update(i, { type: e.target.value })}
              className="input w-32 shrink-0"
            >
              <optgroup label="基础类型">
                {BASIC_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                <option value="enum">enum</option>
              </optgroup>
              {typeRegistry.size > 0 && (
                <optgroup label="自定义类型">
                  {Array.from(typeRegistry.keys()).map((name) => (
                    <option key={name} value={name}>{name}</option>
                  ))}
                </optgroup>
              )}
            </select>
            <label className="flex items-center gap-1 text-caption text-ink-muted shrink-0" title="必填">
              <input
                type="checkbox"
                checked={row.required}
                onChange={(e) => update(i, { required: e.target.checked })}
                className="accent-plaita-500 w-3.5 h-3.5"
              />
              必填
            </label>
            <button
              onClick={() => setRows(rows.filter((_, j) => j !== i))}
              className="text-ink-faint hover:text-status-error shrink-0"
              title="移除字段"
            >
              <Trash2 size={13} />
            </button>
          </div>
          <div className="flex items-center gap-1.5">
            {(row.type === 'enum' || (typeRegistry.get(row.type)?.enum_options.length ?? 0) > 0) && (
              <input
                value={row.enumText}
                onChange={(e) => update(i, { enumText: e.target.value })}
                placeholder="枚举选项，逗号分隔（覆盖类型默认）"
                className="input flex-1 text-[12px]"
              />
            )}
            {row.type === 'array' && (
              <select
                value={row.itemsType}
                onChange={(e) => update(i, { itemsType: e.target.value })}
                className="input w-28"
                title="元素类型"
              >
                {['string', 'number', 'integer', 'boolean'].map((t) => (
                  <option key={t} value={t}>items: {t}</option>
                ))}
              </select>
            )}
            <DefaultValueInput row={row} onChange={(v) => update(i, { defaultText: v })} />
          </div>
          <input
            value={row.desc}
            onChange={(e) => update(i, { desc: e.target.value })}
            placeholder="字段说明（展示在编排表单的提示里）"
            className="input w-full text-[12px]"
          />
        </div>
      ))}
      <button
        onClick={() => setRows([...rows, emptyRow()])}
        className="flex items-center gap-1 text-caption text-plaita-400 hover:text-plaita-300"
      >
        <Plus size={12} />
        添加字段
      </button>
    </div>
  )
}

/** 默认值控件按类型渲染：enum/boolean 下拉，其余文本（number 失焦校验交给保存） */
function DefaultValueInput({ row, onChange }: { row: FieldRow; onChange: (v: string) => void }) {
  if (row.type === 'boolean') {
    return (
      <select value={row.defaultText} onChange={(e) => onChange(e.target.value)} className="input w-28 shrink-0" title="默认值">
        <option value="">默认：未设置</option>
        <option value="true">默认 true</option>
        <option value="false">默认 false</option>
      </select>
    )
  }
  const enumOptions = row.type === 'enum'
    ? row.enumText.split(',').map((x) => x.trim()).filter(Boolean)
    : []
  if (enumOptions.length) {
    return (
      <select value={row.defaultText} onChange={(e) => onChange(e.target.value)} className="input w-36 shrink-0" title="默认值">
        <option value="">默认：未设置</option>
        {enumOptions.map((o) => <option key={o} value={o}>{o}</option>)}
      </select>
    )
  }
  return (
    <input
      value={row.defaultText}
      onChange={(e) => onChange(e.target.value)}
      placeholder="默认值"
      className="input w-36 shrink-0 font-mono text-data-sm"
    />
  )
}

// ── 自定义类型 Tab ───────────────────────────────────────────────────────

function TypesTab({ types }: { types: PropertyTypeView[] }) {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<UpsertPropertyTypeRequest | null>(null)

  const save = useMutation({
    mutationFn: (payload: UpsertPropertyTypeRequest) => api.upsertPropertyType(payload),
    onSuccess: () => {
      setEditing(null)
      qc.invalidateQueries({ queryKey: ['property-types'] })
    },
  })
  const del = useMutation({
    mutationFn: (name: string) => api.deletePropertyType(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['property-types'] }),
  })

  return (
    <>
      <div className="mb-3 flex items-center gap-2">
        <p className="text-caption text-ink-muted flex-1">
          自定义类型是 console 侧的命名别名（基础类型 + 枚举/默认值约束）；保存节点 schema
          时展开为内置基础类型，运行时永不接触自定义类型名。
        </p>
        <Button variant="primary" onClick={() => setEditing({ name: '', base_type: 'string' })}>
          <Plus size={13} />
          新建类型
        </Button>
      </div>

      {editing && (
        <TypeForm
          initial={editing}
          saving={save.isPending}
          error={save.error ? (save.error as Error).message : null}
          onSave={(payload) => save.mutate(payload)}
          onCancel={() => setEditing(null)}
        />
      )}

      <div className="rounded-lg border border-line overflow-hidden bg-surface">
        <Table>
          <thead>
            <tr>
              <Th>类型名</Th>
              <Th>基础类型</Th>
              <Th>枚举选项</Th>
              <Th>默认值</Th>
              <Th>说明</Th>
              <Th className="text-right">操作</Th>
            </tr>
          </thead>
          <tbody>
            {types.map((t) => (
              <Tr key={t.name}>
                <TdData className="text-ink-primary">{t.name}</TdData>
                <TdData>{t.base_type}</TdData>
                <Td>{t.enum_options.length ? t.enum_options.map(String).join(', ') : <span className="text-ink-faint">—</span>}</Td>
                <Td>{t.default_value != null ? String(t.default_value) : <span className="text-ink-faint">—</span>}</Td>
                <Td>{t.desc || <span className="text-ink-faint">—</span>}</Td>
                <Td className="text-right whitespace-nowrap">
                  <button
                    onClick={() =>
                      setEditing({
                        name: t.name,
                        base_type: t.base_type,
                        enum_options: t.enum_options,
                        default_value: t.default_value,
                        desc: t.desc,
                      })
                    }
                    className="text-caption text-plaita-400 hover:text-plaita-300 mr-3"
                  >
                    编辑
                  </button>
                  <button
                    onClick={() => {
                      if (window.confirm(`删除自定义类型 ${t.name}？已引用它的节点字段将回退为基础类型展示。`))
                        del.mutate(t.name)
                    }}
                    className="text-ink-faint hover:text-status-error"
                    title="删除"
                  >
                    <Trash2 size={13} className="inline" />
                  </button>
                </Td>
              </Tr>
            ))}
            {types.length === 0 && (
              <tr>
                <td colSpan={6} className="py-8">
                  <EmptyState message="还没有自定义类型" hint="例如注册 email（string）、订单状态（string + 枚举）供节点字段引用" />
                </td>
              </tr>
            )}
          </tbody>
        </Table>
      </div>
    </>
  )
}

function TypeForm({
  initial,
  saving,
  error,
  onSave,
  onCancel,
}: {
  initial: UpsertPropertyTypeRequest
  saving: boolean
  error: string | null
  onSave: (payload: UpsertPropertyTypeRequest) => void
  onCancel: () => void
}) {
  const [name, setName] = useState(initial.name)
  const [baseType, setBaseType] = useState(initial.base_type)
  const [enumText, setEnumText] = useState((initial.enum_options ?? []).map(String).join(', '))
  const [defaultText, setDefaultText] = useState(initial.default_value == null ? '' : String(initial.default_value))
  const [desc, setDesc] = useState(initial.desc ?? '')

  return (
    <div className="mb-4 p-4 rounded-lg bg-elevated border border-line space-y-2.5">
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-20 shrink-0">类型名</span>
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          disabled={Boolean(initial.name)}
          placeholder="如 order_status"
          className="input flex-1 font-mono text-data-sm disabled:opacity-60"
        />
        <select value={baseType} onChange={(e) => setBaseType(e.target.value)} className="input w-32">
          {BASIC_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-20 shrink-0">枚举选项</span>
        <input
          value={enumText}
          onChange={(e) => setEnumText(e.target.value)}
          placeholder="逗号分隔（可选，仅 string 基础类型有意义）"
          className="input flex-1 text-[12px]"
        />
        <input
          value={defaultText}
          onChange={(e) => setDefaultText(e.target.value)}
          placeholder="默认值（可选）"
          className="input w-40 font-mono text-data-sm"
        />
      </div>
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-20 shrink-0">说明</span>
        <input value={desc} onChange={(e) => setDesc(e.target.value)} placeholder="用途说明" className="input flex-1" />
      </div>
      {error && <p className="text-caption text-status-error">{error}</p>}
      <div className="flex gap-2 justify-end">
        <Button variant="secondary" onClick={onCancel}>取消</Button>
        <Button
          variant="primary"
          disabled={saving || !name.trim()}
          onClick={() =>
            onSave({
              name: name.trim(),
              base_type: baseType,
              enum_options: enumText.split(',').map((x) => x.trim()).filter(Boolean),
              default_value: defaultText === '' ? undefined : defaultText,
              desc,
            })
          }
        >
          {saving ? '保存中…' : '保存'}
        </Button>
      </div>
    </div>
  )
}
