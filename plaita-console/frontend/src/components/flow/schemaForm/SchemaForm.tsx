import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { cn } from '../../ui/cn'
import {
  buildFormPlan,
  type FieldSpec,
  type JsonSchema,
} from './schemaUtils'
import ExpressionInput, { type VarGroup } from './ExpressionInput'
import CodeEditor, { inferLanguage, isCodeField } from './CodeEditor'

/**
 * schema 驱动的类型特定字段表单（DESIGN.md token）。
 * - 核心字段（required + 类型白名单）平铺首屏，次要标量落「更多参数」折叠区
 * - 复杂结构（自由 dict、array of object）与 schema 未覆盖字段统一落
 *   「高级字段」JSON 兜底区，能力不锁死
 * - onChange 即时写回（与画布交互一致）
 * - schema 为 null 时返回 null，由调用方退化为整段 JSON 编辑
 */
export default function SchemaForm({
  fields,
  schema,
  onChange,
  excludeKeys,
  coreFields,
  variableGroups,
}: {
  fields: Record<string, unknown>
  schema: JsonSchema | null
  onChange: (next: Record<string, unknown>) => void
  /** 由调用方专门 UI 接管的键（如 child_flow / branches），不进表单与兜底区 */
  excludeKeys?: Set<string>
  /** 该类型的核心字段白名单（与 required 一起决定首屏平铺） */
  coreFields?: Set<string>
  /** 变量目录（$INPUT/$NODE/$GLOBAL），供表达式输入插入 */
  variableGroups?: VarGroup[]
}) {
  if (!schema) return null
  const plan = buildFormPlan(fields, schema, excludeKeys, coreFields)
  const setValue = (key: string, v: unknown) => onChange({ ...fields, [key]: v })
  const removeValue = (key: string) => {
    const next = { ...fields }
    delete next[key]
    onChange(next)
  }
  const ctrl = (f: FieldSpec) => (
    <FieldControl
      key={f.key}
      spec={f}
      value={fields[f.key]}
      siblingFields={fields}
      variableGroups={variableGroups}
      onChange={(v) => (v === undefined ? removeValue(f.key) : setValue(f.key, v))}
    />
  )

  return (
    <div className="space-y-3">
      {plan.core.map(ctrl)}
      {plan.more.length > 0 && (
        <CollapsibleSection title={`更多参数（${plan.more.length}）`}>
          {plan.more.map(ctrl)}
        </CollapsibleSection>
      )}
      {plan.advanced.length > 0 && (
        <CollapsibleSection title={`高级字段（${plan.advanced.length}）`} defaultOpen={false}>
          {plan.advanced.map((k) => (
            <div key={k}>
              <label className="block text-caption text-ink-muted mb-1">
                <span className="font-mono text-[10px] text-ink-faint">{k}</span>
              </label>
              <JsonField value={fields[k]} onChange={(v) => setValue(k, v)} />
            </div>
          ))}
        </CollapsibleSection>
      )}
    </div>
  )
}

/** 折叠区块（更多参数 / 高级字段共用） */
function CollapsibleSection({
  title,
  children,
  defaultOpen = false,
}: {
  title: string
  children: React.ReactNode
  defaultOpen?: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="border-t border-line pt-2.5">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1 text-caption text-ink-muted hover:text-ink-primary"
      >
        {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        {title}
      </button>
      {open && <div className="mt-2.5 space-y-3">{children}</div>}
    </div>
  )
}

// ── 单字段控件 ───────────────────────────────────────────────────────────

function FieldControl({
  spec,
  value,
  onChange,
  nested = false,
  siblingFields,
  variableGroups,
}: {
  spec: FieldSpec
  value: unknown
  onChange: (v: unknown) => void
  nested?: boolean
  /** 同级字段全集（用于推断代码语言等） */
  siblingFields?: Record<string, unknown>
  variableGroups?: VarGroup[]
}) {
  const label = (
    <label className={cn('block text-caption text-ink-muted mb-1', nested && 'text-[11px]')}>
      {spec.label}
      {spec.required && <span className="text-status-error ml-0.5">*</span>}
      {spec.key !== spec.label && !nested && (
        <span className="ml-1.5 font-mono text-[10px] text-ink-faint">{spec.key}</span>
      )}
    </label>
  )
  const hint = spec.desc ? (
    <p className="mt-1 text-[11px] leading-4 text-ink-faint line-clamp-2">{spec.desc}</p>
  ) : null

  let control: React.ReactNode = null
  switch (spec.kind) {
    case 'string':
      // 存量 flow 可能在 string 型字段上存了结构化值（字面量数组/对象）：
      // 退化为 JSON 编辑，避免 String(value) 渲染成 [object Object]
      if (value != null && typeof value !== 'string') {
        control = <JsonField value={value} onChange={onChange} />
        break
      }
      if (!nested && isCodeField(spec.key)) {
        control = (
          <CodeEditor
            value={typeof value === 'string' ? value : ''}
            language={inferLanguage(siblingFields ?? {})}
            onChange={(v) => onChange(v === '' ? undefined : v)}
          />
        )
      } else if (!nested) {
        control = (
          <ExpressionInput
            value={typeof value === 'string' ? value : value == null ? '' : String(value)}
            groups={variableGroups ?? []}
            placeholder={spec.schema.default != null ? String(spec.schema.default) : undefined}
            onChange={(v) => onChange(v === '' ? undefined : v)}
          />
        )
      } else {
        control = (
          <input
            value={typeof value === 'string' ? value : value == null ? '' : String(value)}
            onChange={(e) => onChange(e.target.value === '' ? undefined : e.target.value)}
            placeholder={spec.schema.default != null ? String(spec.schema.default) : undefined}
            className="input w-full"
          />
        )
      }
      break
    case 'number':
      control = (
        <input
          type="number"
          value={typeof value === 'number' ? value : value == null ? '' : String(value)}
          onChange={(e) =>
            onChange(e.target.value === '' ? undefined : Number(e.target.value))
          }
          className="input w-full font-mono"
        />
      )
      break
    case 'boolean':
      control = (
        <label className="flex items-center gap-2 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={value === true}
            onChange={(e) => onChange(e.target.checked ? true : undefined)}
            className="accent-plaita-500 w-3.5 h-3.5"
          />
          <span className="text-caption text-ink-secondary">{value === true ? '开启' : '关闭'}</span>
        </label>
      )
      break
    case 'enum':
      control = (
        <select
          value={value == null ? '' : String(value)}
          onChange={(e) => onChange(e.target.value === '' ? undefined : e.target.value)}
          className="input w-full"
        >
          <option value="">（未设置）</option>
          {spec.schema.enum?.map((opt) => (
            <option key={String(opt)} value={String(opt)}>
              {String(opt)}
            </option>
          ))}
        </select>
      )
      break
    case 'array':
      control = (
        <ScalarArray
          value={Array.isArray(value) ? (value as unknown[]) : []}
          itemKind={(spec.schema.items && (Array.isArray(spec.schema.items.type) ? spec.schema.items.type[0] : spec.schema.items.type)) === 'number' ? 'number' : 'string'}
          onChange={onChange}
        />
      )
      break
    case 'object': {
      // 一层嵌套平铺：子字段递归为控件
      const childFields = (value ?? {}) as Record<string, unknown>
      const childSchema: JsonSchema = spec.schema
      const required = childSchema.required || []
      control = (
        <div className="border-l border-line pl-3 space-y-3">
          {Object.entries(childSchema.properties || {}).map(([k, raw]) => {
            const childSpec: FieldSpec = {
              key: k,
              label: (raw.title as string) || k,
              desc: raw.description,
              schema: raw,
              kind: raw.enum?.length
                ? 'enum'
                : raw.properties && 'data_type' in raw.properties
                  ? 'property'
                  : (Array.isArray(raw.type) ? raw.type.find((x) => x !== 'null') : raw.type) === 'object'
                    ? 'json'
                    : raw.type === 'array' && (!raw.items || (raw.items.type !== 'string' && raw.items.type !== 'number'))
                      ? 'json'
                      : (Array.isArray(raw.type) ? raw.type[0] : raw.type) === 'array'
                        ? 'array'
                        : ((Array.isArray(raw.type) ? raw.type.find((x) => x !== 'null') : raw.type) as FieldSpec['kind']) || 'json',
              required: required.includes(k),
              core: required.includes(k),
            }
            return (
              <div key={k}>
                <FieldControl
                  spec={childSpec}
                  value={childFields[k]}
                  nested
                  onChange={(v) => {
                    const next = { ...childFields }
                    if (v === undefined) delete next[k]
                    else next[k] = v
                    onChange(next)
                  }}
                />
              </div>
            )
          })}
        </div>
      )
      break
    }
    case 'property':
      control = <PropertyField value={value} onChange={onChange} />
      break
    case 'json':
    default:
      control = <JsonField value={value} onChange={onChange} />
  }

  return (
    <div>
      {label}
      {control}
      {hint}
    </div>
  )
}

// ── 标量数组：逐项编辑 + 增删 ────────────────────────────────────────────

function ScalarArray({
  value,
  itemKind,
  onChange,
}: {
  value: unknown[]
  itemKind: 'string' | 'number'
  onChange: (v: unknown) => void
}) {
  const setItem = (i: number, v: unknown) => {
    const next = [...value]
    next[i] = v
    onChange(next)
  }
  return (
    <div className="space-y-1.5">
      {value.map((item, i) => (
        <div key={i} className="flex items-center gap-1.5">
          <input
            value={item == null ? '' : String(item)}
            onChange={(e) =>
              setItem(i, itemKind === 'number' ? Number(e.target.value) : e.target.value)
            }
            className="input w-full font-mono text-[12px]"
          />
          <button
            onClick={() => onChange(value.filter((_, j) => j !== i))}
            className="text-ink-faint hover:text-status-error text-xs px-1 shrink-0"
            title="移除"
          >
            ✕
          </button>
        </div>
      ))}
      <button
        onClick={() => onChange([...value, itemKind === 'number' ? 0 : ''])}
        className="text-caption text-plaita-400 hover:text-plaita-300"
      >
        + 添加一项
      </button>
    </div>
  )
}

// ── Property 数据槽：dataType 下拉 + 其余结构 JSON ───────────────────────

const DATA_TYPES = [
  'string', 'integer', 'number', 'boolean', 'object', 'array', 'map', 'datetime', 'any',
]

function PropertyField({
  value,
  onChange,
}: {
  value: unknown
  onChange: (v: unknown) => void
}) {
  const obj = (typeof value === 'object' && value !== null ? value : {}) as Record<string, unknown>
  const rest = { ...obj }
  delete rest.data_type
  const hasRest = Object.keys(rest).length > 0
  return (
    <div className="space-y-1.5">
      <select
        value={typeof obj.data_type === 'string' ? obj.data_type : ''}
        onChange={(e) =>
          onChange(e.target.value === '' ? undefined : { ...obj, data_type: e.target.value })
        }
        className="input w-full"
      >
        <option value="">（未设置）</option>
        {DATA_TYPES.map((t) => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>
      {hasRest && (
        <JsonField value={rest} onChange={(v) => onChange(v === undefined ? undefined : { ...(v as Record<string, unknown>), data_type: obj.data_type })} compact />
      )}
    </div>
  )
}

// ── JSON 编辑（失焦提交，错误提示）───────────────────────────────────────

export function JsonField({
  value,
  onChange,
  compact = false,
}: {
  value: unknown
  onChange: (v: unknown) => void
  compact?: boolean
}) {
  const [text, setText] = useState(() => JSON.stringify(value ?? null, null, 2))
  const [error, setError] = useState<string | null>(null)
  const commit = () => {
    try {
      const parsed = text.trim() === '' ? undefined : JSON.parse(text)
      setError(null)
      onChange(parsed)
    } catch (e) {
      setError((e as Error).message)
    }
  }
  return (
    <div>
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        onBlur={commit}
        rows={compact ? 4 : 6}
        spellCheck={false}
        className={cn(
          'input w-full font-mono text-[11px] leading-4 resize-y',
          error && 'border-status-error/60'
        )}
      />
      {error && <p className="mt-1 text-[11px] text-status-error">{error}</p>}
    </div>
  )
}

// ── 高级字段折叠区已由 CollapsibleSection + plan.advanced 取代 ───────────
