import { useState } from 'react'
import { ListPlus, Rows3, Trash2 } from 'lucide-react'
import ExpressionInput, { type VarGroup } from './ExpressionInput'

// schema enum 缺失时的兜底算子表（与引擎 plaita/node/decide.py condition_matcher
// 同步；引擎 Literal 化后正常路径走 conditionOperators() 的 schema 派生）
const FALLBACK_OPS = [
  'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'notIn', 'contains', 'notContains',
]

interface ConditionRow {
  field?: unknown
  operator?: string
  value?: unknown
}

function isGroupValue(v: unknown): v is { relation: string; conditions: unknown[] } {
  return typeof v === 'object' && v !== null && 'relation' in v && 'conditions' in v
}

function isConditionValue(v: unknown): v is ConditionRow {
  return typeof v === 'object' && v !== null && !Array.isArray(v) && ('field' in v || 'operator' in v)
}

/** 引擎 evaluate 兼容的值存储：可 JSON 解析的输入转类型化值，其余按字符串（含 $ 表达式） */
function parseValueInput(text: string): unknown {
  const t = text.trim()
  if (t === '') return null
  try {
    return JSON.parse(t)
  } catch {
    return text
  }
}

function valueToText(v: unknown): string {
  if (v === null || v === undefined) return ''
  return typeof v === 'string' ? v : JSON.stringify(v)
}

/**
 * 条件三段式构造器（2026-09 表单评审 B4）：
 * 字段表达式 + 算子下拉 + 值，支持 AND/OR 条件组——替代裸 JSON 手写
 * {"field":..., "operator":..., "value":...}。整体替换写回（不是逐键合并），
 * 顺带修掉存量组条件被嵌套表单改出混合对象的污染问题。
 */
export default function ConditionEditor({
  value,
  onChange,
  operatorEnum,
  variableGroups,
}: {
  value: unknown
  onChange: (v: unknown) => void
  operatorEnum?: string[]
  variableGroups?: VarGroup[]
}) {
  const ops = operatorEnum?.length ? operatorEnum : FALLBACK_OPS

  if (isGroupValue(value)) {
    // 组内行按宽松形状处理（行编辑器对缺键有默认值兜底）
    const rows = (Array.isArray(value.conditions) ? value.conditions : []) as ConditionRow[]
    const setRows = (next: unknown[]) => onChange({ ...value, conditions: next })
    return (
      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5 text-caption text-ink-muted">
          <span>满足以下</span>
          <select
            value={value.relation === 'or' ? 'or' : 'and'}
            onChange={(e) => onChange({ ...value, relation: e.target.value })}
            className="input w-auto py-0.5"
          >
            <option value="and">全部（AND）</option>
            <option value="or">任一（OR）</option>
          </select>
          <span>条件：</span>
        </div>
        {rows.map((c, i) => (
          <div key={i} className="flex items-start gap-1.5">
            <ConditionRowEditor row={c} ops={ops} variableGroups={variableGroups} onChange={(n) => setRows(rows.map((x, j) => (j === i ? n : x)))} />
            <button
              onClick={() => setRows(rows.filter((_, j) => j !== i))}
              className="mt-1.5 text-ink-faint hover:text-status-error shrink-0"
              title="移除条件"
            >
              <Trash2 size={13} />
            </button>
          </div>
        ))}
        <div className="flex gap-3">
          <button
            onClick={() => setRows([...rows, { field: '', operator: ops[0], value: null }])}
            className="flex items-center gap-1 text-caption text-plaita-400 hover:text-plaita-300"
          >
            <ListPlus size={12} />
            添加条件
          </button>
          {rows.length === 1 && (
            <button
              onClick={() => onChange(rows[0])}
              className="flex items-center gap-1 text-caption text-ink-muted hover:text-ink-primary"
              title="仅剩一条条件时可拆出为简单条件"
            >
              <Rows3 size={12} />
              合并为单条件
            </button>
          )}
        </div>
      </div>
    )
  }

  const unrecognized = value != null && !isConditionValue(value)
  const row: ConditionRow = isConditionValue(value) ? value : { field: '', operator: ops[0], value: null }
  return (
    <div className="space-y-1.5">
      <ConditionRowEditor row={row} ops={ops} variableGroups={variableGroups} onChange={onChange} />
      <div>
        <button
          onClick={() => onChange({ relation: 'and', conditions: [row] })}
          className="flex items-center gap-1 text-caption text-plaita-400 hover:text-plaita-300"
          title="多个条件用 AND/OR 组合"
        >
          <ListPlus size={12} />
          组合条件（AND/OR）
        </button>
        {unrecognized && (
          <p className="mt-1 text-[11px] text-status-warning">原值不是可识别的条件结构，编辑后将被替换</p>
        )}
      </div>
    </div>
  )
}

/** 单行：字段表达式 + 算子下拉 + 值 */
function ConditionRowEditor({
  row,
  ops,
  variableGroups,
  onChange,
}: {
  row: ConditionRow
  ops: string[]
  variableGroups?: VarGroup[]
  onChange: (v: unknown) => void
}) {
  // 值输入框保留本地文本态：数字/浮点的中间输入（如 "1."）不被即时解析打断
  const [valueText, setValueText] = useState(() => valueToText(row.value))
  const setField = (expr: string) => onChange({ ...row, field: expr })
  const setValue = (text: string) => {
    setValueText(text)
    onChange({ ...row, value: parseValueInput(text) })
  }
  return (
    <div className="flex-1 flex items-center gap-1.5 min-w-0">
      <div className="flex-1 min-w-0">
        <ExpressionInput
          value={typeof row.field === 'string' ? row.field : row.field == null ? '' : JSON.stringify(row.field)}
          onChange={setField}
          groups={variableGroups ?? []}
          placeholder="字段，如 $INPUT.age"
        />
      </div>
      <select
        value={typeof row.operator === 'string' && row.operator ? row.operator : ops[0]}
        onChange={(e) => onChange({ ...row, operator: e.target.value })}
        className="input w-28 shrink-0"
        title="比较算子"
      >
        {!ops.includes(String(row.operator)) && row.operator ? (
          <option value={String(row.operator)}>{String(row.operator)}（无效）</option>
        ) : null}
        {ops.map((op) => (
          <option key={op} value={op}>{op}</option>
        ))}
      </select>
      <div className="w-32 shrink-0">
        <input
          value={valueText}
          onChange={(e) => setValue(e.target.value)}
          placeholder="值（支持 $ 表达式）"
          className="input w-full"
        />
      </div>
    </div>
  )
}
