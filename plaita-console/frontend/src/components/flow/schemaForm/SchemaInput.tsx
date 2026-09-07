import { useMemo, useState } from 'react'
import SchemaForm from './SchemaForm'
import { propertyToJsonSchema, type JsonSchema } from './schemaUtils'

/**
 * schema 驱动的输入组件（C4/C6 共用：试跑面板 / 启动流程弹窗）。
 * 表单 ⇄ JSON 双 tab，**JSON 文本是唯一事实源**：表单改动即时序列化回文本；
 * 文本非法时锁表单 tab 并提示。schema 缺失/无 properties 时只有 JSON tab。
 */
export default function SchemaInput({
  inputType,
  text,
  onTextChange,
  rows = 6,
}: {
  /** 流程 inputType（引擎 Property 结构），可空 */
  inputType: unknown
  /** 输入 JSON 文本（事实源） */
  text: string
  onTextChange: (t: string) => void
  rows?: number
}) {
  const schema: JsonSchema | null = useMemo(() => propertyToJsonSchema(inputType), [inputType])
  const hasForm = Boolean(schema?.properties && Object.keys(schema.properties).length)
  const [tab, setTab] = useState<'form' | 'json'>(hasForm ? 'form' : 'json')
  const parsed = useMemo(() => {
    try {
      const v = text.trim() ? JSON.parse(text) : {}
      if (v && typeof v === 'object' && !Array.isArray(v)) return v as Record<string, unknown>
      return null
    } catch {
      return null
    }
  }, [text])
  const jsonInvalid = parsed === null

  return (
    <div>
      {hasForm && (
        <div className="flex gap-1 mb-1.5">
          {(
            [
              { k: 'form', label: '表单' },
              { k: 'json', label: 'JSON' },
            ] as const
          ).map((t) => (
            <button
              key={t.k}
              onClick={() => setTab(t.k)}
              className={`px-2 py-0.5 rounded-md text-caption transition-colors border ${
                tab === t.k
                  ? 'bg-plaita-500/10 text-plaita-400 border-plaita-500/40'
                  : 'text-ink-muted border-line hover:text-ink-primary hover:bg-elevated'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      )}
      {(!hasForm || tab === 'json') && (
        <textarea
          value={text}
          onChange={(e) => onTextChange(e.target.value)}
          rows={rows}
          spellCheck={false}
          className={`input w-full font-mono text-data-sm resize-y ${jsonInvalid ? 'border-status-error/60' : ''}`}
          placeholder="{}"
        />
      )}
      {hasForm && tab === 'form' && (
        <>
          {jsonInvalid ? (
            <p className="text-caption text-status-error">
              JSON 非法，无法按表单编辑——切到 JSON 修正后再回来
            </p>
          ) : (
            <SchemaForm
              fields={parsed!}
              schema={schema}
              includeCommonKeys
              onChange={(next) => onTextChange(JSON.stringify(next, null, 2))}
            />
          )}
        </>
      )}
      {!hasForm && (
        <p className="mt-1 text-[11px] text-ink-faint">
          流程未定义入参 schema（可在流程设置的「入参类型」中定义），请直接编辑 JSON
        </p>
      )}
    </div>
  )
}
