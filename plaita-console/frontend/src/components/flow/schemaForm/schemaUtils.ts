/**
 * JSON Schema → 表单字段计划的工具层。
 *
 * 数据源：GET /api/nodes 下发的节点类型 schema（pydantic model_json_schema 生成）。
 * 键名约定：schema 属性为 snake_case（引擎 model_dump 的权威键名）；存量 flow 实例
 * 可能带 camelCase 输入别名（childFlow/itemType/async 等，引擎 validator 兼容接受），
 * 表单读写前统一归一到 schema 键，保证保存后的 definition 键名规范一致。
 */

export interface JsonSchema {
  type?: string | string[]
  title?: string
  description?: string
  properties?: Record<string, JsonSchema>
  required?: string[]
  items?: JsonSchema
  enum?: unknown[]
  default?: unknown
  $ref?: string
  allOf?: JsonSchema[]
  anyOf?: JsonSchema[]
  $defs?: Record<string, JsonSchema>
  [k: string]: unknown
}

export type FieldKind =
  | 'string'
  | 'number'
  | 'boolean'
  | 'enum'
  | 'array'
  | 'object'
  | 'property'
  | 'json'

export interface FieldSpec {
  key: string
  label: string
  desc?: string
  schema: JsonSchema
  kind: FieldKind
  required: boolean
}

/** 通用字段（抽屉单独渲染）与由画布边推导的连接字段，不进类型表单。
 *  timeout_handler/error_handler/desc 是 Node 基类共有字段，由抽屉的固定表单接管。 */
export const COMMON_KEYS = new Set(['output', 'timeout', 'desc', 'timeout_handler', 'error_handler'])
export const CONNECT_KEYS = new Set(['type', 'id', 'name', 'desc', 'next', 'else_next'])
/** 纯内部字段：引擎簿记用，不展示 */
const INTERNAL_KEYS = new Set(['source_line'])

/** 已知输入别名 → schema 权威键（引擎 validator 兼容的历史写法） */
const LEGACY_ALIASES: Record<string, string> = {
  childFlow: 'child_flow',
  itemType: 'item_type',
  typeDefs: 'item_type',
  async: 'concurrent',
  maxConcurrent: 'max_concurrent',
  elseNext: 'else_next',
  flowID: 'flow_id',
  flowVersion: 'flow_version',
}

/**
 * 把实例 fields 的别名键归一到 schema 权威键。
 * - LEGACY_ALIASES 是引擎既定的输入别名映射，无条件归一（store 层无 schema 时也安全）
 * - 通用 camel → snake 仅在 schema 提供且确认存在该键时迁移，不盲改未知字段
 * 返回新对象，不修改原值。
 */
export function normalizeFieldKeys(
  fields: Record<string, unknown>,
  schema?: JsonSchema | null,
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...fields }
  for (const [legacy, canonical] of Object.entries(LEGACY_ALIASES)) {
    if (legacy in out) {
      if (!(canonical in out)) out[canonical] = out[legacy]
      delete out[legacy]
    }
  }
  if (!schema?.properties) return out
  const canon = new Set(Object.keys(schema.properties))
  for (const k of Object.keys(out)) {
    const snake = k.replace(/([A-Z])/g, (c) => `_${c.toLowerCase()}`)
    if (snake !== k && canon.has(snake) && !(snake in out)) {
      out[snake] = out[k]
      delete out[k]
    }
  }
  return out
}

/** 解引用：$ref → $defs、allOf 合并、anyOf 取首个非 null/非空分支；递归 properties/items */
export function derefSchema(root: JsonSchema, s: JsonSchema, depth = 0): JsonSchema {
  if (!s || typeof s !== 'object' || depth > 8) return s ?? {}
  let out = s
  if (s.$ref) {
    const path = s.$ref.replace(/^#\//, '').split('/')
    let cur: unknown = root
    for (const seg of path) cur = (cur as Record<string, unknown>)?.[seg]
    out = derefSchema(root, cur as JsonSchema, depth + 1)
  }
  if (out.allOf?.length) {
    const merged: JsonSchema = { ...out }
    delete merged.allOf
    for (const part of out.allOf) {
      const d = derefSchema(root, part, depth + 1)
      merged.properties = { ...d.properties, ...merged.properties }
      if (d.required?.length)
        merged.required = [...(merged.required || []), ...d.required]
      if (d.type) merged.type = d.type
      if (d.enum) merged.enum = d.enum
      if (d.items) merged.items = d.items
      if (d.description) merged.description = merged.description || d.description
    }
    out = merged
  }
  if (out.anyOf?.length) {
    // 取首个「非 null 且非空对象」的分支作为主型；全空则视为任意 JSON
    const main = out.anyOf.find((p) => {
      if (!p || typeof p !== 'object') return false
      const t = Array.isArray(p.type) ? p.type[0] : p.type
      if (t === 'null') return false
      if (p.$ref) return true
      return Boolean(t || p.properties || p.enum || p.allOf)
    })
    if (main) {
      const d = derefSchema(root, main, depth + 1)
      const rest = { ...out }
      delete rest.anyOf
      out = { ...rest, ...d }
    }
  }
  if (out.properties) {
    const props: Record<string, JsonSchema> = {}
    for (const [k, v] of Object.entries(out.properties))
      props[k] = derefSchema(root, v as JsonSchema, depth + 1)
    out = { ...out, properties: props }
  }
  if (out.items) out = { ...out, items: derefSchema(root, out.items, depth + 1) }
  return out
}

/** 判定字段控件类型 */
export function fieldKind(s: JsonSchema): FieldKind {
  if (s.enum?.length) return 'enum'
  // Property 数据槽结构（data_type + children/item_type）走专用编辑
  if (s.properties && 'data_type' in s.properties) return 'property'
  const t = Array.isArray(s.type) ? s.type.find((x) => x !== 'null') : s.type
  if (t === 'boolean') return 'boolean'
  if (t === 'integer' || t === 'number') return 'number'
  if (t === 'string') return 'string'
  if (t === 'array') {
    const it = s.items
    const ik = it ? (Array.isArray(it.type) ? it.type[0] : it.type) : undefined
    return ik === 'string' || ik === 'integer' || ik === 'number' ? 'array' : 'json'
  }
  if (t === 'object') {
    return s.properties && Object.keys(s.properties).length > 0 ? 'object' : 'json'
  }
  return 'json'
}

/**
 * 生成表单计划：
 * - fields：schema 覆盖、可表单化的字段（保持 schema 声明顺序）
 * - advanced：实例中存在但 schema 未覆盖的键（进「高级字段」JSON 兜底）
 * - excludeKeys：由调用方专门 UI 接管的键（如 child_flow 子图编辑、parallel 分支列表）
 */
export function buildFormPlan(
  fields: Record<string, unknown>,
  schema: JsonSchema | null,
  excludeKeys?: Set<string>,
): { fields: FieldSpec[]; advanced: string[] } {
  if (!schema?.properties) return { fields: [], advanced: Object.keys(fields) }
  const specs: FieldSpec[] = []
  const consumed = new Set<string>([
    ...COMMON_KEYS,
    ...CONNECT_KEYS,
    ...INTERNAL_KEYS,
    ...(excludeKeys ?? []),
  ])
  for (const [schemaKey, rawProp] of Object.entries(schema.properties)) {
    if (consumed.has(schemaKey)) continue
    const prop = derefSchema(schema, rawProp)
    specs.push({
      key: schemaKey,
      label: (prop.title as string) || schemaKey,
      desc: prop.description,
      schema: prop,
      kind: fieldKind(prop),
      required: schema.required?.includes(schemaKey) ?? false,
    })
  }
  const advanced = Object.keys(fields).filter(
    (k) => !consumed.has(k) && !specs.some((f) => f.key === k)
  )
  return { fields: specs, advanced }
}
