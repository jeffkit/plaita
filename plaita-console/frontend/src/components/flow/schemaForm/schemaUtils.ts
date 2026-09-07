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
  | 'kv'
  | 'json'

export interface FieldSpec {
  key: string
  label: string
  desc?: string
  schema: JsonSchema
  kind: FieldKind
  required: boolean
  /** 核心/次要分组：required 或类型白名单命中为 true */
  core: boolean
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

/** anyOf 的全部非 null 分支（$ref 已解引用），供 Union 字段的控件选型参考 */
function nonNullVariants(root: JsonSchema, s: JsonSchema, depth = 0): JsonSchema[] {
  if (!s.anyOf?.length || depth > 4) return []
  return s.anyOf
    .filter((p): p is JsonSchema => {
      if (!p || typeof p !== 'object') return false
      const t = Array.isArray(p.type) ? p.type[0] : p.type
      return t !== 'null'
    })
    .map((p) => derefSchema(root, p, depth + 1))
}

/** 键值对结构字段（headers/query/validate_* 等 Dict[str,str] 语义）：KV 表格编辑而非裸 JSON */
export const KV_KEYS = new Set(['headers', 'query', 'validate_headers', 'validate_params'])

/**
 * 生成表单计划：
 * - core / more：schema 覆盖、可表单化的字段，按「required + 类型白名单」分核心与次要
 * - advanced：实例中存在但 schema 未覆盖的键 + schema 判定为 JSON 兜底的复杂字段
 *   （带 title/desc 元数据，折叠区内不再只渲染裸键名）
 * - excludeKeys：由调用方专门 UI 接管的键（如 child_flow 子图编辑、parallel 分支列表）
 */
export function buildFormPlan(
  fields: Record<string, unknown>,
  schema: JsonSchema | null,
  excludeKeys?: Set<string>,
  coreKeys?: Set<string>,
  opts?: { includeCommonKeys?: boolean }
): {
  core: FieldSpec[]
  more: FieldSpec[]
  advanced: Array<{ key: string; title?: string; desc?: string }>
} {
  if (!schema?.properties)
    return { core: [], more: [], advanced: Object.keys(fields).map((k) => ({ key: k })) }
  const specs: FieldSpec[] = []
  const consumed = new Set<string>([
    // COMMON/CONNECT/INTERNAL 是「节点编辑」语境的排除（Node 基类字段由抽屉
    // 固定表单/画布连线接管）。流程入参 schema（SchemaInput）复用本函数时，
    // 入参字段可与基类字段同名（如 name/id），必须跳过这三张清单——否则同名字段
    // 被静默吞掉（2026-09 验收实测：inputType.name 消失）
    ...(opts?.includeCommonKeys ? [] : [...COMMON_KEYS, ...CONNECT_KEYS, ...INTERNAL_KEYS]),
    ...(excludeKeys ?? []),
  ])
  for (const [schemaKey, rawProp] of Object.entries(schema.properties)) {
    if (consumed.has(schemaKey)) continue
    const prop = derefSchema(schema, rawProp)
    let kind = fieldKind(prop)
    // B2 变体感知：含 string 分支的数值 Union 按 string 渲染——表达式是引擎
    // 的万能入口（如 delay_seconds "$INPUT.x"），number 控件会在输入 $ 时把值
    // 敲成空串并删掉必填键
    if (kind === 'number' && nonNullVariants(schema, rawProp).some((v) => fieldKind(v) === 'string')) {
      kind = 'string'
    }
    if (kind === 'json') {
      // B5：键值对语义字段升格为 KV 表格控件（http.headers/query 因此进首屏，
      // validate_headers/params 进「更多参数」），不再落高级 JSON 兜底区
      if (KV_KEYS.has(schemaKey)) {
        specs.push({
          key: schemaKey,
          label: (prop.title as string) || schemaKey,
          desc: prop.description,
          schema: prop,
          kind: 'kv',
          required: schema.required?.includes(schemaKey) ?? false,
          core:
            (schema.required?.includes(schemaKey) ?? false) ||
            (coreKeys?.has(schemaKey) ?? false),
        })
      }
      continue
    }
    specs.push({
      key: schemaKey,
      label: (prop.title as string) || schemaKey,
      desc: prop.description,
      schema: prop,
      kind,
      required: schema.required?.includes(schemaKey) ?? false,
      core:
        (schema.required?.includes(schemaKey) ?? false) || (coreKeys?.has(schemaKey) ?? false),
    })
  }
  const covered = new Set(specs.map((f) => f.key))
  const advanced: Array<{ key: string; title?: string; desc?: string }> = Object.keys(
    fields
  )
    .filter((k) => !consumed.has(k) && !covered.has(k))
    .map((k) => ({ key: k }))
  for (const [k, raw] of Object.entries(schema.properties)) {
    if (consumed.has(k) || covered.has(k)) continue
    const prop = derefSchema(schema, raw)
    if (fieldKind(prop) !== 'json') continue
    advanced.push({
      key: k,
      title: prop.title as string | undefined,
      desc: prop.description,
    })
  }
  return {
    core: specs.filter((f) => f.core),
    more: specs.filter((f) => !f.core),
    advanced,
  }
}

/**
 * 从节点 schema 派生条件算子清单（condition → $ref Condition → operator.enum）。
 * 引擎 Literal 化（2026-09 A1）后各条件类节点天然携带；取不到时由
 * ConditionEditor 的兜底表接手。
 */
export function conditionOperators(nodeSchema: JsonSchema): string[] | undefined {
  const cond = nodeSchema.properties?.condition
  if (!cond) return undefined
  const d = derefSchema(nodeSchema, cond)
  const ops = d.properties?.operator?.enum
  return Array.isArray(ops) && ops.length ? ops.map(String) : undefined
}

/**
 * 单字段 schema 校验（2026-09 表单评审 B5 后半）。覆盖当前 schema 方言实际
 * 使用到的约束（required / type / enum）——手写实现而非引入 ajv：23 个内置
 * schema 的约束词汇仅此三种，引入完整 JSON Schema 校验器是零收益体积
 * （同 B3 的第二数据源教训）；方言扩展时再评估。
 */
export function fieldError(spec: FieldSpec, value: unknown): string | null {
  const empty = value === undefined || value === null || value === ''
  if (spec.required && empty) return '必填项不能为空'
  if (empty) return null
  switch (spec.kind) {
    case 'number':
      return typeof value === 'number' && Number.isFinite(value) ? null : '需为数字'
    case 'boolean':
      return typeof value === 'boolean' ? null : '需为 true / false'
    case 'enum':
      return spec.schema.enum?.map(String).includes(String(value))
        ? null
        : `需为：${spec.schema.enum?.map(String).join(' / ')}`
    case 'array':
      return Array.isArray(value) ? null : '需为数组'
    case 'kv':
      if (typeof value !== 'object' || value === null || Array.isArray(value)) return '需为键值对对象'
      return Object.values(value).every((v) => typeof v === 'string') ? null : '值需为字符串'
    default:
      return null
  }
}

/** 节点级校验：返回未通过的字段清单（供抽屉即时提示与保存前流程级检查共用） */
export function validateNodeFields(
  fields: Record<string, unknown>,
  schema: JsonSchema | null,
  excludeKeys?: Set<string>,
  coreKeys?: Set<string>,
): Array<{ key: string; message: string }> {
  if (!schema?.properties) return []
  const plan = buildFormPlan(fields, schema, excludeKeys, coreKeys)
  const out: Array<{ key: string; message: string }> = []
  for (const spec of [...plan.core, ...plan.more]) {
    const err = fieldError(spec, fields[spec.key])
    if (err) out.push({ key: spec.key, message: err })
  }
  return out
}

/**
 * 流程 inputType（引擎 Property 结构）→ JSON Schema，供试跑/启动的表单化输入
 * （C4）。Property 兼容 camelCase 别名（dataType/item_type 等，引擎 alias 收敛）。
 * 顶层仅支持 object 入参（流程入参的常规形态）；children 兼容 dict 与 list 两种
 * 存储形态（Property.children 为 Union）。约束方言映射不到的（如 any）交由
 * SchemaForm 的 JSON 兜底，能力不锁死。
 */
export function propertyToJsonSchema(p: unknown): JsonSchema | null {
  if (!p || typeof p !== 'object' || Array.isArray(p)) return null
  const prop = p as Record<string, unknown>
  const type = String(prop.data_type ?? prop.dataType ?? '').toLowerCase()
  if (type && type !== 'object') return null
  const rawChildren = (prop.children ?? prop.properties) as unknown
  const entries: Array<[string, Record<string, unknown>]> = Array.isArray(rawChildren)
    ? (rawChildren as Array<Record<string, unknown>>)
        .filter((c) => c && typeof c === 'object')
        .map((c) => [String(c.name ?? 'field'), c])
    : rawChildren && typeof rawChildren === 'object'
      ? Object.entries(rawChildren as Record<string, unknown>).map(([k, v]) => [
          k,
          (v ?? {}) as Record<string, unknown>,
        ])
      : []
  if (entries.length === 0) return null
  const properties: Record<string, JsonSchema> = {}
  const required: string[] = []
  for (const [k, c] of entries) {
    const t = String(c.data_type ?? c.dataType ?? 'any').toLowerCase()
    const js: JsonSchema = {}
    if (c.label != null) js.title = String(c.label)
    if (c.desc != null) js.description = String(c.desc)
    if (c.default_value != null) js.default = c.default_value
    switch (t) {
      case 'string':
      case 'datetime':
        js.type = 'string'
        break
      case 'integer':
        js.type = 'integer'
        break
      case 'number':
      case 'float':
        js.type = 'number'
        break
      case 'bool':
      case 'boolean':
        js.type = 'boolean'
        break
      case 'array':
        js.type = 'array'
        break
      case 'object':
      case 'map':
        js.type = 'object'
        break
      default:
        break // any / 未知 → 不带 type，JSON 兜底
    }
    properties[k] = js
    if (c.is_required === true || c.isRequired === true) required.push(k)
  }
  return { type: 'object', properties, ...(required.length ? { required } : {}) }
}
