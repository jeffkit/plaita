/**
 * JSON 值分类与折叠态摘要（JsonViewer 与节点时间线共用的纯函数）。
 *
 * 独立成模块而非塞在 JsonViewer 里：组件文件只导出组件，避免打破
 * react-refresh 的 fast-refresh 约束。
 */

export type JsonKind = 'object' | 'array' | 'string' | 'number' | 'boolean' | 'null'

export function kindOf(value: unknown): JsonKind {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return 'array'
  switch (typeof value) {
    case 'object':
      return 'object'
    case 'number':
      return 'number'
    case 'boolean':
      return 'boolean'
    case 'string':
      return 'string'
    default:
      return 'string'
  }
}

export function entriesOf(value: unknown): Array<[string, unknown]> {
  if (Array.isArray(value)) return value.map((v, i) => [String(i), v])
  if (value && typeof value === 'object') return Object.entries(value as Record<string, unknown>)
  return []
}

/** 折叠态摘要：让每个节点不用展开就有信息量 */
export function summaryOf(value: unknown): string {
  const kind = kindOf(value)
  if (kind === 'object') {
    const keys = Object.keys(value as Record<string, unknown>)
    return keys.length === 0 ? '空对象' : `${keys.length} 个键`
  }
  if (kind === 'array') {
    const len = (value as unknown[]).length
    return len === 0 ? '空数组' : `${len} 项`
  }
  if (kind === 'string') {
    const s = value as string
    if (s === '') return '空字符串'
    const oneLine = s.replace(/\s+/g, ' ')
    return `"${oneLine.length > 42 ? `${oneLine.slice(0, 42)}…` : oneLine}"`
  }
  if (kind === 'null') return 'null'
  return String(value)
}

/** 供折叠态列表复用的值摘要：kind = 类型标签，text = 一行摘要 */
export function jsonSummary(value: unknown): { kind: JsonKind; text: string } {
  return { kind: kindOf(value), text: summaryOf(value) }
}
