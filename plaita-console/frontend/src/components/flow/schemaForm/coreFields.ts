/**
 * 各内置类型的核心参数白名单（schema 权威键名，snake_case）。
 * 白名单命中 + schema required 的字段平铺在「配置」Tab 首屏；
 * 其余标量字段落「更多参数」折叠区。改这个清单即可调整各类型首屏。
 */
export const CORE_FIELDS: Record<string, string[]> = {
  start: [],
  end: [],
  http: ['url', 'method', 'headers', 'query', 'body'],
  if: ['condition'],
  switch: ['branches'],
  case: ['target', 'cases', 'default'],
  assignment: ['assignments'],
  calculate: ['expression'],
  code: ['code', 'language'],
  map: ['collection', 'item_type', 'concurrent', 'max_concurrent'],
  loop: ['collection', 'condition'],
  filter: ['collection', 'condition'],
  find: ['collection', 'condition'],
  reduce: ['collection', 'initial'],
  while: ['condition', 'max_iterations'],
  child: [],
  reference: ['flow_id', 'flow_version'],
  parallel: ['mode', 'is_conditional', 'join_branches'],
  event: [],
  approval: [],
  delay: ['duration'],
  http_callback: [],
  kafka_queue: [],
  redis_queue: [],
}

export function coreFieldsOf(nodeType: string): Set<string> {
  return new Set(CORE_FIELDS[nodeType] ?? [])
}
