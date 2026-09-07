/**
 * 各内置类型的核心参数白名单（schema 权威键名，snake_case）。
 * 白名单命中 + schema required 的字段平铺在「配置」Tab 首屏；
 * 其余标量字段落「更多参数」折叠区。改这个清单即可调整各类型首屏。
 */
export const CORE_FIELDS: Record<string, string[]> = {
  start: [],
  end: ['result_type'],
  http: ['url', 'method', 'headers', 'query', 'body'],
  if: ['condition'],
  switch: ['branches'],
  case: ['target', 'cases', 'default'],
  // upstream_output 由抽屉的「上游依赖」行编辑器接管（formExcludeKeys），
  // 此处留空——历史上写的 'assignments' 是引擎里不存在的键（漂移死条目）
  assignment: [],
  // calculate 未在任何 registry 注册（2026-09 表单评审确认），死条目已删
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
  event: ['event_type'],
  approval: ['approval_title', 'approval_content', 'approvers', 'approval_type', 'approval_strategy'],
  // delay_seconds 必填本就置顶；delay_unit 白名单化让「5 分钟」不用去折叠区找单位
  delay: ['delay_seconds', 'delay_unit'],
  http_callback: ['callback_path', 'callback_method', 'callback_timeout_minutes'],
  kafka_queue: ['bootstrap_servers', 'topic', 'group_id', 'security_protocol'],
  redis_queue: ['redis_host', 'redis_port', 'queue_name', 'queue_type'],
}

export function coreFieldsOf(nodeType: string): Set<string> {
  return new Set(CORE_FIELDS[nodeType] ?? [])
}
