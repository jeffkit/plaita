/**
 * 版本源码视图（`GET /flows/{id}/versions/{ver}/source`）的**降级决策与文案**。
 *
 * 后端把「反编译不可用」的原因如实算进响应（`source_kind="unavailable"` + `reason`，
 * 见 plaita-console/backend/api/flows.py），但这类结果不会自己出现在界面上——
 * 调用方必须显式接线。本模块把两件事收敛成纯函数，使**每个分支都能被断言**（#117）：
 *
 * 1. `resolveSourceResponse` / `resolveSourceFailure`：把「响应 body / 请求失败」
 *    映射为可渲染状态，穷尽 `authoritative | decompiled | unavailable | 未知枚举 |
 *    空正文 | 404 | 5xx | 网络失败 | 响应体不可解析`；
 * 2. `sourceNoteText` / `DECOMPILED_BANNER_TEXT`：按 `format × sourceKind` 选文案，
 *    保证反编译横幅与说明段落互相印证，不再自相矛盾。
 *
 * 零依赖（不 import react/zustand），可被 node 直接加载做行为断言——
 * 见 plaita-console/frontend/scripts/check-source-view.mjs。
 */

/** 源码面板页签格式。 */
export type SourceFormat = 'yaml' | 'json' | 'flow'

/** @flow 页签的数据来源；null = 无源码，或来源未标注（不可按行号跳转）。 */
export type SourceKind = 'authoritative' | 'decompiled' | null

/** `GET …/source` 的响应形状（与 services/api.ts 的 getFlowVersionSource 对齐）。 */
export interface FlowVersionSourceResponse {
  source_kind?: string | null
  source?: string | null
  reason?: string | null
}

/** 一次 /source 结果在前端落地后的完整状态。 */
export interface SourceViewState {
  /** @flow 页签正文；null = 不展示 @flow 页签。 */
  source: string | null
  /** 源码来源；source 为 null 时恒为 null。 */
  sourceKind: SourceKind
  /** 无 @flow 源码时呈现给用户的原因；null = 有源码，或尚未取到结果。 */
  unavailableReason: string | null
  /** 需要 console.warn 的可观测告警（排障用）；null = 无需告警。 */
  warning: string | null
}

/** 尚未取到结果 / 已清空：无源码、无原因、无告警。 */
export const EMPTY_SOURCE_VIEW: SourceViewState = {
  source: null,
  sourceKind: null,
  unavailableReason: null,
  warning: null,
}

/** 去空白后仍非空的字符串才算「有声明的值」；空串/空白串按缺失处理。 */
function nonEmptyString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() !== '' ? value : null
}

/**
 * flow 定义 `metadata.source`（内嵌权威源码）；纯空白按「没有」处理。
 *
 * 必须与后端判定一致——`api/flows.py` 用 `md["source"].strip()` 决定是否走
 * `emit_source` 兜底；前端若只判真值，`metadata.source = "   "` 会渲染出一个
 * **空白却自称权威、且不再走兜底**的 @flow 页签（同一处切换的漏格）。
 * 非空白的源码原样返回（不 trim 正文，保留权威字节）。
 */
export function embeddedSource(metadata: unknown): string {
  if (!metadata || typeof metadata !== 'object') return ''
  const source = (metadata as { source?: unknown }).source
  return typeof source === 'string' && source.trim() !== '' ? source : ''
}

/**
 * 响应 body → 状态。穷尽 `source_kind` 的取值：
 * - `authoritative` / `decompiled`：有正文即展示；**声称有源码却给了空正文**
 *   如实降级为不可用（不静默、不留空页签）；
 * - `unavailable`：展示后端 `reason`（后端未给出时给兜底文案并告警）；
 * - 其余取值（缺失 / 未来新增枚举）：有正文则展示但**不认领权威性**
 *   （sourceKind=null，文案按来源未标注），无正文则如实报「无法识别」。
 */
export function resolveSourceResponse(
  res: FlowVersionSourceResponse | null | undefined,
): SourceViewState {
  const kind = nonEmptyString(res?.source_kind)
  const source = nonEmptyString(res?.source)
  const reason = nonEmptyString(res?.reason)

  if (kind === 'authoritative' || kind === 'decompiled') {
    if (source) return { source, sourceKind: kind, unavailableReason: null, warning: null }
    return {
      source: null,
      sourceKind: null,
      unavailableReason: `后端返回 source_kind=${kind} 但源码为空`,
      warning: `[source-view] source_kind=${kind} 但 source 为空，按不可用处理`,
    }
  }

  if (kind === 'unavailable') {
    return {
      source: null,
      sourceKind: null,
      unavailableReason: reason ?? '反编译不可用（后端未给出 reason）',
      warning: reason ? null : '[source-view] unavailable 响应缺少 reason',
    }
  }

  if (source) {
    return {
      source,
      sourceKind: null,
      unavailableReason: null,
      warning: `[source-view] 未识别的 source_kind=${kind ?? '(缺失)'}：按来源未标注展示`,
    }
  }

  return {
    source: null,
    sourceKind: null,
    unavailableReason: kind
      ? `无法识别的 source_kind：${kind}${reason ? `（${reason}）` : ''}`
      : `源码接口响应无法识别（缺少 source_kind）${reason ? `：${reason}` : ''}`,
    warning: `[source-view] 未识别的响应 body：source_kind=${kind ?? '(缺失)'}`,
  }
}

/**
 * 请求失败 → 状态。按**可区分的失败类别**给原因，而不是一律静默：
 * 404（后端版本旧 / 路由缺失）≠ 5xx ≠ 网络或超时 ≠ 响应体不可解析。
 *
 * `request()` 对非 2xx 抛 `Error` 并挂 `status`（services/api.ts），网络失败抛
 * `TypeError`，HTTP 200 但正文不是 JSON 时 `response.json()` 抛 `SyntaxError`。
 */
export function resolveSourceFailure(err: unknown): SourceViewState {
  const status =
    typeof (err as { status?: unknown } | null | undefined)?.status === 'number'
      ? (err as { status: number }).status
      : null
  const message = err instanceof Error && err.message ? err.message : String(err)

  let reason: string
  if (status === 404) {
    reason = '源码接口不可用（HTTP 404）：该版本没有源码视图，或后端版本过旧尚未提供该接口'
  } else if (status !== null && status >= 500) {
    reason = `源码接口暂不可用（HTTP ${status}）：服务端错误，可稍后重试`
  } else if (status !== null) {
    reason = `源码接口请求失败（HTTP ${status}）：${message}`
  } else if (err instanceof SyntaxError) {
    reason = '源码接口返回内容无法解析（响应不是合法 JSON）'
  } else {
    reason = `源码接口请求失败（网络不可达或超时）：${message}`
  }

  return {
    source: null,
    sourceKind: null,
    unavailableReason: reason,
    warning: `[source-view] ${reason}`,
  }
}

/**
 * 面板说明段落文案。必须同时看 `format` 与 `sourceKind`：
 * YAML / JSON 与来源无关；@flow 页签的说明取决于来源是否权威。
 *
 * decompiled 分支与同屏横幅（SourceViewPanel 顶部，逐字「id/行号不对应原文件，
 * 节点行号跳转停用」）互相印证——此处不得再出现「与画布节点 source_line 对应」，
 * 否则就是 #117 的同屏自相矛盾。
 */
export function sourceNoteText(format: SourceFormat, sourceKind: SourceKind): string {
  if (format === 'yaml') {
    return 'YAML：配置文件首选，支持注释。保存到文件用 .yaml / .yml 后缀，Flow.from_file 可直接加载。'
  }
  if (format === 'json') {
    return 'JSON：与可视化编排工具互通的格式，Flow.from_string 可直接加载。'
  }
  if (sourceKind === 'authoritative') {
    return '权威 @flow 源码（节点 id 即变量名 / 条件语义）；行号与画布节点 source_line 对应。'
  }
  if (sourceKind === 'decompiled') {
    return '反编译等效源码（由定义经 emit_source 重构）：行号来自反编译结果，不对应任何仓内文件，也不与画布节点 source_line 对应。'
  }
  return '@flow 源码（来源未标注）：是否与仓内权威源码一致、行号是否对应画布节点 source_line 均未知。'
}
