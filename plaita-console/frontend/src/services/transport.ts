/**
 * 传输层纯函数与结构化错误（S17-03）。
 *
 * 本模块不触碰 window/localStorage，可被 node 脚本直接 import 做行为断言
 * （见 scripts/check-api-transport.mjs）；api.ts 负责注入鉴权与子路径。
 */

export type ApiErrorKind =
  | 'http'
  | 'timeout'
  | 'network'
  | 'non_json'
  | 'spa_fallback'
  | 'aborted'

/** 超时分级（集中常量化；B 席可按实测微调）。 */
export const API_TIMEOUTS = {
  /** 常规 JSON 请求 */
  json: 15_000,
  /** cluster 生命周期（start/stop/remove/infrastructure） */
  lifecycle: 60_000,
  /** flows/dry-run */
  dryRun: 120_000,
  /** supervisor/iterate（长跑） */
  supervisorIterate: 600_000,
  /** SSE / 票据建连 */
  streamConnect: 10_000,
} as const

export interface ApiErrorOptions {
  kind: ApiErrorKind
  url: string
  status?: number
  detail?: unknown
  bodySnippet?: string
  /** 409 in-progress 的 Retry-After（毫秒）；r59 传输层退避用。 */
  retryAfterMs?: number
}

export class ApiError extends Error {
  readonly kind: ApiErrorKind
  readonly url: string
  readonly status?: number
  readonly detail?: unknown
  readonly bodySnippet?: string
  readonly retryAfterMs?: number

  constructor(message: string, opts: ApiErrorOptions) {
    super(message)
    this.name = 'ApiError'
    this.kind = opts.kind
    this.url = opts.url
    this.status = opts.status
    this.detail = opts.detail
    this.bodySnippet = opts.bodySnippet
    this.retryAfterMs = opts.retryAfterMs
  }

  /** 面向 UI 的一句话；页面不得再把错误折算成空列表/零值。 */
  get userMessage(): string {
    switch (this.kind) {
      case 'timeout':
        return this.message
      case 'network':
        return this.message || '无法连接服务端，请检查网络或服务状态'
      case 'non_json':
        return `服务端返回了非 JSON 响应（HTTP ${this.status ?? '?'}）：${this.bodySnippet || this.url}`
      case 'spa_fallback':
        return this.message
      case 'aborted':
        return this.message || '请求已取消'
      case 'http':
      default:
        return this.message
    }
  }
}

/** 执行是否已到终态（SSE 关闭不应再当作断连）。 */
export function isTerminalStatus(status: string | null | undefined): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled' || status === 'error'
}

const HTML_PREFIX = /^\s*(?:<!doctype\s+html|<html(?:\s|>))/i

export function looksLikeHtml(body: string): boolean {
  return HTML_PREFIX.test(body || '')
}

/** 请求 URL 是否落在 API 命名空间（含子路径部署）。 */
export function isApiRequestUrl(url: string): boolean {
  const path = url.split(/[?#]/, 1)[0]
  return path === '/api' || path.endsWith('/api') || path.includes('/api/')
}

/**
 * 把相对请求路径解析到部署子路径：
 * - 绝对 URL 原样返回；
 * - 已带 SUB_PATH 前缀的不重复拼接；
 * - 其余补前导 `/` 后拼 SUB_PATH。
 */
export function resolveApiUrl(input: string, subPath: string): string {
  if (/^https?:\/\//i.test(input)) return input
  const path = input.startsWith('/') ? input : `/${input}`
  if (subPath && (path === subPath || path.startsWith(`${subPath}/`))) return path
  return `${subPath}${path}`
}

/** 从 401 以外的错误响应体里取展示文案（兼容 detail 字符串/结构化对象）。 */
export function messageFromDetail(rawDetail: unknown): string | null {
  if (typeof rawDetail === 'string' && rawDetail.trim()) return rawDetail
  if (rawDetail && typeof rawDetail === 'object') {
    const message = (rawDetail as { message?: unknown }).message
    if (typeof message === 'string' && message.trim()) return message
  }
  return null
}

export function defaultHttpMessage(status: number): string {
  if (status >= 500) return `服务端错误（HTTP ${status}）`
  if (status === 408) return '请求超时（HTTP 408）'
  if (status === 429) return '请求过于频繁（HTTP 429）'
  if (status === 403) return '无权访问（HTTP 403）'
  if (status === 404) return '资源不存在（HTTP 404）'
  return `请求失败（HTTP ${status}）`
}

/**
 * 构造 HTTP 错误：保留 status 与结构化 detail；HTML 错误页不丢状态码。
 */
export function buildHttpError(
  status: number,
  bodyText: string,
  contentType: string | null,
  url: string,
  retryAfterMs?: number,
): ApiError {
  const text = bodyText || ''
  let rawDetail: unknown = null
  const isJson = /json/i.test(contentType || '')
  if (isJson && text.trim()) {
    try {
      rawDetail = (JSON.parse(text) as { detail?: unknown } | null)?.detail ?? null
    } catch {
      rawDetail = null
    }
  }
  const message = messageFromDetail(rawDetail) || defaultHttpMessage(status)
  return new ApiError(message, {
    kind: 'http',
    url,
    status,
    detail: rawDetail && typeof rawDetail === 'object' ? rawDetail : undefined,
    bodySnippet: isJson ? undefined : text.slice(0, 1024),
    retryAfterMs,
  })
}

/** Retry-After → 毫秒（支持秒数与 HTTP-date；非法/缺失返回 undefined）。 */
export function parseRetryAfterMs(raw: string | null, nowMs = Date.now()): number | undefined {
  if (!raw) return undefined
  const value = raw.trim()
  if (/^\d+$/.test(value)) return Math.max(0, Number(value) * 1000)
  const when = Date.parse(value)
  if (Number.isNaN(when)) return undefined
  return Math.max(0, when - nowMs)
}

/** 409 execution_start_in_progress：同 dedup_key 重试可收敛到既有 id。 */
export function isExecutionStartInProgress(error: unknown): boolean {
  const err = error as { status?: number; detail?: unknown } | null
  if (!err || err.status !== 409) return false
  const detail = err.detail as { error?: unknown } | null | undefined
  return detail?.error === 'execution_start_in_progress'
}

/** 启动 409 的退避：优先服务端 Retry-After，否则 200ms 指数退避（上限 1s）。 */
export function startRetryDelayMs(error: unknown, attempt: number): number {
  const err = error as { retryAfterMs?: number } | null
  const hinted = err?.retryAfterMs
  if (typeof hinted === 'number' && Number.isFinite(hinted) && hinted >= 0) {
    return Math.min(hinted, 5000)
  }
  return Math.min(200 * 2 ** Math.max(0, attempt), 1000)
}

/**
 * r60/A106-02：409 重试窗口必须覆盖 owner claiming TTL（60s）。窗口内
 * in-progress/superseded 都按同一 dedup_key 重试；超窗给出具名引导，绝不
 * 静默换 key 再提交（那会形成第二次执行）。
 */
export const START_RETRY_WINDOW_MS = 65_000

export function isExecutionStartRetryable(error: unknown): boolean {
  const err = error as { status?: number; detail?: unknown } | null
  if (!err || err.status !== 409) return false
  const detail = err.detail as { error?: unknown } | null | undefined
  return (
    detail?.error === 'execution_start_in_progress' ||
    detail?.error === 'execution_start_superseded'
  )
}

export function startRetryWindowError(error: unknown): ApiError {
  const err = error as { detail?: unknown; retryAfterMs?: number } | null
  return new ApiError(
    '同 dedup_key 的启动受理仍在进行中（已超过等待窗口）。请刷新执行列表确认结果；' +
      '不要直接重复提交——新提交会生成新的 dedup_key，可能产生第二次执行。',
    {
      kind: 'http',
      url: '/api/executions',
      status: 409,
      detail: err?.detail,
      retryAfterMs: err?.retryAfterMs,
    },
  )
}

/**
 * 成功响应解码：非 JSON / SPA 兜底 HTML 一律抛结构化错误，绝不再让
 * `response.json()` 的 `Unexpected token '<'` 冒到 UI。
 */
export function decodeSuccessBody(
  contentType: string | null,
  bodyText: string,
  url: string,
): unknown {
  const text = bodyText || ''
  if (!text.trim()) return undefined
  const isJson = /json/i.test(contentType || '')
  if (!isJson) {
    if (looksLikeHtml(text) && isApiRequestUrl(url)) {
      throw new ApiError('接口不存在或前端/后端版本不一致（HTTP 200 返回 HTML）', {
        kind: 'spa_fallback',
        url,
        status: 200,
        bodySnippet: text.slice(0, 1024),
      })
    }
    throw new ApiError(`服务端返回了非 JSON 响应：${url}`, {
      kind: 'non_json',
      url,
      status: 200,
      bodySnippet: text.slice(0, 1024),
    })
  }
  try {
    return JSON.parse(text)
  } catch {
    if (looksLikeHtml(text)) {
      throw new ApiError('接口不存在或前端/后端版本不一致（HTTP 200 返回 HTML）', {
        kind: 'spa_fallback',
        url,
        status: 200,
        bodySnippet: text.slice(0, 1024),
      })
    }
    throw new ApiError(`服务端返回了非法 JSON：${url}`, {
      kind: 'non_json',
      url,
      status: 200,
      bodySnippet: text.slice(0, 1024),
    })
  }
}

/** React Query 查询重试策略：最多 1 次，仅超时/网络/408/429/5xx。 */
export function shouldRetryQuery(failureCount: number, error: unknown): boolean {
  if (failureCount >= 1) return false
  const err = error as { kind?: string; status?: number } | null
  if (err?.kind === 'timeout' || err?.kind === 'network') return true
  const status = err?.status
  return status === 408 || status === 429 || (typeof status === 'number' && status >= 500)
}

/** 统一的人话错误文案（ApiError 优先）。 */
export function errorUserMessage(error: unknown): string {
  if (error instanceof ApiError) return error.userMessage
  if (error instanceof Error) return error.message
  return String(error)
}
