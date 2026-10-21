/**
 * API 服务层
 */
import {
  API_TIMEOUTS,
  ApiError,
  START_RETRY_WINDOW_MS,
  buildHttpError,
  decodeSuccessBody,
  isExecutionStartRetryable,
  parseRetryAfterMs,
  resolveApiUrl,
  startRetryDelayMs,
  startRetryWindowError,
} from './transport'

export {
  API_TIMEOUTS,
  ApiError,
  buildHttpError,
  errorUserMessage,
  isTerminalStatus,
  shouldRetryQuery,
  type ApiErrorKind,
} from './transport'

// 子路径部署探测：dsht 隧道以 https://dsht.agentstudio.cc/plaita/（nginx 剥前缀）
// 暴露；本机等根路径部署检测不命中、行为不变。
export const SUB_PATH = /^\/plaita(\/|$)/.test(window.location.pathname) ? '/plaita' : ''

export const API_BASE = `${SUB_PATH}/api`

// #266：平台管理 API Key 只属于服务端/CLI 调用方，浏览器端一律只用会话
// Bearer。构建期 VITE_* 变量会被 Vite 内联进公开 bundle——任何管理密钥都
// 不得出现在前端源码/构建配置里。

// 会话 token（RBAC 登录后保存）
export function getToken(): string | null {
  return localStorage.getItem('plaita_token')
}

export interface MembershipInfo {
  tenant_id: string
  role: string
}

export function setSession(
  token: string,
  username: string,
  role: string,
  opts?: { tenantId?: string | null; platformAdmin?: boolean; memberships?: MembershipInfo[] },
): void {
  localStorage.setItem('plaita_token', token)
  localStorage.setItem('plaita_username', username)
  localStorage.setItem('plaita_role', role)
  localStorage.setItem('plaita_tenant', opts?.tenantId || '')
  localStorage.setItem('plaita_platform_admin', opts?.platformAdmin ? '1' : '')
  localStorage.setItem('plaita_memberships', JSON.stringify(opts?.memberships || []))
}

export function clearSession(): void {
  localStorage.removeItem('plaita_token')
  localStorage.removeItem('plaita_username')
  localStorage.removeItem('plaita_role')
  localStorage.removeItem('plaita_tenant')
  localStorage.removeItem('plaita_platform_admin')
  localStorage.removeItem('plaita_memberships')
}

export function getRole(): string {
  return localStorage.getItem('plaita_role') || 'viewer'
}

/** 当前活跃租户（多租户成员；空串 = 平台全量视角） */
export function getTenant(): string {
  return localStorage.getItem('plaita_tenant') || ''
}

export function setTenant(tenantId: string, role?: string): void {
  localStorage.setItem('plaita_tenant', tenantId || '')
  if (role) localStorage.setItem('plaita_role', role)
}

export function isPlatformAdmin(): boolean {
  return localStorage.getItem('plaita_platform_admin') === '1'
}

export function getMemberships(): MembershipInfo[] {
  try {
    return JSON.parse(localStorage.getItem('plaita_memberships') || '[]')
  } catch {
    return []
  }
}

/** 鉴权头注入：只发会话 Bearer；平台管理员显式声明租户上下文（普通用户
 *  服务端钉死活跃租户，带头会 403）。供流式 fetch 等绕过 request() 的调用方
 *  复用，避免两处实现漂移。 */
export function authHeaders(): Record<string, string> {
  const headers: Record<string, string> = {}
  const token = getToken()
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  if (isPlatformAdmin() && getTenant()) {
    headers['X-Tenant-ID'] = getTenant()
  }
  return headers
}

export interface RequestOptions {
  /** 401 是否清会话并跳登录页（登录期租户选择必须关闭）。默认 true。 */
  authRedirect?: boolean
  /** 总超时（含响应体读取）。默认 API_TIMEOUTS.json。 */
  timeoutMs?: number
}

export interface ApiFetchOptions {
  /** 401 是否清会话并跳登录页。默认 true。 */
  authRedirect?: boolean
  /** 建连超时；默认 stream 用 10s，其余用 15s。 */
  timeoutMs?: number
  /** 流式读：只覆盖建连，不覆盖读流。 */
  stream?: boolean
}

function isAuthEndpoint(url: string): boolean {
  return /\/auth\/(?:login|setup)(?:$|[/?#])/.test(url)
}

let redirecting = false

/** 401 统一出口：清会话 + 跳登录页（模块级防并发重复跳转）。 */
function triggerAuthRedirect(): void {
  if (redirecting) return
  redirecting = true
  clearSession()
  // 子路径部署下 /login 不是本 console 的登录页（登录页在 ${SUB_PATH}/login，
  // router basename 同源），写死根路径会把用户踢出 console（#103）
  window.location.assign(`${SUB_PATH}/login`)
}

/**
 * 所有直连 fetch 的唯一入口（S17-03）：解析子路径、按调用时点注入鉴权头、
 * 合并外部 signal、建连超时与 401 统一处理。init.headers / init.signal 原样保留。
 */
export async function apiFetch(
  input: string,
  init?: RequestInit,
  opts: ApiFetchOptions = {},
): Promise<Response> {
  const url = resolveApiUrl(input, SUB_PATH)
  const headers = new Headers(init?.headers)
  for (const [key, value] of Object.entries(authHeaders())) {
    if (!headers.has(key)) headers.set(key, value)
  }

  const external = init?.signal ?? null
  const controller = new AbortController()
  let timedOut = false
  const forwardAbort = () => controller.abort()
  if (external) {
    if (external.aborted) controller.abort()
    else external.addEventListener('abort', forwardAbort, { once: true })
  }
  const timeoutMs =
    opts.timeoutMs ?? (opts.stream ? API_TIMEOUTS.streamConnect : API_TIMEOUTS.json)
  const timer = setTimeout(() => {
    timedOut = true
    controller.abort()
  }, timeoutMs)

  try {
    const response = await fetch(url, { ...init, headers, signal: controller.signal })
    if (response.status === 401 && opts.authRedirect !== false && !isAuthEndpoint(url)) {
      triggerAuthRedirect()
    }
    return response
  } catch {
    if (timedOut) {
      throw new ApiError(`请求超时（${Math.round(timeoutMs / 1000)}s）`, {
        kind: 'timeout',
        url,
      })
    }
    if (external?.aborted) {
      throw new ApiError('请求已取消', { kind: 'aborted', url })
    }
    throw new ApiError('无法连接服务端，请检查网络或服务状态', { kind: 'network', url })
  } finally {
    clearTimeout(timer)
    external?.removeEventListener('abort', forwardAbort)
  }
}

// 通用请求函数
async function request<T>(
  url: string,
  options?: RequestInit,
  opts: RequestOptions = {},
): Promise<T> {
  const fullUrl = `${API_BASE}${url}`
  const timeoutMs = opts.timeoutMs ?? API_TIMEOUTS.json
  const external = options?.signal ?? null
  const controller = new AbortController()
  const forwardAbort = () => controller.abort()
  if (external) {
    if (external.aborted) controller.abort()
    else external.addEventListener('abort', forwardAbort, { once: true })
  }
  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    controller.abort()
  }, timeoutMs)

  const headers = new Headers(options?.headers)
  if (!headers.has('Content-Type')) headers.set('Content-Type', 'application/json')

  const method = (options?.method || 'GET').toUpperCase()
  const writeHint = method === 'GET' || method === 'HEAD' ? '' : '，结果未知，请刷新确认'
  const timeoutError = () =>
    new ApiError(`请求超时（${Math.round(timeoutMs / 1000)}s）${writeHint}`, {
      kind: 'timeout',
      url: fullUrl,
    })

  try {
    const response = await apiFetch(
      fullUrl,
      { ...options, headers, signal: controller.signal },
      { authRedirect: opts.authRedirect, timeoutMs },
    )
    const text = await response.text()
    const contentType = response.headers.get('content-type')

    if (!response.ok) {
      throw buildHttpError(
        response.status,
        text,
        contentType,
        fullUrl,
        parseRetryAfterMs(response.headers.get('retry-after')),
      )
    }
    // 204 / 空 body：现在没有该形态的端点，防御性返回 undefined。
    if (response.status === 204 || !text.trim()) return undefined as T
    return decodeSuccessBody(contentType, text, fullUrl) as T
  } catch (error) {
    if (timedOut) throw timeoutError()
    if (error instanceof ApiError) throw error
    throw new ApiError('无法连接服务端，请检查网络或服务状态', {
      kind: 'network',
      url: fullUrl,
    })
  } finally {
    clearTimeout(timer)
    external?.removeEventListener('abort', forwardAbort)
  }
}

// ============ 类型定义 ============

export interface ServiceInfo {
  instance_id: string
  service_type: string
  host: string
  /** starting / running / draining（优雅下线中，不再领新任务）/ stopping / stopped */
  status: string
  start_time?: string
  metadata: Record<string, unknown>
  active_tasks: number
  last_heartbeat?: string
}

export interface ServiceListResponse {
  services: ServiceInfo[]
  total: number
}

export interface ServiceTopology {
  nodes: Array<{
    instance_id: string
    service_type: string
    name: string
    host: string
    status: string
    start_time?: string
    metadata: Record<string, unknown>
  }>
  edges: Array<{
    source_id: string
    target_id: string
    edge_type: string
    label: string
  }>
  timestamp: string
}

/** 节点级耗时（worker 的 NodeTimingCallback 与本地 trace 折算同构） */
export interface NodeTiming {
  started_at?: string
  ended_at?: string
  /** epoch 毫秒：算相对偏移用，避免与流程 start_time 的时区基准不一致 */
  started_ms?: number
  ended_ms?: number
  /** 最后一次执行的耗时 */
  duration_ms?: number
  /** 同一节点被访问多次（循环/重试）时的累计耗时 */
  total_duration_ms?: number
  attempts?: number
  failed?: boolean
}

export interface ExecutionInfo {
  execution_id: string
  flow_id: string
  flow_version?: string
  status: string
  start_time?: string
  end_time?: string
  last_update_time?: string
  context?: Record<string, unknown>
  error?: Record<string, unknown>
  invoker?: string
  // 本地单机模式专有：节点级 trace 与最终输出
  nodes?: Array<{ id: string; type: string; name?: string; input?: unknown; output?: unknown; status: string; error?: string }> | null
  output?: unknown
  /** 节点级耗时（node_id → 耗时）；老状态/未采集时为 null */
  node_timings?: Record<string, NodeTiming> | null
  // Langfuse 观测深链（后端启用观测且配了 LANGFUSE_PROJECT_ID 时非空）
  langfuse_trace_url?: string | null
}

export interface ExecutionListResponse {
  executions: ExecutionInfo[]
  total: number
  page: number
  size: number
}

export interface QueueInfo {
  name: string
  length: number
  queue_type: string
}

export interface QueueListResponse {
  queues: QueueInfo[]
  total: number
}

export interface QueueTask {
  index: number
  data: Record<string, unknown>
}

export interface QueueDetailResponse {
  name: string
  length: number
  queue_type?: string
  tasks: QueueTask[]
  // A2R24-02：Stream 分页按索引切片；next_start_id 是排他游标（深队列用）。
  truncated?: boolean
  next_start_id?: string | null
}

export interface LogEntry {
  timestamp: string
  level: string
  service_type?: string
  instance_id?: string
  message: string
  context?: Record<string, unknown>
  // A2R24-05：条目 id（游标翻页的键集成分）。
  entry_id?: string | null
}

export interface LogListResponse {
  logs: LogEntry[]
  total: number
  // A2R28-01：读取预算/键扫描触界时 total 只是已返回的条数（不是全集），
  // 前端必须显式提示「结果不完整」，不能把 truncated 当作「没有日志」。
  degraded?: boolean
  skipped?: number
  truncated?: boolean
  // A2R24-05：本页条数与下一页游标（total 是匹配总数；truncated 时是下界）。
  returned?: number
  next_cursor?: string | null
}

// ============ 集群管理类型 ============

export interface ServiceTypeInfo {
  service_type: string
  display_name: string
  default_instances: number
  max_instances: number
  running_count: number
}

export interface ServiceTypesResponse {
  mode: string
  service_types: ServiceTypeInfo[]
}

export interface StartServiceResponse {
  success: boolean
  instance_id?: string
  status: string
  error?: string
}

export interface ManagedInstance {
  instance_id: string
  service_type: string
  pid?: number
  container_id?: string
  status: string
  start_time: string
  error_message?: string
  managed_by: 'console' | 'external'  // console=控制台托管, external=外部注册
}

export interface ManagedInstancesResponse {
  instances: ManagedInstance[]
  total: number
}

// ============ API 函数 ============

export const api = {
  // 服务相关
  async getServices(type?: string): Promise<ServiceListResponse> {
    const url = type ? `/services?type=${type}` : '/services'
    return request(url)
  },

  async getService(instanceId: string): Promise<ServiceInfo> {
    return request(`/services/${instanceId}`)
  },

  async getTopology(): Promise<ServiceTopology> {
    return request('/services/topology')
  },

  async stopService(instanceId: string, graceful = true): Promise<void> {
    return request(`/services/${instanceId}/stop`, {
      method: 'POST',
      body: JSON.stringify({ graceful }),
    })
  },

  async getServiceStatus(instanceId: string) {
    return request(`/services/${instanceId}/status`)
  },

  // 执行相关
  async getExecutions(params: {
    page?: number
    size?: number
    status?: string
    flow_id?: string
  }): Promise<ExecutionListResponse> {
    const searchParams = new URLSearchParams()
    if (params.page) searchParams.set('page', String(params.page))
    if (params.size) searchParams.set('size', String(params.size))
    if (params.status) searchParams.set('status', params.status)
    if (params.flow_id) searchParams.set('flow_id', params.flow_id)
    
    return request(`/executions?${searchParams}`)
  },

  async getExecution(executionId: string): Promise<ExecutionInfo> {
    return request(`/executions/${executionId}`)
  },

  async startExecution(params: {
    flow_id: string
    version?: string
    params: Record<string, unknown>
    /** r59：调用方可复用同一 key；缺省为本次提交生成一次并在内部重试间复用。 */
    dedup_key?: string
  }): Promise<void> {
    const dedupKey =
      params.dedup_key ||
      (typeof crypto !== 'undefined' && 'randomUUID' in crypto
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(16).slice(2)}`)
    const body = JSON.stringify({ ...params, dedup_key: dedupKey })
    // r60/A106-02：窗口覆盖 claiming TTL（60s）；in-progress 与 superseded
    // 都用同一 dedup_key 退避重试，超窗给具名引导而不是静默换 key。
    const deadline = Date.now() + START_RETRY_WINDOW_MS
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await request('/executions', { method: 'POST', body })
      } catch (error) {
        if (!isExecutionStartRetryable(error)) throw error
        const remaining = deadline - Date.now()
        if (remaining <= 0) throw startRetryWindowError(error)
        const delay = Math.min(startRetryDelayMs(error, attempt), remaining)
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }
  },

  async cancelExecution(executionId: string): Promise<{ success: boolean; message: string }> {
    return request(`/executions/${executionId}/cancel`, {
      method: 'POST',
    })
  },

  async deleteExecution(executionId: string): Promise<{ success: boolean; message: string }> {
    return request(`/executions/${executionId}`, {
      method: 'DELETE',
    })
  },

  async resumeExecution(executionId: string, params: {
    resume_type: string
    data?: Record<string, unknown>
  }): Promise<void> {
    return request(`/executions/${executionId}/resume`, {
      method: 'POST',
      body: JSON.stringify(params),
    })
  },

  // 队列相关
  async getQueues(): Promise<QueueListResponse> {
    return request('/queues')
  },

  async getQueueDetail(queueName: string): Promise<QueueDetailResponse> {
    return request(`/queues/${encodeURIComponent(queueName)}`)
  },

  // 日志相关
  async getLogs(params: {
    service_type?: string
    instance_id?: string
    level?: string
    limit?: number
  }): Promise<LogListResponse> {
    const searchParams = new URLSearchParams()
    if (params.service_type) searchParams.set('service_type', params.service_type)
    if (params.instance_id) searchParams.set('instance_id', params.instance_id)
    if (params.level) searchParams.set('level', params.level)
    if (params.limit) searchParams.set('limit', String(params.limit))
    
    return request(`/logs?${searchParams}`)
  },

  // ============ 集群管理 ============
  
  async getServiceTypes(): Promise<ServiceTypesResponse> {
    return request('/cluster/service-types')
  },

  async startManagedService(serviceType: string): Promise<StartServiceResponse> {
    return request('/cluster/start', {
      method: 'POST',
      body: JSON.stringify({ service_type: serviceType }),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async stopManagedService(instanceId: string, graceful = true): Promise<void> {
    return request(`/cluster/stop/${instanceId}`, {
      method: 'POST',
      body: JSON.stringify({ graceful }),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async stopAllManagedServices(graceful = true): Promise<void> {
    return request('/cluster/stop-all', {
      method: 'POST',
      body: JSON.stringify({ graceful }),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async getManagedInstances(serviceType?: string): Promise<ManagedInstancesResponse> {
    const url = serviceType 
      ? `/cluster/instances?service_type=${serviceType}`
      : '/cluster/instances'
    return request(url)
  },

  async getClusterConfig(): Promise<{ mode: string; config_path: string }> {
    return request('/cluster/config')
  },

  async switchClusterMode(mode: 'process' | 'docker'): Promise<void> {
    return request('/cluster/mode', {
      method: 'POST',
      body: JSON.stringify({ mode }),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async removeInstance(instanceId: string): Promise<void> {
    return request(`/cluster/instances/${instanceId}`, {
      method: 'DELETE',
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async clearFailedInstances(): Promise<{ cleared_count: number }> {
    return request('/cluster/instances', {
      method: 'DELETE',
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  // ============ 多集群管理 ============

  async getClusters(): Promise<ClustersListResponse> {
    return request('/clusters')
  },

  async getActiveCluster(): Promise<ClusterInfo> {
    return request('/clusters/active')
  },

  async createCluster(data: CreateClusterRequest): Promise<ClusterInfo> {
    return request('/clusters', {
      method: 'POST',
      body: JSON.stringify(data),
    })
  },

  async updateCluster(clusterId: string, data: UpdateClusterRequest): Promise<ClusterInfo> {
    return request(`/clusters/${clusterId}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    })
  },

  async deleteCluster(clusterId: string): Promise<void> {
    return request(`/clusters/${clusterId}`, {
      method: 'DELETE',
    })
  },

  async switchCluster(clusterId: string): Promise<ClusterInfo> {
    return request('/clusters/switch', {
      method: 'POST',
      body: JSON.stringify({ cluster_id: clusterId }),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async getClusterConfigDetail(clusterId: string): Promise<ClusterConfigDetail> {
    return request(`/clusters/${clusterId}/config`)
  },

  async saveClusterConfigDetail(clusterId: string, config: object): Promise<void> {
    return request(`/clusters/${clusterId}/config`, {
      method: 'PUT',
      body: JSON.stringify({ config }),
    })
  },

  // ============ 基础设施 API ============

  async getInfrastructure(): Promise<InfrastructureListResponse> {
    return request('/cluster/infrastructure')
  },

  async getInfrastructureDetail(name: string): Promise<InfrastructureInfo> {
    return request(`/cluster/infrastructure/${name}`)
  },

  async startInfrastructure(name: string): Promise<{ success: boolean; status?: string; message: string }> {
    return request(`/cluster/infrastructure/${name}/start`, { method: 'POST' }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async stopInfrastructure(name: string): Promise<{ success: boolean; message: string }> {
    return request(`/cluster/infrastructure/${name}/stop`, { method: 'POST' }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async checkInfrastructureHealth(name: string): Promise<{
    name: string
    status: string
    details: unknown
    checked_at: string
  }> {
    return request(`/cluster/infrastructure/${name}/check`, {
      method: 'POST',
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async createInfrastructure(data: CreateInfrastructureRequest): Promise<InfrastructureInfo> {
    return request('/cluster/infrastructure', {
      method: 'POST',
      body: JSON.stringify(data),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async updateInfrastructure(name: string, data: UpdateInfrastructureRequest): Promise<InfrastructureInfo> {
    return request(`/cluster/infrastructure/${name}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async deleteInfrastructure(name: string): Promise<void> {
    return request(`/cluster/infrastructure/${name}`, {
      method: 'DELETE',
    }, { timeoutMs: API_TIMEOUTS.lifecycle })
  },

  async getInfrastructureTemplates(): Promise<{ templates: InfrastructureTemplate[] }> {
    return request('/cluster/infrastructure-templates')
  },

  // ============ 事件系统 API ============

  async getEvents(params: { event_type?: string; limit?: number } = {}): Promise<EventListResponse> {
    const searchParams = new URLSearchParams()
    if (params.event_type) searchParams.set('event_type', params.event_type)
    if (params.limit) searchParams.set('limit', String(params.limit))
    return request(`/events?${searchParams}`)
  },

  async publishEvent(params: {
    event_type: string
    data: Record<string, unknown>
    correlation_id?: string
  }): Promise<{ success: boolean; event_id: string; message: string }> {
    return request('/events/publish', {
      method: 'POST',
      body: JSON.stringify(params),
    })
  },

  async getSubscriptions(params: {
    event_type?: string
    flow_id?: string
  } = {}): Promise<SubscriptionListResponse> {
    const searchParams = new URLSearchParams()
    if (params.event_type) searchParams.set('event_type', params.event_type)
    if (params.flow_id) searchParams.set('flow_id', params.flow_id)
    return request(`/events/subscriptions?${searchParams}`)
  },

  async getSubscription(subscriptionId: string): Promise<SubscriptionInfo> {
    return request(`/events/subscriptions/${subscriptionId}`)
  },

  async deleteSubscription(subscriptionId: string): Promise<{ success: boolean; message: string }> {
    return request(`/events/subscriptions/${subscriptionId}`, {
      method: 'DELETE',
    })
  },

  // ============ 日志增强 API ============

  async getInstanceLogs(instanceId: string, params: {
    level?: string
    limit?: number
    order?: 'asc' | 'desc'
  } = {}): Promise<LogListResponse> {
    const searchParams = new URLSearchParams()
    if (params.level) searchParams.set('level', params.level)
    if (params.limit) searchParams.set('limit', String(params.limit))
    if (params.order) searchParams.set('order', params.order)
    
    return request(`/logs/instance/${instanceId}?${searchParams}`)
  },

  async getLogStats(): Promise<LogStatsResponse> {
    return request('/logs/stats')
  },

  // ============ 快速测试 API ============

  async getTestTemplates(): Promise<{ templates: TestTemplate[] }> {
    return request('/cluster/test-templates')
  },

  async runQuickTest(testType: string, params?: Record<string, unknown>): Promise<QuickTestResponse> {
    return request('/cluster/quick-test', {
      method: 'POST',
      body: JSON.stringify({ test_type: testType, params }),
    })
  },

  // ============ 流程编排 API ============

  async getFlows(): Promise<FlowListResponse> {
    return request('/flows')
  },

  async createFlow(payload: { flow_id: string; author?: string; desc?: string }): Promise<FlowSummaryView> {
    return request('/flows', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
  },

  async getFlow(flowId: string): Promise<FlowDetailResponse> {
    return request(`/flows/${flowId}`)
  },

  async deleteFlow(flowId: string): Promise<{ success: boolean }> {
    return request(`/flows/${flowId}`, { method: 'DELETE' })
  },

  async getVersion(flowId: string, version: string): Promise<VersionView> {
    return request(`/flows/${flowId}/versions/${version}`)
  },

  /** 版本的 codeflow 源码视图：authoritative=metadata.source（仓内权威）；
   *  decompiled=emit_source 反编译兜底（等效非权威，行号不对应原文件）。 */
  async getFlowVersionSource(
    flowId: string,
    version: string,
  ): Promise<{ source_kind: 'authoritative' | 'decompiled' | 'unavailable'; source: string | null; reason?: string }> {
    return request(`/flows/${flowId}/versions/${version}/source`)
  },

  async saveVersion(
    flowId: string,
    version: string,
    payload: {
      definition: string
      layout: string
      created_by?: string
      /** C5-1 乐观锁基准：所基于版本的 updated_at；不一致服务端返回 409 */
      base_updated_at?: string | null
      /** 强制覆盖（绕过乐观锁检查） */
      force?: boolean
      /** 忽略 version 参数，由服务端原子分配下一个 patch 版本 */
      allocate_version?: boolean
    }
  ): Promise<VersionView> {
    return request(`/flows/${flowId}/versions/${version}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    })
  },

  async deleteVersion(flowId: string, version: string): Promise<{ success: boolean }> {
    return request(`/flows/${flowId}/versions/${version}`, { method: 'DELETE' })
  },

  async publishFlow(flowId: string, version: string): Promise<VersionView> {
    return request(`/flows/${flowId}/publish`, {
      method: 'POST',
      body: JSON.stringify({ version }),
    })
  },

  // 注：AI 生成流程走 /flows/ai-generate/stream（SSE），由 AiGenerateDialog
  // 直接 fetch 流式端点，此处不再提供非流式封装（后端无该端点）。

  async getNodes(): Promise<NodeListResponse> {
    return request('/nodes')
  },

  async registerNode(payload: RegisterNodeRequest): Promise<NodeDescriptorView> {
    return request('/nodes', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
  },

  async deleteNode(nodeType: string): Promise<{ success: boolean }> {
    return request(`/nodes/${nodeType}`, { method: 'DELETE' })
  },

  // ---- 自定义属性类型（C2：console 侧命名别名，保存节点 schema 时展开为内置基础类型）----

  async getPropertyTypes(): Promise<PropertyTypeListResponse> {
    return request('/property-types')
  },

  async upsertPropertyType(payload: UpsertPropertyTypeRequest): Promise<PropertyTypeView> {
    return request('/property-types', { method: 'POST', body: JSON.stringify(payload) })
  },

  async deletePropertyType(name: string): Promise<{ success: boolean }> {
    return request(`/property-types/${encodeURIComponent(name)}`, { method: 'DELETE' })
  },

  // ---- 凭据类型模板（C3：后端内置注册表，只读）----

  async getCredentialTemplates(): Promise<CredentialTemplatesResponse> {
    return request('/credential-templates')
  },

  async dryRun(payload: {
    flowJson: string
    input?: Record<string, unknown>
    pinned?: Record<string, unknown>
    onlyNode?: string
  }): Promise<DryRunResponse> {
    return request('/flows/dry-run', {
      method: 'POST',
      body: JSON.stringify(payload),
    }, { timeoutMs: API_TIMEOUTS.dryRun })
  },

  // ============ 审计与部署 ============

  async getAudit(params?: { action?: string; actor?: string; limit?: number }): Promise<{
    logs: Array<{ ts: string; actor: string; action: string; resource: string; resource_id: string; detail: Record<string, unknown>; ip: string }>
  }> {
    const q = new URLSearchParams()
    if (params?.action) q.set('action', params.action)
    if (params?.actor) q.set('actor', params.actor)
    if (params?.limit) q.set('limit', String(params.limit))
    return request(`/audit?${q}`)
  },

  async getDeployments(flowId?: string): Promise<{
    deployments: Array<{ flow_id: string; version: string; environment: string; actor: string; definition_hash: string; created_at: string }>
  }> {
    const q = flowId ? `?flow_id=${encodeURIComponent(flowId)}` : ''
    return request(`/deployments${q}`)
  },

  // ============ 认证与用户 ============

  async setupStatus(): Promise<{ needs_setup: boolean }> {
    return request('/auth/setup-status')
  },

  async setup(payload: { username: string; password: string }): Promise<{
    token: string; username: string; role: string
  }> {
    return request('/auth/setup', { method: 'POST', body: JSON.stringify(payload) })
  },

  async login(username: string, password: string): Promise<{
    token: string; username: string; role: string; expires_at: string
    platform_admin?: boolean; active_tenant?: string | null
    memberships?: MembershipInfo[]
  }> {
    return request('/auth/login', { method: 'POST', body: JSON.stringify({ username, password }) })
  },

  async me(): Promise<{
    actor: string; role: string; tenant_id: string | null; platform_admin: boolean
  }> {
    return request('/auth/me')
  },

  /**
   * 切换服务端会话活跃租户（S17-01）。
   *
   * 登录期 token 还只在组件内存里：显式传 `token` 注入 Authorization；
   * `authRedirect:false` 阻止登录期 401 把租户选择页冲掉。
   */
  async switchTenant(
    tenantId: string | null,
    opts?: { token?: string; authRedirect?: boolean },
  ): Promise<{
    username?: string
    role: string
    tenant_id: string | null
    platform_admin: boolean
  }> {
    return request('/auth/switch-tenant', {
      method: 'POST',
      // A2R17-01：null = 平台管理员显式清空活跃租户（全量视角，只读）。
      body: JSON.stringify({ tenant_id: tenantId }),
      headers: opts?.token ? { Authorization: `Bearer ${opts.token}` } : undefined,
    }, { authRedirect: opts?.authRedirect })
  },

  // ============ 租户管理（平台管理员） ============

  async getTenants(): Promise<{
    tenants: Array<{
      id: string; name: string; status: string
      contract_secret_id: string; member_count: number; created_at: string | null
    }>
  }> {
    return request('/tenants')
  },

  async createTenant(payload: { id: string; name?: string }): Promise<{
    id: string; contract_secret_id: string; contract_secret_key: string
  }> {
    return request('/tenants', { method: 'POST', body: JSON.stringify(payload) })
  },

  async setTenantStatus(tenantId: string, status: 'active' | 'disabled'): Promise<{ success: boolean }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}/status`, {
      method: 'POST', body: JSON.stringify({ status }),
    })
  },

  async rotateTenantSecret(tenantId: string): Promise<{
    tenant_id: string; contract_secret_id: string; contract_secret_key: string
  }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}/rotate-secret`, { method: 'POST' })
  },

  async deleteTenant(tenantId: string): Promise<{ success: boolean }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}`, { method: 'DELETE' })
  },

  async getTenantMembers(tenantId: string): Promise<{
    members: Array<{ username: string; tenant_id: string; role: string }>
  }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}/members`)
  },

  async addTenantMember(tenantId: string, username: string, role: string): Promise<{ success: boolean }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}/members`, {
      method: 'POST', body: JSON.stringify({ username, role }),
    })
  },

  async setTenantMemberRole(tenantId: string, username: string, role: string): Promise<{ success: boolean }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}/members/${encodeURIComponent(username)}`, {
      method: 'PUT', body: JSON.stringify({ username, role }),
    })
  },

  async removeTenantMember(tenantId: string, username: string): Promise<{ success: boolean }> {
    return request(`/tenants/${encodeURIComponent(tenantId)}/members/${encodeURIComponent(username)}`, {
      method: 'DELETE',
    })
  },

  async logout(): Promise<{ success: boolean }> {
    return request('/auth/logout', { method: 'POST' })
  },

  async getUsers(): Promise<{ users: Array<{ username: string; role: string; platform_admin?: boolean; disabled: boolean; memberships?: MembershipInfo[]; created_at: string }> }> {
    return request('/users')
  },

  async createUser(payload: { username: string; password: string; role: string; platform_admin?: boolean; memberships?: MembershipInfo[] }): Promise<{ success: boolean }> {
    return request('/users', { method: 'POST', body: JSON.stringify(payload) })
  },

  async setUserRole(username: string, role: string): Promise<{ success: boolean }> {
    return request(`/users/${encodeURIComponent(username)}/role`, { method: 'POST', body: JSON.stringify({ role }) })
  },

  async setUserPassword(username: string, password: string): Promise<{ success: boolean }> {
    return request(`/users/${encodeURIComponent(username)}/password`, { method: 'POST', body: JSON.stringify({ password }) })
  },

  async deleteUser(username: string): Promise<{ success: boolean }> {
    return request(`/users/${encodeURIComponent(username)}`, { method: 'DELETE' })
  },

  // ============ 凭据 ============

  async getCredentials(): Promise<CredentialListResponse> {
    return request('/credentials')
  },

  async getCredential(name: string): Promise<CredentialDetail> {
    return request(`/credentials/${encodeURIComponent(name)}`)
  },

  async saveCredential(payload: {
    name: string
    type: string
    desc?: string
    data: Record<string, unknown>
  }): Promise<{ success: boolean; name: string; message: string }> {
    return request('/credentials', { method: 'POST', body: JSON.stringify(payload) })
  },

  async deleteCredential(name: string): Promise<{ success: boolean }> {
    return request(`/credentials/${encodeURIComponent(name)}`, { method: 'DELETE' })
  },

  // ============ 调度（触发器） ============

  async getSchedules(): Promise<ScheduleListResponse> {
    return request('/schedules')
  },

  async createSchedule(payload: {
    name: string
    flow_id: string
    version?: string
    cron: string
    params?: Record<string, unknown>
    enabled?: boolean
  }): Promise<ScheduleInfo> {
    return request('/schedules', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
  },

  async updateSchedule(
    scheduleId: string,
    payload: { name?: string; version?: string; cron?: string; params?: Record<string, unknown> }
  ): Promise<ScheduleInfo> {
    return request(`/schedules/${scheduleId}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    })
  },

  async deleteSchedule(scheduleId: string): Promise<{ success: boolean }> {
    return request(`/schedules/${scheduleId}`, { method: 'DELETE' })
  },

  async enableSchedule(scheduleId: string): Promise<ScheduleInfo> {
    return request(`/schedules/${scheduleId}/enable`, { method: 'POST' })
  },

  async disableSchedule(scheduleId: string): Promise<ScheduleInfo> {
    return request(`/schedules/${scheduleId}/disable`, { method: 'POST' })
  },

  async triggerSchedule(scheduleId: string): Promise<{
    success: boolean
    mode: 'local' | 'cluster'
    execution_id?: string | null
    msg_id?: string | null
  }> {
    return request(`/schedules/${scheduleId}/trigger`, { method: 'POST' })
  },

  async getScheduleHistory(
    scheduleId: string,
    limit = 10,
    offset = 0
  ): Promise<ScheduleHistoryResponse> {
    return request(
      `/schedules/${scheduleId}/history?limit=${limit}&offset=${offset}`
    )
  },

  async previewCron(cron: string, count = 5): Promise<{ cron: string; next: string[] }> {
    return request(`/schedules/preview?cron=${encodeURIComponent(cron)}&count=${count}`)
  },
}

// ============ 流程编排类型定义 ============

export interface FlowSummaryView {
  flow_id: string
  author: string
  desc: string
  created_at?: string | null
  updated_at?: string | null
}

export interface FlowListResponse {
  flows: FlowSummaryView[]
  total: number
}

export interface FlowVersionSummary {
  version: string
  status: string
  created_at?: string | null
  published_at?: string | null
}

export interface FlowDetailResponse {
  flow_id: string
  author: string
  desc: string
  versions: FlowVersionSummary[]
}

export interface VersionView {
  flow_id: string
  version: string
  status: string
  definition: string
  layout: string
  created_at?: string | null
  published_at?: string | null
  created_by: string
  /** C5-1：草稿最近保存时间——下次保存作为乐观锁基准回传 */
  updated_at?: string | null
}

export interface NodeDescriptorView {
  node_type: string
  node_name: string
  category: string
  schema_json: string
  is_builtin: boolean
  /** 代码位置：仅内置节点有（Python 模块路径 + 类名）；控制台自定义节点为空 */
  source_module: string
  source_class: string
}

export interface PropertyTypeView {
  name: string
  base_type: string
  enum_options: unknown[]
  default_value: unknown
  desc: string
}

export interface UpsertPropertyTypeRequest {
  name: string
  base_type: string
  enum_options?: unknown[]
  default_value?: unknown
  desc?: string
}

export interface PropertyTypeListResponse {
  types: PropertyTypeView[]
  total: number
}

export interface CredentialTemplateField {
  key: string
  label: string
  input_type: 'string' | 'number'
  required: boolean
  secret: boolean
}

export interface CredentialTemplate {
  type: string
  label: string
  desc: string
  fields: CredentialTemplateField[]
}

export interface CredentialTemplatesResponse {
  templates: CredentialTemplate[]
  total: number
}

export interface NodeListResponse {
  nodes: NodeDescriptorView[]
  total: number
}

export interface RegisterNodeRequest {
  node_type: string
  node_name?: string
  category?: string
  schema_json?: string
}

export interface DryRunNodeResult {
  id?: string | null
  type?: string | null
  name?: string | null
  input?: unknown
  output?: unknown
  status: string
  error?: string | null
  /** 主/子流程层级：根层 depth=0，子流程内节点按嵌套深度递增（试跑面板子图缩进用） */
  depth?: number
  /** 自根 flow 起的标签路径，如 ["main", "c1"] */
  flow_path?: string[]
  /** 节点所属 flow 的 id（内联子流程为 null） */
  flow_id?: string | null
}

export interface DryRunResponse {
  result: unknown
  nodes: DryRunNodeResult[]
  error?: string | null
}

// ============ 基础设施类型定义 ============

export interface InfrastructureInfo {
  name: string
  display_name: string
  type: string
  enabled: boolean
  url?: string
  bootstrap_servers?: string
  status: 'healthy' | 'unhealthy' | 'disabled' | 'unknown'
  details?: Record<string, unknown>
  docker?: Record<string, unknown>
}

export interface InfrastructureListResponse {
  infrastructure: InfrastructureInfo[]
  total: number
}

export interface CreateInfrastructureRequest {
  name: string
  display_name: string
  type: string
  enabled: boolean
  url?: string
  bootstrap_servers?: string
  docker?: Record<string, unknown>
}

export interface UpdateInfrastructureRequest {
  display_name?: string
  enabled?: boolean
  url?: string
  bootstrap_servers?: string
  docker?: Record<string, unknown>
}

export interface InfrastructureTemplate {
  name: string
  display_name: string
  type: string
  url?: string
  bootstrap_servers?: string
  description: string
  docker: Record<string, unknown>
}

export interface LogStatsEntry {
  service_type: string
  instance_id?: string
  level: string
  count: number
}

export interface LogStatsResponse {
  stats: LogStatsEntry[]
  total_logs: number
}

export interface TestTemplate {
  id: string
  name: string
  description: string
  required_services: string[]
  default_params: Record<string, unknown>
}

export interface QuickTestResponse {
  success: boolean
  message: string
  execution_id?: string
  flow_definition?: Record<string, unknown>
  result?: Record<string, unknown>
  error?: string
}

// ============ 事件系统类型 ============

export interface EventInfo {
  event_id: string
  event_type: string
  data: Record<string, unknown>
  timestamp?: number
  correlation_id?: string
  source?: string
}

export interface EventListResponse {
  events: EventInfo[]
  total: number
}

export interface SubscriptionInfo {
  subscription_id: string
  event_type: string
  filter_condition?: Record<string, unknown>
  correlation_id?: string
  flow_id?: string
  node_id?: string
  created_at?: number
  timeout?: number
}

export interface SubscriptionListResponse {
  subscriptions: SubscriptionInfo[]
  total: number
}

// ============ 集群类型定义 ============

export interface ClusterInfo {
  id: string
  name: string
  description: string
  config_path: string
  redis_url: string
  created_at: string
  is_active: boolean
  /** 数据面生效 Redis URL（响应序列化层已脱敏；A2R40-01） */
  data_plane_redis_url?: string | null
  /** 数据面裁决来源：config_file | console_env | registry_metadata */
  data_plane_redis_source?: string | null
  /** 数据面错位告警（services env 冻结/派生 URL 与生效 URL 不一致，#276） */
  warnings?: string[]
}

export interface ClustersListResponse {
  clusters: ClusterInfo[]
  active_cluster_id: string | null
}

export interface CreateClusterRequest {
  id: string
  name: string
  description?: string
  redis_url?: string
  /** 架构配套预设：quickstart（快速上手）| dev（开发）| prod（生产） */
  preset?: string
}

export interface UpdateClusterRequest {
  name?: string
  description?: string
  redis_url?: string
}

export interface ClusterConfigDetail {
  cluster_id: string
  config: object
}

// ============ 调度（触发器） ============

export interface ScheduleInfo {
  schedule_id: string
  name: string
  flow_id: string
  version?: string | null
  cron: string
  params: Record<string, unknown>
  enabled: boolean
  /** 列表视图计算字段：running=已启用 / paused=已暂停 */
  status?: string
  created_at?: string
  updated_at?: string
  created_by?: string
  /** 下次触发时间（epoch 毫秒，由后端/调度服务维护） */
  next_run_at?: string | number
  last_fired_at?: string
  last_enqueue_ok?: boolean | string
}

export interface ScheduleListResponse {
  schedules: ScheduleInfo[]
  total: number
}

export interface ScheduleFireRecord {
  fired_at: string
  trigger_kind: string
  flow_id: string
  version?: string
  enqueue: string
  msg_id?: string | null
  // 本地单机档专有的诊断字段（集群档为 null）
  execution_id?: string | null
  status?: string | null
  error?: string | null
}

export interface ScheduleHistoryResponse {
  schedule_id: string
  records: ScheduleFireRecord[]
  total: number
  returned: number
  truncated: boolean
  next_offset: number | null
}



export interface CredentialItem {
  name: string
  type: string
  desc: string
  updated_at: string
}

export interface CredentialListResponse {
  credentials: CredentialItem[]
  total: number
}

export interface CredentialDetail {
  name: string
  type: string
  desc: string
  data: Record<string, unknown>
}
