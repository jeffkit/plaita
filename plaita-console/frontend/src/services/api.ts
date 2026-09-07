/**
 * API 服务层
 */

const API_BASE = '/api'

/** 管理面 API Key：优先 localStorage，其次 Vite 构建期环境变量。 */
function getAdminApiKey(): string {
  try {
    const fromStore = localStorage.getItem('plaita_admin_api_key')
    if (fromStore) return fromStore
  } catch {
    /* SSR / 隐私模式 */
  }
  return (import.meta as { env?: Record<string, string> }).env?.VITE_PLAITA_ADMIN_API_KEY || ''
}

// 会话 token（RBAC 登录后保存）
export function getToken(): string | null {
  return localStorage.getItem('plaita_token')
}

export function setSession(token: string, username: string, role: string): void {
  localStorage.setItem('plaita_token', token)
  localStorage.setItem('plaita_username', username)
  localStorage.setItem('plaita_role', role)
}

export function clearSession(): void {
  localStorage.removeItem('plaita_token')
  localStorage.removeItem('plaita_username')
  localStorage.removeItem('plaita_role')
}

export function getRole(): string {
  return localStorage.getItem('plaita_role') || 'viewer'
}

// 通用请求函数
async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options?.headers as Record<string, string> | undefined),
  }
  const token = getToken()
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  } else {
    const adminKey = getAdminApiKey()
    if (adminKey) headers['X-Admin-API-Key'] = adminKey
  }

  const response = await fetch(`${API_BASE}${url}`, {
    ...options,
    headers,
  })

  if (response.status === 401 && !url.startsWith('/auth/login') && !url.startsWith('/auth/setup')) {
    clearSession()
    window.location.assign('/login')
    throw new Error('登录已过期，请重新登录')
  }

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: '请求失败' }))
    throw new Error(error.detail || '请求失败')
  }

  return response.json()
}

// ============ 类型定义 ============

export interface ServiceInfo {
  instance_id: string
  service_type: string
  host: string
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
  tasks: QueueTask[]
}

export interface LogEntry {
  timestamp: string
  level: string
  service_type?: string
  instance_id?: string
  message: string
  context?: Record<string, unknown>
}

export interface LogListResponse {
  logs: LogEntry[]
  total: number
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
  }): Promise<void> {
    return request('/executions', {
      method: 'POST',
      body: JSON.stringify(params),
    })
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
    })
  },

  async stopManagedService(instanceId: string, graceful = true): Promise<void> {
    return request(`/cluster/stop/${instanceId}`, {
      method: 'POST',
      body: JSON.stringify({ graceful }),
    })
  },

  async stopAllManagedServices(graceful = true): Promise<void> {
    return request('/cluster/stop-all', {
      method: 'POST',
      body: JSON.stringify({ graceful }),
    })
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
    })
  },

  async removeInstance(instanceId: string): Promise<void> {
    return request(`/cluster/instances/${instanceId}`, {
      method: 'DELETE',
    })
  },

  async clearFailedInstances(): Promise<{ cleared_count: number }> {
    return request('/cluster/instances', {
      method: 'DELETE',
    })
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
    })
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
    return request(`/cluster/infrastructure/${name}/start`, { method: 'POST' })
  },

  async stopInfrastructure(name: string): Promise<{ success: boolean; message: string }> {
    return request(`/cluster/infrastructure/${name}/stop`, { method: 'POST' })
  },

  async checkInfrastructureHealth(name: string): Promise<{
    name: string
    status: string
    details: unknown
    checked_at: string
  }> {
    return request(`/cluster/infrastructure/${name}/check`, {
      method: 'POST',
    })
  },

  async createInfrastructure(data: CreateInfrastructureRequest): Promise<InfrastructureInfo> {
    return request('/cluster/infrastructure', {
      method: 'POST',
      body: JSON.stringify(data),
    })
  },

  async updateInfrastructure(name: string, data: UpdateInfrastructureRequest): Promise<InfrastructureInfo> {
    return request(`/cluster/infrastructure/${name}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    })
  },

  async deleteInfrastructure(name: string): Promise<void> {
    return request(`/cluster/infrastructure/${name}`, {
      method: 'DELETE',
    })
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

  async saveVersion(
    flowId: string,
    version: string,
    payload: { definition: string; layout: string; created_by?: string }
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
    })
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
  }> {
    return request('/auth/login', { method: 'POST', body: JSON.stringify({ username, password }) })
  },

  async me(): Promise<{ actor: string; role: string }> {
    return request('/auth/me')
  },

  async logout(): Promise<{ success: boolean }> {
    return request('/auth/logout', { method: 'POST' })
  },

  async getUsers(): Promise<{ users: Array<{ username: string; role: string; disabled: boolean; created_at: string }> }> {
    return request('/users')
  },

  async createUser(payload: { username: string; password: string; role: string }): Promise<{ success: boolean }> {
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

  async triggerSchedule(scheduleId: string): Promise<{ success: boolean; msg_id: string }> {
    return request(`/schedules/${scheduleId}/trigger`, { method: 'POST' })
  },

  async getScheduleHistory(
    scheduleId: string,
    limit = 10
  ): Promise<{ schedule_id: string; records: ScheduleFireRecord[]; total: number }> {
    return request(`/schedules/${scheduleId}/history?limit=${limit}`)
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
  msg_id?: string
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
