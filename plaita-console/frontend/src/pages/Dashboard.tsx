import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { Server, Play, AlertTriangle, Activity, Inbox, ChevronRight } from 'lucide-react'
import { api, ServiceListResponse, ExecutionListResponse, QueueListResponse } from '../services/api'
import { Page, PageHeader, Card, StatCard, StatusBadge, EmptyState, Table, Th, Tr, Td, TdData } from '../components/ui'

export default function Dashboard() {
  const navigate = useNavigate()

  // 获取服务列表
  const { data: servicesData } = useQuery<ServiceListResponse>({
    queryKey: ['services'],
    queryFn: () => api.getServices(),
    refetchInterval: 5000,
  })

  // 最近执行（表格展示用）
  const { data: executionsData, isLoading: executionsLoading } = useQuery<ExecutionListResponse>({
    queryKey: ['executions'],
    queryFn: () => api.getExecutions({ page: 1, size: 10 }),
    refetchInterval: 5000,
  })

  // 运行中/错误的准确计数：走各自筛选的 total，而不是在前 100 条里客户端数
  const { data: runningData } = useQuery<ExecutionListResponse>({
    queryKey: ['executions', 'running'],
    queryFn: () => api.getExecutions({ page: 1, size: 1, status: 'running' }),
    refetchInterval: 5000,
  })
  const { data: errorData } = useQuery<ExecutionListResponse>({
    queryKey: ['executions', 'error'],
    queryFn: () => api.getExecutions({ page: 1, size: 1, status: 'error' }),
    refetchInterval: 5000,
  })

  // 获取队列状态
  const { data: queuesData } = useQuery<QueueListResponse>({
    queryKey: ['queues'],
    queryFn: () => api.getQueues(),
    refetchInterval: 5000,
  })

  const services = servicesData?.services || []
  const executions = executionsData?.executions || []
  const queues = queuesData?.queues || []

  const onlineServices = services.filter(s => s.status === 'running').length
  const runningExecutions = runningData?.total ?? executions.filter(e => e.status === 'running').length
  const errorExecutions = errorData?.total ?? 0
  const totalQueueLength = queues.reduce((sum, q) => sum + q.length, 0)

  return (
    <Page>
      <PageHeader title="仪表盘" subtitle="集群运行概览与最近执行" />

      {/* 统计卡片：数值主导，图标退后；整卡可点击下钻（DESIGN.md §5） */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          icon={<Server size={16} className="text-status-running" />}
          title="在线服务"
          value={onlineServices}
          total={services.length}
          to="/cluster"
        />
        <StatCard
          icon={<Play size={16} className="text-status-running" />}
          title="运行中执行"
          value={runningExecutions}
          to="/executions?status=running"
        />
        <StatCard
          icon={<AlertTriangle size={16} className="text-status-error" />}
          title="错误执行"
          value={errorExecutions}
          to="/executions?status=error"
        />
        <StatCard
          icon={<Activity size={16} className="text-status-pending" />}
          title="队列任务"
          value={totalQueueLength}
          to="/queues"
        />
      </div>

      {/* 最近执行：整行可点进详情 */}
      <Card className="p-4">
        <div className="flex items-baseline justify-between mb-3">
          <h2 className="text-section text-ink-primary">最近执行</h2>
          <button
            onClick={() => navigate('/executions')}
            className="text-caption text-ink-muted hover:text-plaita-400 transition-colors"
          >
            查看全部 →
          </button>
        </div>
        <Table>
          <thead>
            <tr>
              <Th>执行 ID</Th>
              <Th>流程 ID</Th>
              <Th>状态</Th>
              <Th>开始时间</Th>
              <Th />
            </tr>
          </thead>
          <tbody>
            {executionsLoading ? (
              <tr>
                <td colSpan={5}>
                  <EmptyState message="加载中…" />
                </td>
              </tr>
            ) : (
              <>
                {executions.slice(0, 5).map((exec) => (
                  <Tr
                    key={exec.execution_id}
                    className="cursor-pointer hover:bg-elevated"
                    onClick={() => navigate(`/executions/${exec.execution_id}`)}
                  >
                    <TdData>{exec.execution_id.slice(0, 8)}…</TdData>
                    <TdData>{exec.flow_id}</TdData>
                    <Td><StatusBadge status={exec.status} /></Td>
                    <TdData className="text-ink-muted">
                      {exec.start_time ? new Date(exec.start_time).toLocaleString() : '-'}
                    </TdData>
                    <Td className="text-right">
                      <ChevronRight size={14} className="inline text-ink-faint" />
                    </Td>
                  </Tr>
                ))}
                {executions.length === 0 && (
                  <tr>
                    <td colSpan={5}>
                      <EmptyState
                        icon={<Inbox size={20} />}
                        message="暂无执行记录"
                        hint="到「执行实例」页点击「启动流程」发起一次执行"
                        action={
                          <button
                            onClick={() => navigate('/executions')}
                            className="text-caption text-plaita-400 hover:underline"
                          >
                            去发起执行 →
                          </button>
                        }
                      />
                    </td>
                  </tr>
                )}
              </>
            )}
          </tbody>
        </Table>
      </Card>

      {/* 服务状态 */}
      <Card className="p-4">
        <div className="flex items-baseline justify-between mb-3">
          <h2 className="text-section text-ink-primary">服务状态</h2>
          <span className="text-data-sm text-ink-muted">{services.length}</span>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {services.map((service) => {
            // 获取友好的显示名称
            const displayName = (service.metadata?.display_name as string) || service.service_type
            const typeLabel = service.service_type === 'component' ? '组件' :
                             service.service_type === 'infrastructure' ? '基础设施' :
                             service.service_type
            return (
              <div key={service.instance_id} className="bg-inset border border-line rounded-lg p-3.5">
                <div className="flex items-center justify-between gap-2 mb-1.5">
                  <span className="text-body font-medium text-ink-primary truncate">{displayName}</span>
                  <StatusBadge status={service.status} />
                </div>
                <p className="text-caption text-ink-muted truncate">{typeLabel}</p>
                {service.host && service.host !== '内存' && (
                  <p className="mt-1.5 font-mono text-data-sm text-ink-faint truncate">{service.host}</p>
                )}
              </div>
            )
          })}
          {services.length === 0 && (
            <div className="col-span-full">
              <EmptyState icon={<Server size={20} />} message="暂无在线服务" />
            </div>
          )}
        </div>
      </Card>
    </Page>
  )
}
