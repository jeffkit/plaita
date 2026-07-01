import { useQuery } from '@tanstack/react-query'
import { Activity, Server, Play, AlertTriangle } from 'lucide-react'
import { api, ServiceListResponse, ExecutionListResponse, QueueListResponse } from '../services/api'

export default function Dashboard() {
  // 获取服务列表
  const { data: servicesData } = useQuery<ServiceListResponse>({
    queryKey: ['services'],
    queryFn: () => api.getServices(),
    refetchInterval: 5000,
  })

  // 获取执行列表
  const { data: executionsData } = useQuery<ExecutionListResponse>({
    queryKey: ['executions'],
    queryFn: () => api.getExecutions({ page: 1, size: 100 }),
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
  const runningExecutions = executions.filter(e => e.status === 'running').length
  const errorExecutions = executions.filter(e => e.status === 'error').length
  const totalQueueLength = queues.reduce((sum, q) => sum + q.length, 0)

  return (
    <div className="p-8">
      <h1 className="text-3xl font-bold mb-8">仪表盘</h1>
      
      {/* 统计卡片 */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard
          icon={<Server className="text-plaita-400" />}
          title="在线服务"
          value={onlineServices}
          total={services.length}
          color="green"
        />
        <StatCard
          icon={<Play className="text-blue-400" />}
          title="运行中执行"
          value={runningExecutions}
          total={executions.length}
          color="blue"
        />
        <StatCard
          icon={<AlertTriangle className="text-red-400" />}
          title="错误执行"
          value={errorExecutions}
          color="red"
        />
        <StatCard
          icon={<Activity className="text-yellow-400" />}
          title="队列任务"
          value={totalQueueLength}
          color="yellow"
        />
      </div>
      
      {/* 最近执行 */}
      <div className="bg-dark-800/50 rounded-xl border border-dark-700 p-6 mb-8">
        <h2 className="text-xl font-semibold mb-4">最近执行</h2>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-dark-400 text-sm border-b border-dark-700">
                <th className="text-left py-3 px-4">执行 ID</th>
                <th className="text-left py-3 px-4">流程 ID</th>
                <th className="text-left py-3 px-4">状态</th>
                <th className="text-left py-3 px-4">开始时间</th>
              </tr>
            </thead>
            <tbody>
              {executions.slice(0, 5).map((exec) => (
                <tr key={exec.execution_id} className="border-b border-dark-700/50 hover:bg-dark-700/30">
                  <td className="py-3 px-4 font-mono text-sm">{exec.execution_id.slice(0, 8)}...</td>
                  <td className="py-3 px-4">{exec.flow_id}</td>
                  <td className="py-3 px-4">
                    <StatusBadge status={exec.status} />
                  </td>
                  <td className="py-3 px-4 text-dark-400 text-sm">
                    {exec.start_time ? new Date(exec.start_time).toLocaleString() : '-'}
                  </td>
                </tr>
              ))}
              {executions.length === 0 && (
                <tr>
                  <td colSpan={4} className="py-8 text-center text-dark-400">
                    暂无执行记录
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
      
      {/* 服务状态 */}
      <div className="bg-dark-800/50 rounded-xl border border-dark-700 p-6">
        <h2 className="text-xl font-semibold mb-4">服务状态</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {services.map((service) => {
            // 获取友好的显示名称
            const displayName = (service.metadata?.display_name as string) || service.service_type
            const typeLabel = service.service_type === 'component' ? '组件' : 
                             service.service_type === 'infrastructure' ? '基础设施' : 
                             service.service_type
            return (
              <div
                key={service.instance_id}
                className="bg-dark-700/50 rounded-lg p-4 border border-dark-600"
              >
                <div className="flex items-center justify-between mb-2">
                  <span className="font-medium">{displayName}</span>
                  <StatusBadge status={service.status} />
                </div>
                <p className="text-dark-400 text-sm truncate">
                  {typeLabel}
                </p>
                <p className="text-dark-500 text-xs mt-2">
                  {service.host && service.host !== '内存' ? service.host : ''}
                </p>
              </div>
            )
          })}
          {services.length === 0 && (
            <div className="col-span-full py-8 text-center text-dark-400">
              暂无在线服务
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// 统计卡片组件
function StatCard({ 
  icon, 
  title, 
  value, 
  total,
  color 
}: { 
  icon: React.ReactNode
  title: string
  value: number
  total?: number
  color: 'green' | 'blue' | 'red' | 'yellow'
}) {
  const colorClasses = {
    green: 'bg-plaita-500/10 border-plaita-500/30',
    blue: 'bg-blue-500/10 border-blue-500/30',
    red: 'bg-red-500/10 border-red-500/30',
    yellow: 'bg-yellow-500/10 border-yellow-500/30',
  }

  return (
    <div className={`rounded-xl border p-6 ${colorClasses[color]}`}>
      <div className="flex items-center gap-4">
        <div className="p-3 rounded-lg bg-dark-800/50">
          {icon}
        </div>
        <div>
          <p className="text-dark-400 text-sm">{title}</p>
          <p className="text-2xl font-bold">
            {value}
            {total !== undefined && (
              <span className="text-dark-500 text-lg font-normal"> / {total}</span>
            )}
          </p>
        </div>
      </div>
    </div>
  )
}

// 状态徽章组件
function StatusBadge({ status }: { status: string }) {
  const statusStyles: Record<string, string> = {
    running: 'bg-plaita-500/20 text-plaita-400 border-plaita-500/30',
    completed: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    suspended: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    error: 'bg-red-500/20 text-red-400 border-red-500/30',
    stopped: 'bg-dark-500/20 text-dark-400 border-dark-500/30',
  }

  return (
    <span className={`px-2 py-1 rounded-full text-xs border ${statusStyles[status] || statusStyles.stopped}`}>
      {status}
    </span>
  )
}

