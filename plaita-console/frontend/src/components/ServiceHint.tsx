import { AlertTriangle, Info } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useServiceOnline } from '../hooks/useServiceOnline'

/**
 * 前置服务未运行的警示条：引擎服务没起，页面功能只是「界面壳」。
 * used by 执行实例 / 仪表盘 / 触发器等页面。
 */
export function ServiceHint({
  serviceType,
  message,
  linkTo = '/cluster',
  linkLabel = '去集群管理启动',
}: {
  serviceType: string
  message: string
  linkTo?: string
  linkLabel?: string
}) {
  const { online, isLoading, isError, localMode } = useServiceOnline(serviceType)
  // 加载中不闪警示；本地档 /services 503 是预期形态，不是故障
  if (isLoading) return null
  if (localMode) {
    return (
      <div className="flex items-center gap-2 px-3 py-2 bg-inset border border-line text-ink-muted text-caption rounded-md">
        <Info size={13} className="shrink-0" />
        <span className="min-w-0">本地单机档：流程在 console 进程内执行</span>
      </div>
    )
  }
  if (online) return null
  if (isError) {
    return (
      <div className="flex items-center gap-2 px-3 py-2 bg-status-warning-dim text-status-warning text-caption rounded-md">
        <AlertTriangle size={13} className="shrink-0" />
        <span className="min-w-0">服务状态未知：健康检查/服务列表读取失败，无法判断 {serviceType} 是否运行</span>
      </div>
    )
  }
  return (
    <div className="flex items-center gap-2 px-3 py-2 bg-status-warning-dim text-status-warning text-caption rounded-md">
      <AlertTriangle size={13} className="shrink-0" />
      <span className="min-w-0">{message}</span>
      <Link to={linkTo} className="shrink-0 underline hover:no-underline">
        {linkLabel} →
      </Link>
    </div>
  )
}
