import { AlertTriangle } from 'lucide-react'
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
  const { online } = useServiceOnline(serviceType)
  if (online) return null
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
