import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

/**
 * 按服务类型查询是否在线（任一实例 running 即在线）。
 * 供「前置条件提示」类 UI 使用：引擎服务没起，界面功能再多也不会真正执行。
 */
export function useServiceOnline(serviceType: string) {
  const { data, isLoading } = useQuery({
    queryKey: ['services'],
    queryFn: () => api.getServices(),
    refetchInterval: 8000,
  })
  const online = (data?.services || []).some(
    (s) => s.service_type === serviceType && s.status === 'running'
  )
  return { online, isLoading }
}
