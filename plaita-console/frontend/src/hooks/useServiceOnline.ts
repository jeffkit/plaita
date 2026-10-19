import { useQuery } from '@tanstack/react-query'
import { api, apiFetch, buildHttpError } from '../services/api'

interface HealthInfo {
  status?: string
  local_mode?: boolean
}

/**
 * 按服务类型查询是否在线（任一实例 running 即在线）。
 * 供「前置条件提示」类 UI 使用：引擎服务没起，界面功能再多也不会真正执行。
 *
 * 先查免鉴权 `GET ${SUB_PATH}/health`：本地单机档（local_mode=true）没有
 * flow_worker，不能把 /services 的 503 误报成「worker 未运行」（S17-03 3B-4）。
 */
export function useServiceOnline(serviceType: string) {
  const health = useQuery({
    queryKey: ['health'],
    queryFn: async () => {
      const resp = await apiFetch('/health')
      if (!resp.ok) {
        throw buildHttpError(
          resp.status,
          await resp.text(),
          resp.headers.get('content-type'),
          resp.url,
        )
      }
      return (await resp.json()) as HealthInfo
    },
    refetchInterval: 30000,
  })
  const services = useQuery({
    queryKey: ['services'],
    queryFn: () => api.getServices(),
    refetchInterval: 8000,
    // 本地档 /services 恒 503：不是「服务坏了」，无需反复重试
    retry: false,
    enabled: health.data?.local_mode !== true,
  })
  const localMode = health.data?.local_mode === true
  const online =
    localMode ||
    (services.data?.services || []).some(
      (s) => s.service_type === serviceType && s.status === 'running',
    )
  return {
    online,
    isLoading: health.isLoading || (!localMode && services.isLoading),
    isError: health.isError || (!localMode && services.isError),
    localMode,
  }
}
