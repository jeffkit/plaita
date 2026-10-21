/**
 * 数据面 Redis 错位警示条（A2R40-01 读面 + #276 派生面）。
 *
 * 后端 GET /clusters、/clusters/active、/clusters/{id} 与 POST /clusters/switch
 * 响应统一携带 data_plane_redis_url / data_plane_redis_source / warnings。
 * 元数据与数据面错位（或 warnings 非空）时显式警示，不得只报元数据假象；
 * 判据与来源标签在 services/clusterDataplane.ts。
 */
import { AlertTriangle } from 'lucide-react'
import { ClusterInfo } from '../services/api'
import {
  dataPlaneMismatched,
  dataPlaneNeedsNotice,
  dataPlaneSourceLabel,
} from '../services/clusterDataplane'

export default function DataPlaneNotice({ cluster }: { cluster: ClusterInfo }) {
  if (!dataPlaneNeedsNotice(cluster)) return null

  return (
    <div className="mb-4 p-4 rounded-lg bg-yellow-500/10 border border-status-warning/30">
      <div className="flex items-start gap-3">
        <div className="p-2 rounded bg-status-warning-dim">
          <AlertTriangle className="w-5 h-5 text-status-warning" />
        </div>
        <div className="min-w-0">
          <h3 className="font-medium text-status-warning mb-1">数据面 Redis 配置警示</h3>
          {dataPlaneMismatched(cluster) && (
            <p className="text-sm text-status-warning/80 break-all">
              数据面生效 URL：<code className="font-mono">{cluster.data_plane_redis_url}</code>
              （来源：{dataPlaneSourceLabel(cluster.data_plane_redis_source)}）；
              注册表元数据：<code className="font-mono">{cluster.redis_url}</code>。
            </p>
          )}
          {(cluster.warnings || []).length > 0 && (
            <ul className="text-sm text-status-warning/80 list-disc list-inside mt-1 space-y-0.5">
              {(cluster.warnings || []).map((warning, index) => (
                <li key={index} className="break-all">{warning}</li>
              ))}
            </ul>
          )}
          <p className="text-xs text-status-warning/60 mt-2">
            数据面以上述生效来源为准；注册表元数据的 Redis URL 在 plaita#65
            优先级序中仅为末位，被钉住时修改它不会改变数据面连接
            （后端以 409 cluster_redis_url_pinned 具名拒绝，文案含改面指引）。
          </p>
        </div>
      </div>
    </div>
  )
}
