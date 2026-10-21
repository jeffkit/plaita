/**
 * 数据面 Redis 读面纯函数（A2R40-01 / #276）。
 *
 * redis_url 是注册表元数据——在 plaita#65 优先级序（配置文件具体地址 >
 * PLAITA_CONSOLE_REDIS_URL > 配置占位 > 元数据）里仅为末位；
 * data_plane_redis_url / data_plane_redis_source 才是数据面真相（响应
 * 序列化层已脱敏）。警示条（components/DataPlaneNotice.tsx）与集群切换器
 * 共用本模块的标签与判据。
 */
import { ClusterInfo } from './api'

/** 生效来源的人读标签（与后端 _PIN_SOURCE_LABELS 同文案 + 元数据档）。 */
export const DATA_PLANE_SOURCE_LABELS: Record<string, string> = {
  config_file: '集群配置文件 redis.url',
  console_env: '环境变量 PLAITA_CONSOLE_REDIS_URL',
  registry_metadata: '注册表元数据（plaita#65 优先级序末位）',
}

export function dataPlaneSourceLabel(source?: string | null): string {
  if (source && DATA_PLANE_SOURCE_LABELS[source]) {
    return DATA_PLANE_SOURCE_LABELS[source]
  }
  return source || '未知来源'
}

/** 元数据与数据面是否错位（redis_url ≠ 生效 URL）。 */
export function dataPlaneMismatched(cluster: ClusterInfo): boolean {
  return Boolean(cluster.data_plane_redis_url)
    && cluster.redis_url !== cluster.data_plane_redis_url
}

/** 数据面是否需要警示（错位，或后端 warnings 非空）。 */
export function dataPlaneNeedsNotice(cluster: ClusterInfo): boolean {
  return dataPlaneMismatched(cluster) || (cluster.warnings || []).length > 0
}

/** 错位/告警摘要（切换器行级 tooltip 与页面警示条共用）。 */
export function dataPlaneNoticeText(cluster: ClusterInfo): string {
  const parts: string[] = []
  if (dataPlaneMismatched(cluster)) {
    parts.push(
      `数据面生效 URL ${cluster.data_plane_redis_url}` +
        `（来源：${dataPlaneSourceLabel(cluster.data_plane_redis_source)}），` +
        `与注册表元数据 ${cluster.redis_url} 不一致`,
    )
  }
  for (const warning of cluster.warnings || []) parts.push(warning)
  return parts.join('；')
}
