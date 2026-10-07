import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'
import { extractFlowNodes, type FlowNodeMeta } from '../components/flow/flowDefinition'

/**
 * 执行详情页共享的流程定义查询：节点时间线用它的名称/类型/配置做对照。
 *
 * **为什么要「按节点匹配版本」**：执行记录里的 `flow_version` 可能为空（观察到
 * 真实执行里就有这种），此时只能拿「最新已发布版本」对照；若实际执行的不是那个
 * 版本，节点 id 就对不上——页面上就出现一堆 `unknown`（类型/名称都取不到）。
 * 所以当版本号缺失时，这里用执行过的节点 id 去**有界探测**最近几个版本，
 * 取覆盖率最高的那个，并把「用的是哪个版本、还有几个节点没命中」如实报给 UI。
 *
 * 显式版本号则不做猜测：对不上就是真的对不上（执行期间定义被改/节点由运行时
 * 合成），由 UI 标注为「未在定义中」，不掩盖问题。
 */
export interface FlowDefinitionState {
  /** 解析后的顶层节点元信息（按定义顺序） */
  nodes: FlowNodeMeta[]
  /** 实际用于对照的版本号 */
  resolvedVersion?: string
  /** exact=用了执行记录里的版本；matched=版本缺失、按节点 id 匹配得到 */
  versionSource: 'exact' | 'matched' | 'unknown'
  /** 执行过、但不在对照定义里的节点 id（这些会显示为「未在定义中」） */
  missingIds: string[]
  isLoading: boolean
  /** 人类可读的失败原因（无流程 / 404 / 解析失败），成功时为 null */
  errorMessage: string | null
}

/** 版本缺失时最多探测几个版本（有界：避免慢后端上把页面拖死） */
const MAX_VERSION_PROBES = 3

function safeParse<T>(text: string | undefined | null, fallback: T): T {
  if (!text) return fallback
  try {
    return JSON.parse(text) as T
  } catch {
    return fallback
  }
}

function coverage(definition: string | undefined, requiredIds: string[]): number {
  if (requiredIds.length === 0) return 1
  try {
    const ids = new Set(extractFlowNodes(safeParse<Record<string, unknown> | null>(definition, null)).map((n) => n.id))
    return requiredIds.filter((id) => ids.has(id)).length / requiredIds.length
  } catch {
    return 0
  }
}

export function useFlowDefinition(
  flowId?: string,
  version?: string,
  /** 本次执行实际跑过的节点 id（`$NODE` 的键）；版本号缺失时用于匹配版本 */
  requiredIds: string[] = []
): FlowDefinitionState {
  const requiredKey = requiredIds.join(',')
  const query = useQuery({
    // 只在「版本缺失、需要按节点匹配」时把节点集合纳入 key，
    // 显式版本下避免上下文到达后重复抓同一个版本
    queryKey: ['flow-definition', flowId, version ?? 'latest', version ? '' : requiredKey],
    queryFn: async (): Promise<{ version: string; definition: string; source: 'exact' | 'matched' }> => {
      // 显式版本：直接用，不做猜测
      if (version) {
        const v = await api.getVersion(flowId!, version)
        return { version: v.version, definition: v.definition, source: 'exact' }
      }
      const detail = await api.getFlow(flowId!)
      const versions = (detail.versions || []).filter((v) => v.version)
      if (versions.length === 0) throw new Error('流程暂无任何版本')
      // 已发布优先，其次保持后端返回的「新 → 旧」顺序
      const ordered = [
        ...versions.filter((v) => v.status === 'published'),
        ...versions.filter((v) => v.status !== 'published'),
      ]
      let best: { version: string; definition: string; score: number } | null = null
      for (const candidate of ordered.slice(0, MAX_VERSION_PROBES)) {
        let picked
        try {
          picked = await api.getVersion(flowId!, candidate.version)
        } catch {
          continue // 单个版本取不到（已删/权限）不应让整块对照失败
        }
        const score = coverage(picked.definition, requiredIds)
        if (score >= 1) return { version: picked.version, definition: picked.definition, source: 'matched' }
        if (!best || score > best.score) best = { version: picked.version, definition: picked.definition, score }
      }
      if (!best) throw new Error('流程定义不可用')
      return { version: best.version, definition: best.definition, source: 'matched' }
    },
    enabled: !!flowId,
    retry: false,
    staleTime: 60_000,
  })

  const definition = useMemo(
    () => (query.data ? safeParse<Record<string, unknown> | null>(query.data.definition, null) : null),
    [query.data]
  )
  const nodes = useMemo(() => extractFlowNodes(definition), [definition])
  const missingIds = useMemo(() => {
    if (nodes.length === 0) return []
    const ids = new Set(nodes.map((n) => n.id))
    return requiredIds.filter((id) => !ids.has(id))
  }, [nodes, requiredIds])

  return {
    nodes,
    resolvedVersion: query.data?.version,
    versionSource: query.data?.source ?? 'unknown',
    missingIds,
    isLoading: query.isLoading,
    errorMessage: query.isError
      ? ((query.error as Error)?.message || '流程定义加载失败')
      : definition && nodes.length === 0
        ? '流程定义里没有可展示的节点'
        : null,
  }
}
