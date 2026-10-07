import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'
import { extractFlowNodes, type FlowNodeMeta } from '../components/flow/flowDefinition'

/**
 * 执行详情页共享的流程定义查询：节点时间线用它的名称/类型/配置做对照。
 *
 * 取版本规则：显式 version 优先，否则取最新已发布版本。
 * loading / error **显式暴露**给调用方——旧实现里请求未回来时会先渲染
 * 「无法解析流程结构」，再闪成真实图，观感上像坏了。
 */
export interface FlowDefinitionState {
  /** 解析后的顶层节点元信息（按定义顺序） */
  nodes: FlowNodeMeta[]
  isLoading: boolean
  /** 人类可读的失败原因（无流程 / 404 / 解析失败），成功时为 null */
  errorMessage: string | null
}

function safeParse<T>(text: string | undefined | null, fallback: T): T {
  if (!text) return fallback
  try {
    return JSON.parse(text) as T
  } catch {
    return fallback
  }
}

export function useFlowDefinition(flowId?: string, version?: string): FlowDefinitionState {
  const query = useQuery({
    queryKey: ['flow-definition', flowId, version ?? 'latest'],
    queryFn: async () => {
      if (version) {
        const v = await api.getVersion(flowId!, version)
        return v.definition
      }
      const detail = await api.getFlow(flowId!)
      const versions = (detail.versions || []) as Array<{ version: string; status?: string }>
      const best = versions.find((v) => v.status === 'published') || versions[versions.length - 1]
      if (!best) throw new Error('流程暂无任何版本')
      const v = await api.getVersion(flowId!, best.version)
      return v.definition
    },
    enabled: !!flowId,
    retry: false,
    staleTime: 60_000,
  })

  const definition = useMemo(
    () => (query.data ? safeParse<Record<string, unknown> | null>(query.data, null) : null),
    [query.data]
  )
  const nodes = useMemo(() => extractFlowNodes(definition), [definition])

  return {
    nodes,
    isLoading: query.isLoading,
    errorMessage: query.isError
      ? ((query.error as Error)?.message || '流程定义加载失败')
      : definition && nodes.length === 0
        ? '流程定义里没有可展示的节点'
        : null,
  }
}
