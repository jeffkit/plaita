import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'
import { extractFlowNodes, type FlowNodeMeta } from '../components/flow/flowDefinition'

/**
 * 执行详情页共享的流程定义查询：流程图与节点时间线各取所需，只发一次请求。
 *
 * 取版本规则与原 FlowViewer 一致：显式 version 优先，否则取最新已发布版本。
 * 关键区别是把 loading / error **显式暴露**给调用方——旧实现里请求未回来时
 * 会先渲染「无法解析流程结构」，再闪成真实图，观感上像坏了。
 */
export interface FlowDefinitionState {
  /** 解析后的顶层节点元信息（按定义顺序） */
  nodes: FlowNodeMeta[]
  /** 原始定义对象（供画布转换器使用） */
  definition: Record<string, unknown> | null
  /** 版本布局坐标（多数流程为空对象，由布局算法兜底） */
  layout: Record<string, { x: number; y: number }>
  /** 实际生效的版本号（显式传入或探测到的最新已发布版本） */
  resolvedVersion?: string
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
        return { version: v.version, definition: v.definition, layout: v.layout }
      }
      const detail = await api.getFlow(flowId!)
      const versions = (detail.versions || []) as Array<{ version: string; status?: string }>
      const best = versions.find((v) => v.status === 'published') || versions[versions.length - 1]
      if (!best) throw new Error('流程暂无任何版本')
      const v = await api.getVersion(flowId!, best.version)
      return { version: v.version, definition: v.definition, layout: v.layout }
    },
    enabled: !!flowId,
    retry: false,
    staleTime: 60_000,
  })

  const definition = useMemo(
    () => (query.data ? safeParse<Record<string, unknown> | null>(query.data.definition, null) : null),
    [query.data]
  )
  const layout = useMemo(
    () => (query.data ? safeParse<Record<string, { x: number; y: number }>>(query.data.layout, {}) : {}),
    [query.data]
  )
  const nodes = useMemo(() => extractFlowNodes(definition), [definition])

  return {
    nodes,
    definition,
    layout,
    resolvedVersion: query.data?.version,
    isLoading: query.isLoading,
    errorMessage: query.isError
      ? ((query.error as Error)?.message || '流程定义加载失败')
      : definition && nodes.length === 0
        ? '流程定义里没有可展示的节点'
        : null,
  }
}
