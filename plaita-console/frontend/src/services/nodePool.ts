/**
 * runtime pool 的节点面板过滤（plaita#300 需求4）——纯判定单源。
 *
 * 节点面板此前展示全量节点：go worker 跑不了的节点（map/while/case 等
 * Python 家族）也能被拖进画布，发布/启动才被能力门 422——报错现场远离
 * 编辑动作。本模块把「当前版本的目标 runtime 池支持哪些节点」收敛为纯
 * 函数，供 NodePalette 灰置过滤与本仓 check:node-pool-filter 负控共用。
 *
 * 语义与后端能力门（plaita-console/backend/services/capability_gate.py）
 * 对齐：
 * - 池内全部清单 ``supported_node_types`` 取**交集**（混部取最严口径）；
 * - 池内没有任何带清单的 worker → ``null``（兜底全量 + 面板提示——与门
 *   的 fail-open + 具名 WARNING 同一语义：降级可观测，不是静默放行）；
 * - 形状坏的清单跳过（与 manifest_from_metadata 同判据）。
 * python 池不过滤：console 注册表（含自定义节点）本就是 python 进程的
 * 权威面，按 worker 清单过滤反而会漏掉控制台侧自定义节点。
 */

export interface CapabilityManifest {
  engine?: string
  ir_contract_version?: string
  supported_node_types?: string[]
  unsupported_node_types?: string[]
  features?: string[]
}

/** go worker 的 engine 名（与后端 plaita.server.capability.ENGINE_GO 同值） */
export const GO_ENGINE = 'plaita-go'

/**
 * 目标引擎池的 supported 节点交集；池内没有任何带清单的 worker → ``null``
 * （调用方兜底全量并提示）。
 */
export function poolSupportedNodeTypes(
  manifests: Array<CapabilityManifest | null | undefined>,
  engine: string
): string[] | null {
  const pooled = (manifests || []).filter(
    (m): m is CapabilityManifest =>
      !!m &&
      m.engine === engine &&
      Array.isArray(m.supported_node_types) &&
      m.supported_node_types.length > 0
  )
  if (!pooled.length) return null
  let acc = new Set(pooled[0].supported_node_types!)
  for (const m of pooled.slice(1)) {
    acc = new Set([...acc].filter((t) => m.supported_node_types!.includes(t)))
  }
  return [...acc].sort()
}

/** 全量节点里目标池不支持的子集（灰置对象）；``supported=null`` → 空集。 */
export function unsupportedNodeTypes(
  nodes: Array<{ node_type: string }>,
  supported: string[] | null
): Set<string> {
  if (supported === null) return new Set()
  const pooled = new Set(supported)
  return new Set(
    nodes.map((n) => n.node_type).filter((t) => t && !pooled.has(t))
  )
}
