import { isRoutingNodeId, type FlowNodeMeta } from './flowDefinition'
import { computeNodeStatus } from './nodeStatusStyles'
import type { NodeStatus } from './nodeTypes'

/**
 * 执行详情页的「节点详情」组装：把三处线索合成一个可读的节点视图。
 *
 * 引擎只在 ``$NODE`` 里持久化**返回值**，不保留解析后的入参，所以"输入"分三种来源，
 * 按可信度排序并在 UI 上如实标注：
 * 1. ``trace``   —— 本地模式回调采集的真实入参（有就是准的）；
 * 2. 表达式引用  —— 流程定义里该节点字段引用的 ``$NODE.x`` / ``$INPUT.y``（引擎就是这样取值
 *    的，属于"按定义解析"，比猜上游更准）；
 * 3. ``upstream`` —— 定义里没有任何引用时的兜底：上一个已执行节点的返回值（明确标为推断）。
 */

export type NodeInputKind = 'trace' | 'node' | 'input' | 'upstream'

export interface NodeInputRef {
  kind: NodeInputKind
  /** 展示用引用名：$NODE.summary.text / $INPUT.repo / 上游返回值 summary */
  label: string
  value: unknown
  /** 引用的目标是否在本次执行里存在（未执行的节点为 false） */
  present: boolean
}

export interface ExecutedNodeDetail {
  id: string
  /** 执行序（1 起）；未执行节点为 0 */
  order: number
  /** 本次执行是否留下了该节点的记录（$NODE 键） */
  executed: boolean
  meta?: FlowNodeMeta
  status: NodeStatus
  /** 引擎合成的内部路由节点（``_n1`` 且定义里没有） */
  isRouting: boolean
  output: unknown
  hasOutput: boolean
  inputs: NodeInputRef[]
  /** 定义里的静态配置（不含连接键与子流程体） */
  config: Record<string, unknown> | null
  subflowNodes: number
  /** 本地模式采集的真实入参/出参 */
  traceInput?: unknown
  traceOutput?: unknown
}

interface RawRef {
  kind: 'node' | 'input'
  nodeId?: string
  path: string[]
}

const NODE_REF_RE = /\$NODE\.([A-Za-z_][\w]*)((?:\.[\w]+)*)/g
const INPUT_REF_RE = /\$INPUT((?:\.[\w]+)*)/g

function readPath(root: unknown, path: string[]): { value: unknown; present: boolean } {
  let cur = root
  for (const p of path) {
    if (cur && typeof cur === 'object' && p in (cur as Record<string, unknown>)) {
      cur = (cur as Record<string, unknown>)[p]
    } else {
      return { value: undefined, present: false }
    }
  }
  return { value: cur, present: true }
}

/** 递归扫描字段里的字符串，收集 ``$NODE.x`` / ``$INPUT.y`` 引用（跳过 childFlow 体内的引用） */
function collectRefs(value: unknown, out: RawRef[]): void {
  if (typeof value === 'string') {
    NODE_REF_RE.lastIndex = 0
    let m: RegExpExecArray | null
    while ((m = NODE_REF_RE.exec(value)) !== null) {
      out.push({ kind: 'node', nodeId: m[1], path: m[2] ? m[2].split('.').filter(Boolean) : [] })
    }
    INPUT_REF_RE.lastIndex = 0
    while ((m = INPUT_REF_RE.exec(value)) !== null) {
      out.push({ kind: 'input', path: m[1] ? m[1].split('.').filter(Boolean) : [] })
    }
    return
  }
  if (Array.isArray(value)) {
    value.forEach((v) => collectRefs(v, out))
    return
  }
  if (value && typeof value === 'object') {
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (k === 'childFlow') continue // 子流程内部引用的是子流程自身的 $INPUT，不是本次执行的
      collectRefs(v, out)
    }
  }
}

function refsOf(fields: Record<string, unknown> | undefined): RawRef[] {
  if (!fields) return []
  const all: RawRef[] = []
  collectRefs(fields, all)
  const seen = new Set<string>()
  return all.filter((r) => {
    const key = `${r.kind}:${r.nodeId ?? ''}:${r.path.join('.')}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

/**
 * 组装节点详情：先按 ``$NODE`` 顺序给出**已执行**节点，再补上定义里**未执行**的节点
 * （画布上点它同样要能看到输入/配置，否则点选会"什么也没有"）。
 */
export function buildNodeDetails({
  context,
  status,
  flowNodes,
  traces,
}: {
  context?: Record<string, unknown>
  status: string
  flowNodes: FlowNodeMeta[]
  traces?: Array<{ id: string; input?: unknown; output?: unknown }> | null
}): ExecutedNodeDetail[] {
  const nodeMap =
    context?.$NODE && typeof context.$NODE === 'object'
      ? (context.$NODE as Record<string, unknown>)
      : null
  const resultIds = nodeMap ? Object.keys(nodeMap) : []
  const executed = new Set(resultIds)
  const lastNodeId =
    (typeof context?.$LAST_NODE === 'string' ? (context.$LAST_NODE as string) : undefined) ??
    resultIds[resultIds.length - 1]
  const metaById = new Map(flowNodes.map((n) => [n.id, n]))
  const traceById = new Map((traces ?? []).map((t) => [t.id, t]))
  const flowInput = context?.$INPUT

  const detailFor = (id: string, order: number): ExecutedNodeDetail => {
    const meta = metaById.get(id)
    const isRouting = isRoutingNodeId(id) && !meta
    const trace = traceById.get(id)
    const hasOutput = executed.has(id)
    const inputs: NodeInputRef[] = []
    const pushInput = (ref: NodeInputRef) => {
      if (inputs.some((x) => x.label === ref.label)) return
      inputs.push(ref)
    }

    if (trace && trace.input !== undefined) {
      pushInput({ kind: 'trace', label: '实际入参（本地模式回调采集）', value: trace.input, present: true })
    }
    // start 节点收的就是流程入参
    if (meta?.type === 'start' || id === 'start') {
      pushInput({ kind: 'input', label: '$INPUT（流程入参）', value: flowInput, present: flowInput !== undefined })
    }
    for (const r of refsOf(meta?.fields)) {
      if (r.kind === 'node' && r.nodeId) {
        const { value, present } = readPath(nodeMap ?? {}, [r.nodeId, ...r.path])
        pushInput({
          kind: 'node',
          label: `$NODE.${r.nodeId}${r.path.length ? `.${r.path.join('.')}` : ''}`,
          value,
          present,
        })
      } else if (r.kind === 'input') {
        const { value, present } = readPath(flowInput, r.path)
        pushInput({
          kind: 'input',
          label: `$INPUT${r.path.length ? `.${r.path.join('.')}` : ''}`,
          value,
          present,
        })
      }
    }
    if (inputs.length === 0 && order > 0) {
      const prev = resultIds.slice(0, order - 1).reverse().find((pid) => pid !== id)
      if (prev) {
        pushInput({
          kind: 'upstream',
          label: `上游返回值 ${prev}（推断：定义未显式引用）`,
          value: nodeMap ? nodeMap[prev] : undefined,
          present: executed.has(prev),
        })
      }
    }

    return {
      id,
      order,
      executed: hasOutput,
      meta,
      status: computeNodeStatus(id, { status, lastNodeId, executed }),
      isRouting,
      output: nodeMap ? nodeMap[id] : undefined,
      hasOutput,
      inputs,
      config: meta && Object.keys(meta.fields).length > 0 ? meta.fields : null,
      subflowNodes: meta?.subflowNodes ?? 0,
      traceInput: trace?.input,
      traceOutput: trace?.output,
    }
  }

  const executedDetails = resultIds.map((id, i) => detailFor(id, i + 1))
  const pendingDetails = flowNodes
    .filter((n) => !executed.has(n.id))
    .map((n) => detailFor(n.id, 0))
  return [...executedDetails, ...pendingDetails]
}
