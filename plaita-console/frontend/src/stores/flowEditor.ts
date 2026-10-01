import { create } from 'zustand'
import type { Node, Edge, Connection, OnNodesChange, OnEdgesChange, OnConnect } from '@xyflow/react'
import { applyNodeChanges, applyEdgeChanges, addEdge } from '@xyflow/react'
import { jsonToFlow, flowToJson, type FlowMeta, type FlowNodeData } from '../components/flow/flowConverter'
import { EDGE_COLOR, EDGE_TYPE, nodeHeightFor } from '../components/flow/flowLayout'
import { normalizeFieldKeys } from '../components/flow/schemaForm/schemaUtils'

/** 编辑栈中的一层：进入子图时暂存的父图状态 */
export interface GraphFrame {
  /** 面包屑标题，如「map · 处理订单」 */
  title: string
  /** 父图中承载子图的节点 id */
  nodeId: string
  kind: 'child_flow' | 'branch'
  /** kind === 'branch' 时的分支下标（parallel branches[i].flow） */
  branchIndex?: number
  nodes: Node[]
  edges: Edge[]
  selectedNodeId: string | null
}

export interface FlowEditorState {
  flowId: string
  version: string
  meta: FlowMeta
  nodes: Node[]
  edges: Edge[]
  selectedNodeId: string | null
  dirty: boolean
  /** 子图编辑栈：空 = 主图；非空 = 栈顶为当前编辑层，nodes/edges 即栈顶内容 */
  graphStack: GraphFrame[]
  /** 退出子图时的结构校验提示（如缺 start/end） */
  subgraphWarning: string | null
  /** 当前 flow 定义是否带 @flow 源码（metadata.source）——节点详情「查看源码」按钮的开关 */
  hasFlowSource: boolean
  /** 节点详情发起的「跳源码第 N 行」请求；FlowEditor 消费后置回 null */
  sourceLineRequest: number | null

  setFlowContext: (flowId: string, version: string, meta: FlowMeta) => void
  setGraph: (nodes: Node[], edges: Edge[]) => void
  onNodesChange: OnNodesChange
  onEdgesChange: OnEdgesChange
  onConnect: OnConnect
  addNode: (node: Node) => void
  updateNodeData: (id: string, data: Partial<Record<string, unknown>>) => void
  removeNode: (id: string) => void
  setSelected: (id: string | null) => void
  markDirty: () => void
  enterSubgraph: (nodeId: string, kind: 'child_flow' | 'branch', branchIndex?: number) => void
  /** 方案 A：循环族节点原位容器展开/收拢（视图态，不进 IR；子节点编辑双写回 childFlow） */
  toggleSubflowExpanded: (nodeId: string) => void
  exitSubgraph: () => void
  /** 归位到指定层（0 = 主图） */
  exitToLevel: (level: number) => void
  /** 试跑结果标记：出错节点写 status=error、其余清除——不置 dirty（运行态不是编辑内容） */
  setRunErrorNodes: (ids: string[]) => void
  reset: () => void
}

// map 族子流程的元素注入契约：每个元素以 item/index 进入子流程
const ITEM_INDEX_INPUT = {
  inputType: {
    dataType: 'object',
    properties: {
      item: { dataType: 'any', label: '元素' },
      index: { dataType: 'integer', label: '索引' },
    },
  },
}

function seedSubflowJson(nodeType: string): Record<string, unknown> {
  const base: Record<string, unknown> = {
    nodes: [
      { type: 'start', id: 'start', name: 'start' },
      { type: 'end', id: 'end', name: 'end' },
    ],
  }
  if (['map', 'loop', 'filter', 'find', 'reduce'].includes(nodeType)) {
    return { ...ITEM_INDEX_INPUT, ...base }
  }
  return base
}

function subflowJsonOf(
  fields: Record<string, unknown>,
  kind: 'child_flow' | 'branch',
  branchIndex?: number,
): Record<string, unknown> | undefined {
  if (kind === 'branch') {
    const branches = (fields.branches as Array<Record<string, unknown>>) || []
    const raw = branches[branchIndex ?? -1]?.flow
    if (raw === undefined) return undefined
    return typeof raw === 'string' ? (JSON.parse(raw) as Record<string, unknown>) : (raw as Record<string, unknown>)
  }
  const raw = fields.child_flow
  if (raw === undefined) return undefined
  return typeof raw === 'string' ? (JSON.parse(raw) as Record<string, unknown>) : (raw as Record<string, unknown>)
}

export const useFlowEditor = create<FlowEditorState>((set, get) => ({
  flowId: '',
  version: '',
  meta: {},
  nodes: [],
  edges: [],
  selectedNodeId: null,
  dirty: false,
  graphStack: [],
  subgraphWarning: null,
  hasFlowSource: false,
  sourceLineRequest: null,

  setFlowContext: (flowId, version, meta) => set({ flowId, version, meta }),

  setGraph: (nodes, edges) => set({ nodes, edges, dirty: false }),

  // 选中/尺寸变化是 xyflow 的交互噪音，不算「未保存」；
  // 只有增删节点、改位置、改连线才置 dirty，否则唯一的状态指示器会失去公信力
  onNodesChange: (changes) => {
    const meaningful = changes.some(
      (c) => c.type !== 'select' && c.type !== 'dimensions'
    )
    set((s) => ({
      nodes: applyNodeChanges(changes, s.nodes) as Node[],
      dirty: meaningful ? true : s.dirty,
    }))
  },

  onEdgesChange: (changes) => {
    const meaningful = changes.some((c) => c.type !== 'select')
    set((s) => ({
      edges: applyEdgeChanges(changes, s.edges) as Edge[],
      dirty: meaningful ? true : s.dirty,
    }))
  },

  onConnect: (connection: Connection) =>
    set((s) => ({
      edges: addEdge(
        { ...connection, type: EDGE_TYPE, style: { stroke: EDGE_COLOR } },
        s.edges
      ) as Edge[],
      dirty: true,
    })),

  addNode: (node) => set((s) => ({ nodes: [...s.nodes, node], dirty: true })),

  updateNodeData: (id, data) =>
    set((s) => {
      let nodes = s.nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, ...data } } : n
      )
      // 容器展开态的子节点（id 形如 owner::childId）：参数编辑双写回
      // owner 的 childFlow/child_flow.nodes——「编辑存在于 childflow，
      // 不产生任何新的内容」（不新增顶层节点/版本结构）
      const sep = id.lastIndexOf('::')
      if (sep > 0) {
        const ownerId = id.slice(0, sep)
        const childId = id.slice(sep + 2)
        nodes = nodes.map((n) => {
          if (n.id !== ownerId) return n
          const d = n.data as FlowNodeData
          const fields = { ...(d.fields ?? {}) } as Record<string, unknown>
          const cfKey = fields.childFlow ? 'childFlow' : 'child_flow'
          const cf = fields[cfKey] as { nodes?: Array<Record<string, unknown>> } | undefined
          if (!cf?.nodes) return n
          const cfNodes = cf.nodes.map((ir) => {
            if (ir.id !== childId) return ir
            const merged = { ...ir }
            if (data.name !== undefined) merged.name = data.name
            for (const [k, v] of Object.entries(data.fields ?? {})) merged[k] = v
            return merged
          })
          fields[cfKey] = { ...cf, nodes: cfNodes }
          return { ...n, data: { ...d, fields } }
        })
      }
      return { nodes, dirty: true }
    }),

  toggleSubflowExpanded: (nodeId) => {
    const s = get()
    const node = s.nodes.find((n) => n.id === nodeId)
    if (!node) return
    const d = node.data as FlowNodeData

    // ---- 收拢：移除本容器展开出的全部子节点/子边，还原原子卡片 ----
    if (d.expanded) {
      const prefix = nodeId + '::'
      const childIds = new Set(s.nodes.filter((n) => n.id.startsWith(prefix)).map((n) => n.id))
      set({
        nodes: s.nodes
          .filter((n) => !n.id.startsWith(prefix))
          .map((n) =>
            n.id === nodeId
              ? { ...n, data: { ...d, expanded: false }, style: undefined, extent: undefined }
              : n
          ),
        edges: s.edges.filter((e) => !childIds.has(e.source) && !childIds.has(e.target)),
      })
      return
    }

    // ---- 展开：childFlow/child_flow -> 容器内真节点（纵向栈布局） ----
    const fields = (d.fields ?? {}) as Record<string, unknown>
    const cf = (fields.childFlow || fields.child_flow) as
      | { runtime?: string; inputType?: unknown; nodes?: Array<Record<string, unknown>> }
      | undefined
    if (!cf?.nodes?.length) return
    const childDef = {
      runtime: cf.runtime || 'python',
      flow_id: nodeId,
      inputType: cf.inputType ?? { dataType: 'object' },
      nodes: cf.nodes,
    }
    const { nodes: cn, edges: ce } = jsonToFlow(
      childDef as Record<string, unknown>,
      {},
      {},
      { parentId: nodeId }
    )
    // 纵向栈布局（循环体通常 1-3 节点的线性链；分支按 IR 序排布）。
    // 首个节点从容器标题栏（48px）之下起排，避免被 header 覆盖。
    let y = 56
    const laidOut = cn.map((n) => {
      const h = nodeHeightFor(n)
      const withPos = { ...n, position: { x: 20, y } }
      y += h + 24
      return withPos
    })
    const containerW = 240 + 40
    const containerH = 48 + y + 6
    const childNodes = laidOut.map((n) => ({
      ...n,
      parentId: nodeId,
      extent: 'parent' as const,
      connectable: false,
      deletable: false,
    }))
    set({
      nodes: s.nodes
        .concat(childNodes)
        .map((n) =>
          n.id === nodeId
            ? {
                ...n,
                data: { ...(n.data as FlowNodeData), expanded: true, containerW, containerH },
                style: { width: containerW, height: containerH },
              }
            : n
        ),
      edges: s.edges.concat(ce),
    })
  },

  setRunErrorNodes: (ids) =>
    set((s) => ({
      nodes: s.nodes.map((n) => {
        const isErr = ids.includes(n.id)
        const cur = (n.data as Record<string, unknown>).status
        if (isErr && cur !== 'error') return { ...n, data: { ...n.data, status: 'error' } }
        if (!isErr && cur === 'error') return { ...n, data: { ...n.data, status: 'idle' } }
        return n
      }),
    })),

  removeNode: (id) =>
    set((s) => ({
      nodes: s.nodes.filter((n) => n.id !== id),
      edges: s.edges.filter((e) => e.source !== id && e.target !== id),
      selectedNodeId: s.selectedNodeId === id ? null : s.selectedNodeId,
      dirty: true,
    })),

  setSelected: (id) => set({ selectedNodeId: id }),

  markDirty: () => set({ dirty: true }),

  enterSubgraph: (nodeId, kind, branchIndex) => {
    const s = get()
    const node = s.nodes.find((n) => n.id === nodeId)
    if (!node) return
    const d = node.data as FlowNodeData

    // 先归一别名键（childFlow→child_flow 等，固定映射无 schema 也安全），
    // 避免旧键残留导致子图读取落空、写回后双键并存
    const fields = normalizeFieldKeys(d.fields)
    if (fields !== d.fields) {
      const normalizedNodes = s.nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...d, fields } } : n
      )
      set({ nodes: normalizedNodes, dirty: true })
    }

    let flowJson = subflowJsonOf(fields, kind, branchIndex)
    let seeded = false
    if (!flowJson || !Array.isArray(flowJson.nodes) || flowJson.nodes.length === 0) {
      flowJson = seedSubflowJson(d.type)
      seeded = true
    }

    const { nodes: subNodes, edges: subEdges } = jsonToFlow(flowJson, {})
    // frame 必须暂存归一化写回后的最新父图（get() 重新取），否则退出时
    // 会基于旧 fields 合并，导致别名键残留、子图既有顶层键丢失
    const frame: GraphFrame = {
      title: `${d.type}${d.name && d.name !== d.type ? ` · ${d.name}` : ''}`,
      nodeId,
      kind,
      branchIndex,
      nodes: get().nodes,
      edges: get().edges,
      selectedNodeId: get().selectedNodeId,
    }
    set({
      graphStack: [...s.graphStack, frame],
      nodes: subNodes as Node[],
      edges: subEdges as Edge[],
      selectedNodeId: null,
      subgraphWarning: null,
      dirty: s.dirty || seeded,
    })
  },

  exitSubgraph: () => {
    const s = get()
    const frame = s.graphStack[s.graphStack.length - 1]
    if (!frame) return
    const def = flowToJson(s.nodes as Node<FlowNodeData>[], s.edges, {})
    const subNodes = (def.nodes as Array<Record<string, unknown>>) || []

    // 把编辑后的子图写回父图对应节点的 child_flow / branches[i].flow
    // （保留子 Flow 的 inputType 等既有顶层键，仅替换 nodes 与连线推导字段）
    const parentNodes = frame.nodes.map((n) => {
      if (n.id !== frame.nodeId) return n
      const d = n.data as FlowNodeData
      const fields = { ...d.fields }
      if (frame.kind === 'branch') {
        const branches = [...((fields.branches as Array<Record<string, unknown>>) || [])]
        const bi = frame.branchIndex ?? -1
        if (branches[bi]) {
          const existing = (branches[bi].flow as Record<string, unknown>) ?? {}
          branches[bi] = { ...branches[bi], flow: { ...existing, nodes: subNodes } }
        }
        fields.branches = branches
      } else {
        const existing = (fields.child_flow as Record<string, unknown>) ?? {}
        fields.child_flow = { ...existing, nodes: subNodes }
      }
      return { ...n, data: { ...d, fields } }
    })

    const hasStart = subNodes.some((nd) => nd.type === 'start')
    const hasEnd = subNodes.some((nd) => nd.type === 'end')
    const warning =
      hasStart && hasEnd
        ? null
        : `子流程「${frame.title}」缺少 ${!hasStart ? 'start' : ''}${
            !hasStart && !hasEnd ? ' 和 ' : ''
          }${!hasEnd ? 'end' : ''} 节点，保存后端校验会失败`

    set({
      nodes: parentNodes,
      edges: frame.edges,
      selectedNodeId: frame.selectedNodeId,
      graphStack: s.graphStack.slice(0, -1),
      subgraphWarning: warning,
      dirty: true,
    })
  },

  exitToLevel: (level) => {
    const s = get()
    let guard = s.graphStack.length
    while (get().graphStack.length > Math.max(0, level) && guard-- > 0) {
      get().exitSubgraph()
    }
  },

  reset: () =>
    set({
      flowId: '',
      version: '',
      meta: {},
      nodes: [],
      edges: [],
      selectedNodeId: null,
      dirty: false,
      graphStack: [],
      subgraphWarning: null,
      hasFlowSource: false,
      sourceLineRequest: null,
    }),
}))
