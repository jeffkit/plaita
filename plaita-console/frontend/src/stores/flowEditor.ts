import { create } from 'zustand'
import type { Node, Edge, Connection, OnNodesChange, OnEdgesChange, OnConnect } from '@xyflow/react'
import { applyNodeChanges, applyEdgeChanges, addEdge } from '@xyflow/react'
import { jsonToFlow, flowToJson, type FlowMeta, type FlowNodeData } from '../components/flow/flowConverter'
import { EDGE_COLOR, EDGE_TYPE } from '../components/flow/flowLayout'
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
    set((s) => ({
      nodes: s.nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, ...data } } : n
      ),
      dirty: true,
    })),

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
    }),
}))
