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
  /** 撤销/重做栈（2026-10 表单评审）：画布与表单编辑历史；载入版本时清空 */
  past: Array<{ nodes: Node[]; edges: Edge[] }>
  future: Array<{ nodes: Node[]; edges: Edge[] }>
  /** 调色板「点击添加」入口：由 FlowCanvas 注册（需要 useReactFlow 换算视口坐标） */
  addNodeFromPalette: ((nodeType: string, name: string) => void) | null

  setFlowContext: (flowId: string, version: string, meta: FlowMeta) => void
  setGraph: (nodes: Node[], edges: Edge[]) => void
  /** 文档级替换但可撤销（C5-2/C5-4）：当前图先压入撤销栈再整体替换，
   *  不清空历史——AI 整图应用、自动布局走这里；调用方随后自行 markDirty */
  replaceGraph: (nodes: Node[], edges: Edge[]) => void
  undo: () => void
  redo: () => void
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

// ── 撤销/重做（2026-10 表单评审）────────────────────────────────────────
const HISTORY_LIMIT = 50
let lastPushAt = 0
let lastPushNodeId = ''
type HistSnap = { nodes: Node[]; edges: Edge[] }
/** 变更前快照入栈，并作废重做栈 */
function pushHist(s: { nodes: Node[]; edges: Edge[]; past: HistSnap[] }): Pick<FlowEditorState, 'past' | 'future'> {
  return {
    past: [...s.past, { nodes: s.nodes, edges: s.edges }].slice(-HISTORY_LIMIT),
    future: [],
  }
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

// 容器子节点 IR 写回时保留的连接/元字段：jsonToFlow 把它们从 fields 剥离到
// 画布节点本体（data.type/name/next 等），画布 fields 不携带，重建 IR 时取 IR 原值。
// source_line/desc 等簿记键本就在画布 fields 里，随全量覆盖自然保留/更新。
const IR_KEEP_KEYS = new Set(['id', 'type', 'name', 'next', 'else_next'])

// ── 容器子节点 → owner 链写回（2026-10 评审 MC1）────────────────────────
// 显式打标（data.isContainerChild/ownerId，jsonToFlow 注入）取代
// 「id.lastIndexOf('::')」字符串启发式：手写 IR 顶层 id 恰好含 :: 不再误触发。
/** 把容器子节点 canvasChild 的最新画布形态重建为 owner childFlow IR 条目：
 *  簿记键取 IR 原值，业务字段以画布 fields 全量覆盖（与 E2 写回逻辑同源），
 *  连接字段（next/else_next/branches[].next）由父层画布边回填——画布边是
 *  连接拓扑的唯一推导来源（fields.branches 被 jsonToFlow 剥掉 next）。 */
function irEntryFromCanvasChild(
  ir: Record<string, unknown>,
  child: Node,
  edges: Edge[],
  ownerId: string,
): Record<string, unknown> {
  const cd = child.data as FlowNodeData
  const merged: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(ir)) {
    if (IR_KEEP_KEYS.has(k)) merged[k] = v
  }
  for (const [k, v] of Object.entries(cd.fields ?? {})) merged[k] = v
  // name 回填：IR 有名 → 取画布名（改名传播）；IR 无名且画布名仍是 jsonToFlow
  // 的合成兜底（'owner::裸id'）→ 不写，避免把合成名污染回 IR
  const prefix = ownerId + '::'
  const bare = (x: string) => (x.startsWith(prefix) ? x.slice(prefix.length) : x)
  if (ir.name !== undefined || cd.name !== child.id) merged.name = cd.name
  // 连接字段回填：target 去掉 owner 前缀还原裸 id（与 flowToJson 的边推导一致）
  const outEdges = edges.filter((e) => e.source === child.id)
  if (cd.type === 'if') {
    delete merged.next
    delete merged.else_next
    for (const e of outEdges) {
      if (e.sourceHandle === 'false') merged.else_next = bare(e.target)
      else merged.next = bare(e.target)
    }
  } else if (cd.type === 'switch' || cd.type === 'case') {
    const branches = (Array.isArray(merged.branches) ? merged.branches : []) as Array<
      Record<string, unknown>
    >
    const resolved = branches.map((b) => ({ ...b }))
    for (const e of outEdges) {
      const idx = e.sourceHandle ? resolved.findIndex((b) => b.name === e.sourceHandle) : -1
      if (idx >= 0) resolved[idx].next = bare(e.target)
    }
    if (branches.length > 0) merged.branches = resolved
  } else if (outEdges.length > 0) {
    merged.next = bare(outEdges[0].target)
  } else {
    delete merged.next
  }
  return merged
}

/** 把容器子节点 childId 的编辑沿 owner 链逐层镜像：每层 owner 的 childFlow IR
 *  都更新到以最内层画布为真相源的最新值（方案 Y 下通常只有一层，链式上溯是
 *  对「owner 自身也是容器子节点」的兜底，保证任何层收拢/保存都不丢编辑）。 */
function syncContainerChildUp(nodes: Node[], edges: Edge[], childId: string): Node[] {
  const child = nodes.find((n) => n.id === childId)
  if (!child) return nodes
  const cd = child.data as FlowNodeData
  if (!cd.isContainerChild || !cd.ownerId) return nodes
  const ownerId = cd.ownerId
  const owner = nodes.find((n) => n.id === ownerId)
  if (!owner) return nodes
  const bareId = childId.startsWith(ownerId + '::') ? childId.slice(ownerId.length + 2) : childId
  const od = owner.data as FlowNodeData
  const fields = { ...(od.fields ?? {}) } as Record<string, unknown>
  const cfKey = fields.childFlow ? 'childFlow' : 'child_flow'
  const cf = fields[cfKey] as { nodes?: Array<Record<string, unknown>> } | undefined
  if (!cf?.nodes) return nodes
  let hit = false
  const cfNodes = cf.nodes.map((ir) => {
    if (ir.id !== bareId) return ir
    hit = true
    return irEntryFromCanvasChild(ir, child, edges, ownerId)
  })
  if (!hit) return nodes
  fields[cfKey] = { ...cf, nodes: cfNodes }
  const nextNodes = nodes.map((n) =>
    n.id === ownerId ? { ...n, data: { ...od, fields } } : n
  )
  return syncContainerChildUp(nextNodes, edges, ownerId)
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
  past: [],
  future: [],
  addNodeFromPalette: null,

  setFlowContext: (flowId, version, meta) => set({ flowId, version, meta }),

  // 载入/切换版本是文档级替换：历史失去前置语义，清空两栈
  setGraph: (nodes, edges) => set({ nodes, edges, dirty: false, past: [], future: [] }),

  // 文档级替换但保留历史（C5-2/C5-4）：与 setGraph 的差异只在历史栈——
  // 当前图入撤销栈、重做栈作废，替换本身可被 Cmd+Z 撤销
  replaceGraph: (nodes, edges) =>
    set((s) => ({ nodes, edges, ...pushHist(s) })),

  // 撤销/重做在子图视图内禁用：历史快照是「某编辑层的整图」，跨层回退会造成
  // 画布内容与面包屑层级错位。禁用比猜层级安全。
  undo: () => {
    const s = get()
    if (s.past.length === 0 || s.graphStack.length > 0) return
    const prev = s.past[s.past.length - 1]
    lastPushAt = 0 // 撤销后重置合并窗口，下一次编辑从新的一步开始
    set({
      nodes: prev.nodes,
      edges: prev.edges,
      past: s.past.slice(0, -1),
      future: [...s.future, { nodes: s.nodes, edges: s.edges }].slice(-HISTORY_LIMIT),
      dirty: true,
    })
  },

  redo: () => {
    const s = get()
    if (s.future.length === 0 || s.graphStack.length > 0) return
    const next = s.future[s.future.length - 1]
    lastPushAt = 0
    set({
      nodes: next.nodes,
      edges: next.edges,
      future: s.future.slice(0, -1),
      past: [...s.past, { nodes: s.nodes, edges: s.edges }].slice(-HISTORY_LIMIT),
      dirty: true,
    })
  },

  // 选中/尺寸变化是 xyflow 的交互噪音，不算「未保存」；
  // 只有增删节点、改位置、改连线才置 dirty，否则唯一的状态指示器会失去公信力
  onNodesChange: (changes) => {
    const meaningful = changes.some(
      (c) => c.type !== 'select' && c.type !== 'dimensions'
    )
    // 拖拽中的 position 微步（dragging:true 批次）不进历史，落点批次
    // （dragging:false）或增删才记一步——否则一次拖拽刷出几十条撤销步
    const dragTail = changes.some(
      (c) => c.type === 'position' && (c as { dragging?: boolean }).dragging === false,
    )
    const structural = changes.some((c) => c.type === 'add' || c.type === 'remove')
    const record = meaningful && (structural || dragTail)
    set((s) => ({
      nodes: applyNodeChanges(changes, s.nodes) as Node[],
      dirty: meaningful ? true : s.dirty,
      ...(record ? pushHist(s) : {}),
    }))
  },

  onEdgesChange: (changes) => {
    const meaningful = changes.some((c) => c.type !== 'select')
    set((s) => ({
      edges: applyEdgeChanges(changes, s.edges) as Edge[],
      dirty: meaningful ? true : s.dirty,
      ...(meaningful ? pushHist(s) : {}),
    }))
  },

  onConnect: (connection: Connection) =>
    set((s) => ({
      edges: addEdge(
        { ...connection, type: EDGE_TYPE, style: { stroke: EDGE_COLOR } },
        s.edges
      ) as Edge[],
      dirty: true,
      ...pushHist(s),
    })),

  addNode: (node) =>
    set((s) => ({ nodes: [...s.nodes, node], dirty: true, ...pushHist(s) })),

  updateNodeData: (id, data) =>
    set((s) => {
      // 表单抽屉逐键写回：600ms 内对同一节点的连续编辑合并为一步历史
      const now = Date.now()
      const coalesce = now - lastPushAt < 600 && lastPushNodeId === id
      lastPushAt = now
      lastPushNodeId = id
      let nodes = s.nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, ...data } } : n
      )
      // 容器展开态的子节点（jsonToFlow 显式打标 isContainerChild/ownerId）：
      // 参数编辑沿 owner 链逐层镜像写回各层 childFlow IR——「编辑存在于
      // childflow，不产生任何新的内容」（不新增顶层节点/版本结构）。
      // 逐层镜像保证 owner 自身也在容器内时（嵌套兜底），任何一层收拢/保存
      // 都不丢编辑（2026-10 评审 MC1）；真相源=本节点画布 data。
      const target = nodes.find((n) => n.id === id)
      const td = target?.data as FlowNodeData | undefined
      if (td?.isContainerChild && td.ownerId) {
        nodes = syncContainerChildUp(nodes, s.edges, id)
      }
      return { nodes, dirty: true, ...(!coalesce ? pushHist(s) : {}) }
    }),

  toggleSubflowExpanded: (nodeId) => {
    const s = get()
    const node = s.nodes.find((n) => n.id === nodeId)
    if (!node) return
    const d = node.data as FlowNodeData
    // 方案 Y（MC1）：容器子节点禁止再原位展开——嵌套容器的编辑写回极易丢层，
    // 且容器高度/布局只按单层设计。嵌套体编辑统一走「进入子图编辑」路径
    // （exitSubgraph 已沿 owner 链写回），节点画布不渲染 [⊞]，此处兜底守卫
    if (d.isContainerChild) return

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

  removeNode: (id) => {
    const s = get()
    // 容器子节点不可经此删除（MC2）：画布删除无法写回 owner 的 childFlow IR，
    // 收拢/保存后节点会「复活」。删除子流程节点请进入子图编辑（写回落 IR）
    // 或收拢容器后在对应层操作；画布已设 deletable:false，这里兜底所有入口
    const td = s.nodes.find((n) => n.id === id)?.data as FlowNodeData | undefined
    if (td?.isContainerChild) return
    set({
      nodes: s.nodes.filter((n) => n.id !== id),
      edges: s.edges.filter((e) => e.source !== id && e.target !== id),
      selectedNodeId: s.selectedNodeId === id ? null : s.selectedNodeId,
      dirty: true,
      ...pushHist(s),
    })
  },

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

    // 容器展开态下编辑的是容器子节点（frame.nodeId 打了 isContainerChild 标，
    // MC1）：上面的写回落在本画布节点，还需沿 owner 链镜像进顶层 IR——否则
    // 收拢容器时本画布节点随容器子节点一起被移除，本次编辑静默丢失
    let writeBackNodes = parentNodes
    const frameTarget = parentNodes.find((n) => n.id === frame.nodeId)
    const fd = frameTarget?.data as FlowNodeData | undefined
    if (fd?.isContainerChild && fd.ownerId) {
      writeBackNodes = syncContainerChildUp(parentNodes, frame.edges, frame.nodeId)
    }

    const hasStart = subNodes.some((nd) => nd.type === 'start')
    const hasEnd = subNodes.some((nd) => nd.type === 'end')
    const warning =
      hasStart && hasEnd
        ? null
        : `子流程「${frame.title}」缺少 ${!hasStart ? 'start' : ''}${
            !hasStart && !hasEnd ? ' 和 ' : ''
          }${!hasEnd ? 'end' : ''} 节点，保存后端校验会失败`

    set({
      nodes: writeBackNodes,
      edges: frame.edges,
      selectedNodeId: frame.selectedNodeId,
      graphStack: s.graphStack.slice(0, -1),
      subgraphWarning: warning,
      dirty: true,
      // 子图编辑写回父图是内容变更，记一步。必须压「父图快照」（frame 暂存的
      // 进入时父图）而非当前子图：undo 恢复的是画布，退出后 graphStack 已空、
      // 门禁放行，若压子图快照，撤销会把画布替换成循环体内容，再保存即把子
      // 流程序列化成顶层 flow（2026-10 评审 E1）
      ...pushHist({ nodes: frame.nodes, edges: frame.edges, past: s.past }),
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
      past: [],
      future: [],
      addNodeFromPalette: null,
    }),
}))
