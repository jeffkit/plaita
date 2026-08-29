import { create } from 'zustand'
import type { Node, Edge, Connection, OnNodesChange, OnEdgesChange, OnConnect } from '@xyflow/react'
import { applyNodeChanges, applyEdgeChanges, addEdge } from '@xyflow/react'
import type { FlowMeta } from '../components/flow/flowConverter'
import { EDGE_COLOR, EDGE_TYPE } from '../components/flow/flowLayout'

export interface FlowEditorState {
  flowId: string
  version: string
  meta: FlowMeta
  nodes: Node[]
  edges: Edge[]
  selectedNodeId: string | null
  dirty: boolean

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
  reset: () => void
}

export const useFlowEditor = create<FlowEditorState>((set) => ({
  flowId: '',
  version: '',
  meta: {},
  nodes: [],
  edges: [],
  selectedNodeId: null,
  dirty: false,

  setFlowContext: (flowId, version, meta) => set({ flowId, version, meta }),

  setGraph: (nodes, edges) => set({ nodes, edges, dirty: false }),

  onNodesChange: (changes) =>
    set((s) => ({ nodes: applyNodeChanges(changes, s.nodes) as Node[], dirty: true })),

  onEdgesChange: (changes) =>
    set((s) => ({ edges: applyEdgeChanges(changes, s.edges) as Edge[], dirty: true })),

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

  removeNode: (id) =>
    set((s) => ({
      nodes: s.nodes.filter((n) => n.id !== id),
      edges: s.edges.filter((e) => e.source !== id && e.target !== id),
      selectedNodeId: s.selectedNodeId === id ? null : s.selectedNodeId,
      dirty: true,
    })),

  setSelected: (id) => set({ selectedNodeId: id }),

  markDirty: () => set({ dirty: true }),

  reset: () =>
    set({ flowId: '', version: '', meta: {}, nodes: [], edges: [], selectedNodeId: null, dirty: false }),
}))
