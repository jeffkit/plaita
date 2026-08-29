import { Component, useEffect, useMemo, useRef, type ReactNode } from 'react'
import { CopilotKit, useCopilotAction, useCopilotChat, useCopilotReadable } from '@copilotkit/react-core'
import { useInterrupt } from '@copilotkit/react-core/v2/headless'
import { CopilotSidebar } from '@copilotkit/react-ui'
import { HttpAgent } from '@ag-ui/client'
import '@copilotkit/react-ui/styles.css'
import { useFlowEditor } from '../../stores/flowEditor'
import { api } from '../../services/api'
import { flowToJson } from './flowConverter'
import { symmetricLayout } from './symmetricLayout'

// 错误边界：Copilot 异常时不拖垮整个编辑器
class CopilotErrorBoundary extends Component<{ children: ReactNode }, { err?: Error }> {
  state: { err?: Error } = {}
  static getDerivedStateFromError(err: Error) {
    return { err }
  }
  render() {
    if (this.state.err) {
      return (
        <div data-testid="copilot-err" className="p-3 text-[11px] text-status-error whitespace-pre-wrap overflow-auto">
          {String(this.state.err.stack || this.state.err)}
        </div>
      )
    }
    return this.props.children
  }
}

/**
 * 编排页 Copilot 面板（方案 docs/copilot-agent-plan.md M1/M2）。
 *
 * - CopilotKit selfManagedAgents 直连后端 /api/copilot（AG-UI 协议，反代 recursive /agui）
 * - 当前画布状态经 useCopilotReadable 注入每轮请求的 context（后端拼进 user 消息）
 * - agent 回复中的 ```plaita-flow 代码块 = 完整 flow IR，自动应用（onApplyFlow）
 * - 前端工具（M2：recursive client tools 桥实时调用）：apply_flow / read_flow /
 *   select_node / dry_run——dry_run 让 agent 自检修改后的流程是否可执行（自纠闭环）
 */

// 从文本中提取最后一个 ```plaita-flow 代码块并解析为 IR
export function extractFlowIR(text: string | undefined | null): Record<string, unknown> | null {
  if (!text) return null
  const matches = [...text.matchAll(/```plaita-flow\s*\n([\s\S]*?)```/g)]
  if (matches.length === 0) return null
  try {
    const parsed = JSON.parse(matches[matches.length - 1][1].trim())
    return parsed && typeof parsed === 'object' ? parsed : null
  } catch {
    return null
  }
}

/** 监听聊天消息流中的 plaita-flow 代码块，自动应用（内容不变不重复触发） */
function useAutoApplyFlow(onApplyFlow: (ir: Record<string, unknown>) => void) {
  const chat = useCopilotChat() as unknown as { messages?: Array<Record<string, unknown>> }
  const messages = chat?.messages ?? []
  const lastApplied = useRef('')
  const applyRef = useRef(onApplyFlow)
  applyRef.current = onApplyFlow

  useEffect(() => {
    for (let i = messages.length - 1; i >= 0; i--) {
      const m = messages[i] as { role?: string; content?: unknown }
      if (m.role !== 'assistant') break
      const content = typeof m.content === 'string' ? m.content : ''
      const ir = extractFlowIR(content)
      if (!ir) return
      const key = JSON.stringify(ir)
      if (key === lastApplied.current) return
      lastApplied.current = key
      applyRef.current(ir)
      return
    }
  }, [messages])
}

function CopilotInner({
  flowContext,
  onApplyFlow,
}: {
  flowContext: string
  onApplyFlow: (ir: Record<string, unknown>) => void
}) {
  const nodes = useFlowEditor((s) => s.nodes)
  const edges = useFlowEditor((s) => s.edges)
  const meta = useFlowEditor((s) => s.meta)
  const setSelected = useFlowEditor((s) => s.setSelected)


  useCopilotReadable({
    description: '当前画布的完整 flow JSON 与状态',
    value: flowContext,
  })
  useAutoApplyFlow(onApplyFlow)

  /** 前端工具执行器：recursive interrupt 到达时按名分发（与下方 action 同一套语义） */
  const executeFrontendTool = async (
    toolName: string
  ): Promise<Record<string, unknown> | string> => {
    if (toolName === 'read_flow') return flowContext
    if (toolName === 'dry_run') {
      const def = flowToJson(nodes as never[], edges as never[], { ...meta })
      const res = await api.dryRun({
        flowJson: JSON.stringify(def),
        input: {},
      })
      const summary = (res.nodes || [])
        .map((n) => {
          const base = `[${n.status}] ${n.id ?? '(未知节点)'}`
          return n.error ? `${base}（${String(n.error).slice(0, 120)}）` : base
        })
        .join('\n')
      return { flowError: res.error || null, nodes: summary || '（无节点执行）' }
    }
    return { error: `未知前端工具: ${toolName}` }
  }

  // ── recursive interrupt 适配：执行前端工具并自动 resume ────────────
  // recursive 的 client tools 以 RunFinished(interrupt) 暂停，前端在此
  // 执行对应工具并 resolve(payload)，CopilotKit 提交 resume 续跑。
  useInterrupt({
    agentId: 'default',
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    enabled: (event: any) => {
      const it = event.interrupt as { metadata?: { frontendTool?: boolean } } | null
      return Boolean(it?.metadata?.frontendTool)
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    handler: async ({ interrupt, resolve }: any) => {
      if (!interrupt) return null
      const meta = interrupt.metadata as { toolName?: string } | undefined
      const toolName = meta?.toolName || 'read_flow'
      const payload = await executeFrontendTool(toolName)
      resolve(payload)
      return payload
    },
    render: () => (
      <div className="p-2 text-caption text-ink-faint">前端工具执行中…</div>
    ),
  })

  // ── 前端工具（recursive client tools 桥实时调用）────────────────────

  useCopilotAction({
    name: 'apply_flow',
    description: '把完整的新 flow IR 应用到画布（整体替换，自动刷新）',
    parameters: [
      { name: 'ir', type: 'object', description: '完整 flow IR（含 nodes 数组）', required: true },
    ],
    handler: async ({ ir }) => {
      onApplyFlow(ir as Record<string, unknown>)
      return '已应用到画布（未保存）'
    },
  })

  useCopilotAction({
    name: 'read_flow',
    description: '读取当前画布的 flow JSON、dirty 状态与子图栈',
    parameters: [],
    handler: async () => flowContext,
  })

  useCopilotAction({
    name: 'select_node',
    description: '在画布上选中并高亮指定节点',
    parameters: [
      { name: 'nodeId', type: 'string', description: '节点 id', required: true },
    ],
    handler: async ({ nodeId }) => {
      const exists = useFlowEditor.getState().nodes.some((n) => n.id === nodeId)
      if (!exists) return `节点 ${nodeId} 不存在`
      setSelected(nodeId)
      return `已选中 ${nodeId}`
    },
  })

  useCopilotAction({
    name: 'dry_run',
    description:
      '对当前画布流程做真实试跑（引擎执行），返回每个节点状态与错误——用于自检修改后的流程是否可执行',
    parameters: [
      { name: 'input', type: 'object', description: '流程入参（$INPUT）', required: false },
    ],
    handler: async ({ input }) => {
      const def = flowToJson(nodes as never[], edges as never[], { ...meta })
      const res = await api.dryRun({
        flowJson: JSON.stringify(def),
        input: (input as Record<string, unknown>) ?? {},
      })
      const summary = (res.nodes || [])
        .map((n) => {
          const base = `[${n.status}] ${n.id ?? '(未知节点)'}`
          return n.error ? `${base}（${String(n.error).slice(0, 120)}）` : base
        })
        .join('\n')
      return `flowError: ${res.error || '无'}\n节点执行:\n${summary || '（无节点执行）'}`
    },
  })

  // ── 原子编辑工具集（M2-4）──────────────────────────────────────────

  let nodeSeq = Date.now() % 100000
  useCopilotAction({
    name: 'add_node',
    description:
      '向当前画布追加一个节点。nodeType 见画布节点面板（如 http/map/if/assignment/code/while）；返回新节点 id',
    parameters: [
      { name: 'nodeType', type: 'string', description: '节点类型', required: true },
      { name: 'name', type: 'string', description: '节点显示名', required: false },
      { name: 'fields', type: 'object', description: '类型特定字段（如 http 的 url/method）', required: false },
    ],
    handler: async ({ nodeType, name, fields }) => {
      nodeSeq += 1
      const id = `${nodeType}_${Date.now()}_${nodeSeq}`
      const store = useFlowEditor.getState()
      store.addNode({
        id,
        type: 'plaitaNode',
        position: { x: 240 + (store.nodes.length % 4) * 60, y: 120 + store.nodes.length * 110 },
        data: { type: nodeType, name: name || nodeType, fields: fields ?? {} },
      })
      return `已添加节点 ${id}（当前画布共 ${store.nodes.length} 个节点）`
    },
  })

  useCopilotAction({
    name: 'update_node',
    description:
      '修改既有节点：fields 为与现有字段合并的增量 patch（不删除未提及字段）；name 可改显示名',
    parameters: [
      { name: 'nodeId', type: 'string', description: '节点 id', required: true },
      { name: 'fields', type: 'object', description: '要合并的字段增量', required: false },
      { name: 'name', type: 'string', description: '新显示名', required: false },
    ],
    handler: async ({ nodeId, fields, name }) => {
      const target = useFlowEditor.getState().nodes.find((n) => n.id === nodeId)
      if (!target) return `节点 ${nodeId} 不存在`
      const d = target.data as { name?: string; fields?: Record<string, unknown> }
      const merged = { ...(d.fields ?? {}), ...(fields ?? {}) }
      useFlowEditor.getState().updateNodeData(nodeId, {
        ...(name ? { name } : {}),
        fields: merged,
      })
      return `已更新 ${nodeId}`
    },
  })

  useCopilotAction({
    name: 'remove_node',
    description: '删除节点（级联删除其连线）',
    parameters: [
      { name: 'nodeId', type: 'string', description: '节点 id', required: true },
    ],
    handler: async ({ nodeId }) => {
      const exists = useFlowEditor.getState().nodes.some((n) => n.id === nodeId)
      if (!exists) return `节点 ${nodeId} 不存在`
      useFlowEditor.getState().removeNode(nodeId)
      return `已删除 ${nodeId}`
    },
  })

  useCopilotAction({
    name: 'connect_nodes',
    description:
      '连接两个节点。默认从 source 的真分支（next）连到 target；if 节点的假分支用 sourceHandle="false"',
    parameters: [
      { name: 'source', type: 'string', description: '源节点 id', required: true },
      { name: 'target', type: 'string', description: '目标节点 id', required: true },
      { name: 'sourceHandle', type: 'string', description: '源 handle（默认 true；if 假分支用 false）', required: false },
    ],
    handler: async ({ source, target, sourceHandle }) => {
      const st = useFlowEditor.getState()
      if (!st.nodes.some((n) => n.id === source)) return `源节点 ${source} 不存在`
      if (!st.nodes.some((n) => n.id === target)) return `目标节点 ${target} 不存在`
      st.onConnect({
        source,
        target,
        sourceHandle: sourceHandle || 'true',
        targetHandle: 'in',
      })
      return `已连接 ${source} → ${target}`
    },
  })

  useCopilotAction({
    name: 'enter_subgraph',
    description: '进入指定子流程节点的子图编辑（map/loop/filter/find/reduce/while/child）',
    parameters: [
      { name: 'nodeId', type: 'string', description: '子流程节点 id', required: true },
    ],
    handler: async ({ nodeId }) => {
      const st = useFlowEditor.getState()
      const node = st.nodes.find((n) => n.id === nodeId)
      if (!node) return `节点 ${nodeId} 不存在`
      try {
        st.enterSubgraph(nodeId, 'child_flow')
        return `已进入 ${nodeId} 的子图`
      } catch {
        return `节点 ${nodeId} 不含可编辑的子流程`
      }
    },
  })

  useCopilotAction({
    name: 'exit_subgraph',
    description: '退出当前子图，返回上一层（level 0 表示主图）',
    parameters: [
      { name: 'level', type: 'number', description: '目标层级（默认返回上一层）', required: false },
    ],
    handler: async ({ level }) => {
      const st = useFlowEditor.getState()
      const target = typeof level === 'number' ? Math.max(0, Math.min(level, st.graphStack.length)) : st.graphStack.length - 1
      st.exitToLevel(target)
      return `已返回到层级 ${useFlowEditor.getState().graphStack.length}`
    },
  })

  useCopilotAction({
    name: 'auto_layout',
    description: '对当前画布执行对称自动排版（分支左右均匀分布）',
    parameters: [],
    handler: async () => {
      const st = useFlowEditor.getState()
      const layouted = symmetricLayout(st.nodes, st.edges, 'TB')
      st.setGraph(layouted, st.edges)
      st.markDirty()
      return '已重新排版'
    },
  })

  return null
}

export default function CopilotPanel({
  open,
  flowContext,
  flowId,
  onApplyFlow,
  onClose,
}: {
  open: boolean
  flowContext: string
  flowId: string
  onApplyFlow: (ir: Record<string, unknown>) => void
  onClose: () => void
}) {
  const agent = useMemo(
    // @copilotkit 内嵌的 @ag-ui/core 实例与直接依赖在类型上有私有字段差异，运行时同源
    () =>
      new HttpAgent({
        url: '/api/copilot',
        headers: flowId ? { 'X-Flow-Id': flowId } : undefined,
      }) as never,
    [flowId]
  )

  return (
    <div
      data-testid="copilot-panel"
      style={{ display: open ? undefined : 'none' }}
      className="w-[380px] shrink-0 border-l border-line h-full"
    >
      <CopilotErrorBoundary>
      <CopilotKit selfManagedAgents={{ default: agent }}>
        <CopilotInner flowContext={flowContext} onApplyFlow={onApplyFlow} />
        <CopilotSidebar
          defaultOpen
          onSetOpen={(o) => {
            if (!o) onClose()
          }}
          labels={{
            title: '编排助手',
            initial:
              '我可以读取并修改当前流程。试试：\n「加一个 http 节点调用 example.com」「把 map 的并发打开」',
          }}
        />
      </CopilotKit>
      </CopilotErrorBoundary>
    </div>
  )
}
