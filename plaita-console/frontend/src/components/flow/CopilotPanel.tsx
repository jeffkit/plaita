import { Component, useEffect, useMemo, useRef, type ReactNode } from 'react'
import { CopilotKit, useCopilotAction, useCopilotChat, useCopilotReadable } from '@copilotkit/react-core'
import { useInterrupt } from '@copilotkit/react-core/v2/headless'
import { CopilotSidebar } from '@copilotkit/react-ui'
import { HttpAgent } from '@ag-ui/client'
import '@copilotkit/react-ui/styles.css'
import { useFlowEditor } from '../../stores/flowEditor'
import { api } from '../../services/api'
import { flowToJson } from './flowConverter'

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
  const chat = useCopilotChat() as unknown as Record<string, unknown>

  // dev 调试钩子：自动化测试环境无法操作聊天输入框时，经 window 触发发送
  useEffect(() => {
    ;(window as unknown as { __copilotChat?: Record<string, unknown> }).__copilotChat = chat
  }, [chat])

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
