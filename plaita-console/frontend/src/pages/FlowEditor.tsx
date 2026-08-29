import { useEffect, useMemo, useState } from 'react'
import { useParams, useSearchParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../services/api'
import { useFlowEditor } from '../stores/flowEditor'
import { jsonToFlow, flowToJson, extractLayout, type FlowNodeData } from '../components/flow/flowConverter'
import NodePalette from '../components/flow/NodePalette'
import FlowCanvas from '../components/flow/FlowCanvas'
import NodeConfigDrawer from '../components/flow/NodeConfigDrawer'
import AiGenerateDialog from '../components/flow/AiGenerateDialog'
import { autoLayout, type LayoutDirection } from '../components/flow/flowLayout'
import { symmetricLayout } from '../components/flow/symmetricLayout'
import DryRunPanel from '../components/flow/DryRunPanel'
import SourceViewPanel from '../components/flow/SourceViewPanel'
import type { Node, Edge } from '@xyflow/react'
import { ArrowLeft, Zap, Sparkles, Code2, Save, Rocket, Play, ChevronRight, Bot } from 'lucide-react'
import { Button, StatusBadge } from '../components/ui'
import CopilotPanel from '../components/flow/CopilotPanel'

export default function FlowEditor() {
  const { flowId } = useParams<{ flowId: string }>()
  const [search, setSearch] = useSearchParams()
  const versionParam = search.get('version') || ''
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [version, setVersion] = useState(versionParam || '0.0.1')
  const [desc, setDesc] = useState('')
  // 流程输入类型：决定 $INPUT 在试跑/运行时是否可用。默认 object，使
  // $INPUT.xxx 表达式能取到传入参数；加载已有版本时沿用其声明。
  const [inputType, setInputType] = useState<unknown>({ dataType: 'object' })
  const [saveError, setSaveError] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [showDryRun, setShowDryRun] = useState(false)
  const [showSource, setShowSource] = useState(false)
  const [showAiDialog, setShowAiDialog] = useState(false)
  // Copilot 面板默认展开，可随时收起（关闭后本会话不再自动弹出）
  const [showCopilot, setShowCopilot] = useState(true)

  const setFlowContext = useFlowEditor((s) => s.setFlowContext)
  const setGraph = useFlowEditor((s) => s.setGraph)
  const markDirty = useFlowEditor((s) => s.markDirty)
  const reset = useFlowEditor((s) => s.reset)
  const nodes = useFlowEditor((s) => s.nodes)
  const edges = useFlowEditor((s) => s.edges)
  const dirty = useFlowEditor((s) => s.dirty)
  const graphStack = useFlowEditor((s) => s.graphStack)
  const subgraphWarning = useFlowEditor((s) => s.subgraphWarning)
  const exitToLevel = useFlowEditor((s) => s.exitToLevel)

  /** 保存/发布/试跑/源码前把子图逐层归位（子图写回父节点），始终序列化主图 */
  const collapseToRoot = () => {
    if (useFlowEditor.getState().graphStack.length > 0) {
      exitToLevel(0)
      return useFlowEditor.getState()
    }
    return useFlowEditor.getState()
  }

  const flowQuery = useQuery({
    queryKey: ['flow', flowId],
    queryFn: () => api.getFlow(flowId!),
    enabled: !!flowId,
  })

  const versionQuery = useQuery({
    queryKey: ['version', flowId, versionParam],
    queryFn: () => api.getVersion(flowId!, versionParam),
    enabled: !!flowId && !!versionParam,
    retry: false,
  })

  // 初始化画布
  useEffect(() => {
    if (!flowId) return
    if (versionParam && versionQuery.data) {
      const def = JSON.parse(versionQuery.data.definition || '{}') as Record<string, unknown>
      const layout = JSON.parse(versionQuery.data.layout || '{}') as Record<string, { x: number; y: number }>
      const { nodes: ns, edges: es } = jsonToFlow(def, layout)
      setGraph(ns as Node[], es as Edge[])
      setDesc((def.desc as string) || '')
      setInputType(def.inputType ?? { dataType: 'object' })
      setVersion(versionParam)
      setFlowContext(flowId, versionParam, { flow_id: flowId, version: versionParam, desc: def.desc as string })
    } else if (!versionParam && flowQuery.data) {
      // 无版本参数（列表「编辑」入口）：自动选最新已发布版本，交回版本加载分支
      const versions = (flowQuery.data.versions || []) as Array<{ version: string; status?: string }>
      const best = versions.find((v) => v.status === 'published') || versions[versions.length - 1]
      if (best) {
        setSearch(new URLSearchParams({ version: best.version }))
        return
      }
      const start: Node = {
        id: 'start',
        type: 'plaitaNode',
        position: { x: 200, y: 80 },
        data: { type: 'start', name: 'start', fields: {} },
      }
      const end: Node = {
        id: 'end',
        type: 'plaitaNode',
        position: { x: 200, y: 240 },
        data: { type: 'end', name: 'end', fields: { output: '$INPUT.name', resultType: 'success' } },
      }
      const edge: Edge = { id: 'e-start-end', source: 'start', target: 'end', sourceHandle: 'true' }
      setGraph([start, end], [edge])
      setFlowContext(flowId, version, { flow_id: flowId, version })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flowId, versionParam, versionQuery.data])

  useEffect(() => () => reset(), [reset])

  const saveMutation = useMutation({
    mutationFn: async () => {
      const state = collapseToRoot()
      const meta = { flow_id: flowId, version, desc, inputType }
      const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], meta)
      const layout = extractLayout(state.nodes as Node[])
      return api.saveVersion(flowId!, version, {
        definition: JSON.stringify(def, null, 2),
        layout: JSON.stringify(layout),
      })
    },
    onSuccess: () => {
      setSaveError(null)
      setMsg(`已保存草稿 ${flowId}@${version}`)
      qc.invalidateQueries({ queryKey: ['flow', flowId] })
    },
    onError: (e: Error) => setSaveError(e.message),
  })

  const publishMutation = useMutation({
    mutationFn: async () => {
      // 先保存再发布
      const state = collapseToRoot()
      const meta = { flow_id: flowId, version, desc, inputType }
      const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], meta)
      const layout = extractLayout(state.nodes as Node[])
      await api.saveVersion(flowId!, version, {
        definition: JSON.stringify(def, null, 2),
        layout: JSON.stringify(layout),
      })
      return api.publishFlow(flowId!, version)
    },
    onSuccess: () => {
      setSaveError(null)
      setMsg(`已发布 ${flowId}@${version}`)
      qc.invalidateQueries({ queryKey: ['flow', flowId] })
    },
    onError: (e: Error) => setSaveError(e.message),
  })

  const status = useMemo(() => {
    const versions = flowQuery.data?.versions || []
    return versions.find((v) => v.version === version)?.status
  }, [flowQuery.data, version])

  // Copilot 上下文：当前画布完整 flow + 状态，随每轮请求自动带最新值
  const copilotContext = useMemo(() => {
    const def = flowToJson(nodes as Node<FlowNodeData>[], edges as Edge[], {
      flow_id: flowId,
      version,
      desc,
      inputType,
    })
    return JSON.stringify(
      {
        flow: def,
        dirty,
        subgraph_stack: graphStack.map((f) => f.title),
        note: '修改画布时输出完整新 flow JSON 于 ```plaita-flow 代码块中',
      },
      null,
      1
    )
  }, [nodes, edges, flowId, version, desc, inputType, dirty, graphStack])

  // 应用 agent 的 plaita-flow 输出：归位主图 → 整图替换（自动应用）
  const applyAiFlow = (ir: Record<string, unknown>) => {
    exitToLevel(0)
    const { nodes: ns, edges: es } = jsonToFlow(ir, {})
    useFlowEditor.getState().setGraph(ns as Node[], es as Edge[])
    markDirty()
    setMsg('已应用 AI 助手的画布修改（未保存，可继续编辑后保存草稿）')
  }

  return (
    <div className="h-full flex flex-col">
      {/* 顶部工具栏：内容过宽时横向滚动，不挤压按钮 */}
      <div className="flex items-center gap-2 px-4 py-2 bg-surface border-b border-line overflow-x-auto">
        <div className="flex items-center gap-2 shrink-0">
        <Button variant="ghost" size="sm" onClick={() => navigate('/flows')}>
          <ArrowLeft size={14} />
          返回列表
        </Button>
        <span className="font-mono text-data font-semibold text-ink-primary">{flowId}</span>
        <span className="text-ink-faint">@</span>
        <input
          value={version}
          onChange={(e) => setVersion(e.target.value)}
          className="input w-24"
          placeholder="0.0.1"
        />
        {status && <StatusBadge status={status} />}
        <input
          value={desc}
          onChange={(e) => setDesc(e.target.value)}
          placeholder="流程描述"
          className="input w-44"
        />
        <div className="flex-1" />
        {dirty && <span className="text-caption text-status-warning">未保存</span>}
        <Button variant="secondary" size="sm" onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending}>
          <Save size={13} />
          保存草稿
        </Button>
        <Button variant="primary" size="sm" onClick={() => publishMutation.mutate()} disabled={publishMutation.isPending}>
          <Rocket size={13} />
          发布
        </Button>
        <Button variant="secondary" size="sm" onClick={() => { collapseToRoot(); setShowDryRun((v) => !v) }}>
          <Play size={13} />
          试跑
        </Button>
        <div
          className="flex items-center rounded-md border border-line overflow-hidden"
          title="自动布局：从开始节点单方向展开，分支自然分叉"
        >
          <span className="pl-2.5 pr-1.5 text-caption text-ink-muted flex items-center gap-1">
            <Zap size={12} />
            布局
          </span>
          {(['TB', 'LR'] as LayoutDirection[]).map((dir) => (
            <button
              key={dir}
              onClick={() => {
                const layouted =
                  dir === 'TB'
                    ? symmetricLayout(nodes as Node[], edges as Edge[], 'TB')
                    : autoLayout(nodes as Node[], edges as Edge[], 'LR')
                setGraph(layouted as Node[], edges as Edge[])
                markDirty()
              }}
              className="px-2.5 h-7 text-caption text-ink-secondary hover:bg-elevated hover:text-ink-primary transition-colors"
            >
              {dir === 'TB' ? '纵向' : '横向'}
            </button>
          ))}
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={() => setShowAiDialog(true)}
          title="自然语言 → AI 生成 @flow（后端 agent 宿主经 agentproc 运行）"
        >
          <Sparkles size={13} className="text-plaita-400" />
          AI 生成
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setShowCopilot((v) => !v)}
          className={showCopilot ? 'bg-plaita-500/10 text-plaita-400 hover:text-plaita-400' : undefined}
        >
          <Bot size={13} />
          AI 助手
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => { collapseToRoot(); setShowSource((v) => !v) }}
          className={showSource ? 'bg-plaita-500/10 text-plaita-400 hover:text-plaita-400' : undefined}
        >
          <Code2 size={13} />
          源码
        </Button>
        </div>
      </div>

      {/* 子图编辑面包屑：主图 › map · 处理订单 › …；点击任意层归位到该层 */}
      {(graphStack.length > 0 || subgraphWarning) && (
        <div className="flex items-center gap-1.5 px-4 py-1.5 bg-surface border-b border-line text-caption overflow-x-auto">
          {graphStack.length > 0 && (
            <>
              <button
                onClick={() => exitToLevel(0)}
                className="text-ink-secondary hover:text-ink-primary shrink-0"
              >
                主图
              </button>
              {graphStack.map((f, i) => (
                <span key={i} className="flex items-center gap-1.5 shrink-0">
                  <ChevronRight size={12} className="text-ink-faint" />
                  <button
                    onClick={() => exitToLevel(i + 1)}
                    className={
                      i === graphStack.length - 1
                        ? 'text-ink-primary'
                        : 'text-ink-secondary hover:text-ink-primary'
                    }
                  >
                    {f.title}
                  </button>
                </span>
              ))}
            </>
          )}
          {subgraphWarning && (
            <span className="ml-auto text-status-warning shrink-0">⚠ {subgraphWarning}</span>
          )}
        </div>
      )}

      {saveError && (
        <div className="px-4 py-1 bg-status-error-dim text-status-error text-caption">{saveError}</div>
      )}
      {msg && (
        <div className="px-4 py-1 bg-status-success-dim text-status-success text-caption">{msg}</div>
      )}

      {/* 编辑器主体 */}
      <div className="flex-1 flex min-h-0">
        <NodePalette />
        <FlowCanvas />
        <NodeConfigDrawer />
        <CopilotPanel
          open={showCopilot}
          flowContext={copilotContext}
          onApplyFlow={applyAiFlow}
          onClose={() => setShowCopilot(false)}
        />
        {showDryRun && (
          <DryRunPanel
            flowJson={JSON.stringify(
              flowToJson(nodes as Node<FlowNodeData>[], edges as Edge[], { flow_id: flowId, version, desc, inputType }),
              null,
              2
            )}
            onClose={() => setShowDryRun(false)}
          />
        )}
        {showSource && (
          <SourceViewPanel
            flow={flowToJson(nodes as Node<FlowNodeData>[], edges as Edge[], { flow_id: flowId, version, desc, inputType })}
            onClose={() => setShowSource(false)}
          />
        )}
      </div>
      {showAiDialog && (
        <AiGenerateDialog
          onClose={() => setShowAiDialog(false)}
          onImport={(ir) => {
            // 复用与「加载已保存版本」相同的 IR→画布 转换器（已验证兼容）
            const { nodes: ns, edges: es } = jsonToFlow(ir as Parameters<typeof jsonToFlow>[0], {})
            setGraph(ns as Node[], es as Edge[])
            setDesc(`AI 生成（${new Date().toLocaleString()}）`)
            const draftV = `0.${Math.floor(Date.now() / 1000) % 1000}.${Math.floor(Math.random() * 100)}`
            setVersion(draftV)
            setFlowContext(flowId!, draftV, { flow_id: flowId, version: draftV })
            setShowAiDialog(false)
            qc.invalidateQueries({ queryKey: ['version', flowId] })
          }}
        />
      )}
    </div>
  )
}
