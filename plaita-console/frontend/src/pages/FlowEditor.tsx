import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useSearchParams, useNavigate, useBlocker } from 'react-router-dom'
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
import {
  ArrowLeft,
  Zap,
  Sparkles,
  Code2,
  Save,
  Rocket,
  Play,
  ChevronRight,
  Bot,
  AlertTriangle,
} from 'lucide-react'
import { Button, StatusBadge, EmptyState, ConfirmDialog } from '../components/ui'
import CopilotPanel from '../components/flow/CopilotPanel'

// ---------- 版本工具 ----------

function semverTuple(v: string): [number, number, number] | null {
  const m = /^(\d+)\.(\d+)\.(\d+)$/.exec(v)
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : null
}

/** 全部版本（含草稿）中的下一个 patch 版本号；无合法版本时从 0.0.1 起步 */
function nextVersionOf(versions: Array<{ version: string }>): string {
  let best: [number, number, number] = [0, 0, -1]
  for (const { version } of versions) {
    const t = semverTuple(version)
    if (!t) continue
    if (t[0] > best[0] || (t[0] === best[0] && t[1] > best[1]) || (t[0] === best[0] && t[1] === best[1] && t[2] > best[2])) {
      best = t
    }
  }
  if (best[2] < 0) return '0.0.1'
  return `${best[0]}.${best[1]}.${best[2] + 1}`
}

function versionStatusLabel(status?: string): string {
  if (status === 'published') return '已发布'
  if (status === 'draft') return '草稿'
  return status || ''
}

interface VersionDiff {
  added: string[]
  removed: string[]
  changed: string[]
}

function diffDefinitions(next: Record<string, unknown>, base: Record<string, unknown> | null): VersionDiff {
  const toMap = (d: Record<string, unknown> | null) => {
    const arr = ((d?.nodes as Array<Record<string, unknown>>) || []) as Array<Record<string, unknown>>
    return new Map(arr.map((n) => [String(n.id ?? n.type ?? ''), JSON.stringify(n)]))
  }
  const nextMap = toMap(next)
  const baseMap = toMap(base)
  const added: string[] = []
  const removed: string[] = []
  const changed: string[] = []
  for (const [id, json] of nextMap) {
    const old = baseMap.get(id)
    if (old === undefined) added.push(id)
    else if (old !== json) changed.push(id)
  }
  for (const id of baseMap.keys()) {
    if (!nextMap.has(id)) removed.push(id)
  }
  return { added, removed, changed }
}

export default function FlowEditor() {
  const { flowId } = useParams<{ flowId: string }>()
  const [search, setSearch] = useSearchParams()
  const versionParam = search.get('version') || ''
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [version, setVersion] = useState('')
  const [desc, setDesc] = useState('')
  // 流程输入类型：决定 $INPUT 在试跑/运行时是否可用。默认 object，使
  // $INPUT.xxx 表达式能取到传入参数；加载已有版本时沿用其声明。
  const [inputType, setInputType] = useState<unknown>({ dataType: 'object' })
  const [saveError, setSaveError] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [showDryRun, setShowDryRun] = useState(false)
  const [showSource, setShowSource] = useState(false)
  const [showAiDialog, setShowAiDialog] = useState(false)
  const [pendingAiIr, setPendingAiIr] = useState<Record<string, unknown> | null>(null)
  const [showPublish, setShowPublish] = useState(false)
  const [publishDiff, setPublishDiff] = useState<VersionDiff | null>(null)
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
  })

  // 载入画布时的基准定义：发布确认里的变更摘要与它对比
  const baseDefRef = useRef<Record<string, unknown> | null>(null)

  // 初始化画布
  useEffect(() => {
    if (!flowId) return
    if (versionParam && versionQuery.data) {
      try {
        const def = JSON.parse(versionQuery.data.definition || '{}') as Record<string, unknown>
        const layout = JSON.parse(versionQuery.data.layout || '{}') as Record<string, { x: number; y: number }>
        const { nodes: ns, edges: es } = jsonToFlow(def, layout)
        baseDefRef.current = def
        setGraph(ns as Node[], es as Edge[])
        setDesc((def.desc as string) || '')
        setInputType(def.inputType ?? { dataType: 'object' })
        setVersion(versionParam)
        setFlowContext(flowId, versionParam, { flow_id: flowId, version: versionParam, desc: def.desc as string })
      } catch (e) {
        // 定义损坏时不静默：清空画布并把错误交给保存/发布前的序列化兜底
        baseDefRef.current = null
        setGraph([], [])
        setMsg(`版本定义解析失败：${(e as Error).message}`)
      }
    } else if (!versionParam && flowQuery.data) {
      // 无版本参数（列表「编辑」入口）：自动选最新已发布版本，交回版本加载分支
      const versions = (flowQuery.data.versions || []) as Array<{ version: string; status?: string }>
      const best = versions.find((v) => v.status === 'published') || versions[versions.length - 1]
      if (best) {
        setSearch(new URLSearchParams({ version: best.version }))
        return
      }
      baseDefRef.current = null
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
      setVersion('0.0.1')
      setFlowContext(flowId, '0.0.1', { flow_id: flowId, version: '0.0.1' })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flowId, versionParam, versionQuery.data, flowQuery.data])

  useEffect(() => () => reset(), [reset])

  const versions = useMemo(
    () => (flowQuery.data?.versions || []) as Array<{ version: string; status?: string }>,
    [flowQuery.data]
  )
  const suggestedNext = useMemo(() => nextVersionOf(versions), [versions])
  // 当前工作版本的状态（版本列表里的；未保存的新草稿不在列表里）
  const workingStatus = versions.find((v) => v.version === version)?.status
  // 画布内容来自已发布版本时，保存必须另存新版本（发布即不可变）
  const loadedStatus = versionQuery.data?.status
  const editingPublishedBase = loadedStatus === 'published'

  // 成功提示自动消失，避免与新错误长期并存
  useEffect(() => {
    if (!msg) return
    const t = setTimeout(() => setMsg(null), 4000)
    return () => clearTimeout(t)
  }, [msg])

  const saveMutation = useMutation({
    mutationFn: async (targetVersion: string) => {
      const state = collapseToRoot()
      const meta = { flow_id: flowId, version: targetVersion, desc, inputType }
      const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], meta)
      const layout = extractLayout(state.nodes as Node[])
      return api.saveVersion(flowId!, targetVersion, {
        definition: JSON.stringify(def, null, 2),
        layout: JSON.stringify(layout),
      })
    },
    onSuccess: (_res, targetVersion) => {
      setSaveError(null)
      setMsg(`已保存 ${flowId}@${targetVersion}`)
      // 保存成功后工作版本即为目标版本；URL 由下方同步 effect 对齐
      setVersion(targetVersion)
      useFlowEditor.setState({ dirty: false })
      qc.invalidateQueries({ queryKey: ['flow', flowId] })
    },
    onError: (e: Error) => setSaveError(e.message),
  })

  const publishMutation = useMutation({
    mutationFn: async (targetVersion: string) => {
      // 先保存再发布；后端保证已发布版本不可覆盖（409）
      const state = collapseToRoot()
      const meta = { flow_id: flowId, version: targetVersion, desc, inputType }
      const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], meta)
      const layout = extractLayout(state.nodes as Node[])
      await api.saveVersion(flowId!, targetVersion, {
        definition: JSON.stringify(def, null, 2),
        layout: JSON.stringify(layout),
      })
      return api.publishFlow(flowId!, targetVersion)
    },
    onSuccess: (_res, targetVersion) => {
      setSaveError(null)
      setShowPublish(false)
      setMsg(`已发布 ${flowId}@${targetVersion}`)
      setVersion(targetVersion)
      useFlowEditor.setState({ dirty: false })
      qc.invalidateQueries({ queryKey: ['flow', flowId] })
      qc.invalidateQueries({ queryKey: ['version', flowId] })
    },
    onError: (e: Error) => {
      setShowPublish(false)
      setSaveError(e.message)
    },
  })

  // URL ↔ 工作版本同步：保存另存新版本 / 加载失败回退时，把地址栏对齐到工作版本。
  // 只在非 dirty 时生效，避免绕过未保存拦截。
  useEffect(() => {
    if (!dirty && version && versionParam && versionParam !== version) {
      setSearch(new URLSearchParams({ version }))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [version, versionParam, dirty])

  /** 保存入口：基于已发布版本编辑时自动另存为下一个版本 */
  const doSave = useCallback(() => {
    if (saveMutation.isPending || publishMutation.isPending) return
    const target = workingStatus === 'published' ? suggestedNext : version || suggestedNext
    setMsg(null)
    saveMutation.mutate(target)
  }, [saveMutation, publishMutation.isPending, workingStatus, suggestedNext, version])

  const openPublish = () => {
    if (publishMutation.isPending) return
    const state = collapseToRoot()
    const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], {
      flow_id: flowId,
      version,
      desc,
      inputType,
    })
    setPublishDiff(diffDefinitions(def, baseDefRef.current))
    setShowPublish(true)
  }

  // 未保存拦截：应用内导航（含返回列表、切版本）与刷新/关闭双保险
  const blocker = useBlocker(dirty)

  useEffect(() => {
    if (!dirty) return
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault()
      e.returnValue = ''
    }
    window.addEventListener('beforeunload', handler)
    return () => window.removeEventListener('beforeunload', handler)
  }, [dirty])

  const saveThenProceed = () => {
    const target = workingStatus === 'published' ? suggestedNext : version || suggestedNext
    saveMutation.mutate(target, { onSuccess: () => blocker.proceed?.() })
  }

  // Cmd/Ctrl+S 保存
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 's') {
        e.preventDefault()
        doSave()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [doSave])

  const status = workingStatus

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

  const applyAiImport = (ir: Record<string, unknown>) => {
    const { nodes: ns, edges: es } = jsonToFlow(ir as Parameters<typeof jsonToFlow>[0], {})
    setGraph(ns as Node[], es as Edge[])
    setDesc((ir.desc as string) || `AI 生成（${new Date().toLocaleString()}）`)
    // 落到下一个新草稿版本，避免与现有版本号撞车
    const target = workingStatus === 'published' ? suggestedNext : version || suggestedNext
    setVersion(target)
    setFlowContext(flowId!, target, { flow_id: flowId, version: target })
    useFlowEditor.setState({ dirty: true })
    qc.invalidateQueries({ queryKey: ['version', flowId] })
  }

  const flowLoading = flowQuery.isLoading || (!!versionParam && versionQuery.isLoading)
  const flowError = flowQuery.isError
    ? flowQuery.error
    : !!versionParam && versionQuery.isError
      ? versionQuery.error
      : null
  const retryFlowError = flowQuery.isError ? () => flowQuery.refetch() : () => versionQuery.refetch()

  const switchingVersion = (next: string) => {
    if (next === version) return
    // dirty 时 useBlocker 会拦截并弹确认；这里只负责改 URL
    setSearch(new URLSearchParams({ version: next }))
  }

  return (
    <div className="h-full flex flex-col relative">
      {/* 顶部工具栏：内容过宽时横向滚动，不挤压按钮 */}
      <div className="flex items-center gap-2 px-4 py-2 bg-surface border-b border-line overflow-x-auto">
        <div className="flex items-center gap-2 shrink-0">
        <Button variant="ghost" size="sm" onClick={() => navigate('/flows')}>
          <ArrowLeft size={14} />
          返回列表
        </Button>
        <span className="font-mono text-data font-semibold text-ink-primary">{flowId}</span>
        <span className="text-ink-faint">@</span>
        <select
          value={version}
          onChange={(e) => switchingVersion(e.target.value)}
          className="input w-44 font-mono"
          title="切换版本；已发布版本不可修改，保存时自动另存新版本"
        >
          {versions.map((v) => (
            <option key={v.version} value={v.version}>
              {v.version}（{versionStatusLabel(v.status)}）
            </option>
          ))}
          {!versions.some((v) => v.version === version) && (
            <option value={version}>{version}（新草稿）</option>
          )}
        </select>
        {status && <StatusBadge status={status} />}
        <input
          value={desc}
          onChange={(e) => setDesc(e.target.value)}
          placeholder="流程描述"
          className="input w-44"
        />
        <div className="flex-1" />
        {dirty && <span className="text-caption text-status-warning">未保存</span>}
        <Button variant="secondary" size="sm" onClick={doSave} disabled={saveMutation.isPending || flowLoading} title="Cmd/Ctrl+S">
          <Save size={13} />
          {editingPublishedBase ? '保存为新版本' : '保存草稿'}
        </Button>
        <Button
          variant="primary"
          size="sm"
          onClick={openPublish}
          disabled={publishMutation.isPending || flowLoading || !version || status === 'published'}
          title={status === 'published' ? '该版本已发布（不可变）；编辑后保存为新版本再发布' : '保存并发布当前版本'}
        >
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

      {editingPublishedBase && (
        <div className="px-4 py-1 bg-status-warning-dim text-status-warning text-caption">
          已发布版本不可修改（发布即不可变）：当前编辑基于 {versionQuery.data?.version}，保存将创建新版本 {suggestedNext}
        </div>
      )}

      {saveError && (
        <div className="px-4 py-1 bg-status-error-dim text-status-error text-caption">{saveError}</div>
      )}
      {msg && (
        <div className="px-4 py-1 bg-status-success-dim text-status-success text-caption">{msg}</div>
      )}

      {/* 加载 / 错误面：不允许静默空画布 */}
      {flowLoading ? (
        <div className="flex-1 flex items-center justify-center">
          <EmptyState message="加载中…" hint={`正在获取 ${flowId} 的流程定义`} />
        </div>
      ) : flowError ? (
        <div className="flex-1 flex items-center justify-center">
          <EmptyState
            icon={<AlertTriangle size={20} />}
            message="加载失败"
            hint={(flowError as Error).message}
            action={
              <div className="flex gap-2">
                <Button variant="secondary" size="sm" onClick={retryFlowError}>
                  重试
                </Button>
                <Button variant="ghost" size="sm" onClick={() => navigate('/flows')}>
                  返回列表
                </Button>
              </div>
            }
          />
        </div>
      ) : (
        /* 编辑器主体 */
        <div className="flex-1 flex min-h-0 relative">
          <NodePalette />
          <FlowCanvas />
          {nodes.length === 0 && (
            <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
              <EmptyState
                message="画布为空"
                hint="从左侧节点面板拖入节点开始编排，或点击「AI 生成」由需求描述直接生成"
              />
            </div>
          )}
          <NodeConfigDrawer />
          <CopilotPanel
            open={showCopilot}
            flowContext={copilotContext}
            flowId={flowId || ''}
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
      )}
      {showAiDialog && (
        <AiGenerateDialog
          onClose={() => setShowAiDialog(false)}
          onImport={(ir) => {
            // 覆盖导入前必须确认（画布上未保存的内容会丢）
            setShowAiDialog(false)
            setPendingAiIr(ir as Record<string, unknown>)
          }}
        />
      )}
      <ConfirmDialog
        open={!!pendingAiIr}
        title="导入并覆盖当前画布？"
        variant="danger"
        confirmLabel="覆盖导入"
        onCancel={() => setPendingAiIr(null)}
        onConfirm={() => {
          if (pendingAiIr) applyAiImport(pendingAiIr)
          setPendingAiIr(null)
        }}
      >
        当前画布上未保存的修改将被丢弃，AI 生成的内容会整体替换画布。
      </ConfirmDialog>
      <ConfirmDialog
        open={showPublish}
        title={`发布 ${flowId}@${version}`}
        confirmLabel={publishMutation.isPending ? '发布中…' : '确认发布'}
        cancelLabel="取消"
        busy={publishMutation.isPending}
        wide
        onCancel={() => setShowPublish(false)}
        onConfirm={() => publishMutation.mutate(version)}
      >
        <p>发布后该版本<strong className="text-ink-primary">不可再修改</strong>；后续改动请另存新版本。</p>
        {publishDiff && (
          <div className="text-caption space-y-1">
            <p className="text-ink-muted">
              相对基准版本「{versionQuery.data?.version || '空白'}」的结构变更：
            </p>
            <p>
              <span className="text-status-success">新增 {publishDiff.added.length}</span>
              <span className="mx-2 text-ink-faint">·</span>
              <span className="text-status-error">删除 {publishDiff.removed.length}</span>
              <span className="mx-2 text-ink-faint">·</span>
              <span className="text-status-warning">修改 {publishDiff.changed.length}</span>
            </p>
            {publishDiff.added.length > 0 && (
              <p className="font-mono text-data-sm truncate">+ {publishDiff.added.join(', ')}</p>
            )}
            {publishDiff.removed.length > 0 && (
              <p className="font-mono text-data-sm truncate">− {publishDiff.removed.join(', ')}</p>
            )}
            {publishDiff.changed.length > 0 && (
              <p className="font-mono text-data-sm truncate">~ {publishDiff.changed.join(', ')}</p>
            )}
          </div>
        )}
      </ConfirmDialog>
      {/* 未保存拦截：应用内路由跳转 */}
      {blocker.state === 'blocked' && (
        <ConfirmDialog
          open
          title="有未保存的更改"
          variant="danger"
          confirmLabel="放弃更改并离开"
          cancelLabel="继续编辑"
          onCancel={() => blocker.reset()}
          onConfirm={() => blocker.proceed()}
        >
          <p>离开当前页面将丢失未保存的画布修改。</p>
          <Button variant="secondary" size="sm" onClick={saveThenProceed} disabled={saveMutation.isPending} className="w-full">
            <Save size={13} />
            {saveMutation.isPending ? '保存中…' : `保存${editingPublishedBase ? '为新版本 ' + suggestedNext : '草稿'}并继续`}
          </Button>
        </ConfirmDialog>
      )}
    </div>
  )
}
