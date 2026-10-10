import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useSearchParams, useNavigate, useBlocker } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../services/api'
import { useFlowEditor } from '../stores/flowEditor'
import { jsonToFlow, flowToJson, extractLayout, type FlowNodeData } from '../components/flow/flowConverter'
import { normalizeFieldKeys, validateNodeFields, type JsonSchema } from '../components/flow/schemaForm/schemaUtils'
import NodePalette from '../components/flow/NodePalette'
import FlowCanvas from '../components/flow/FlowCanvas'
import NodeConfigDrawer from '../components/flow/NodeConfigDrawer'
import { autoLayout, type LayoutDirection } from '../components/flow/flowLayout'
import { symmetricLayout } from '../components/flow/symmetricLayout'
import DryRunPanel from '../components/flow/DryRunPanel'
import SourceViewPanel from '../components/flow/SourceViewPanel'
import type { Node, Edge } from '@xyflow/react'
import { ArrowLeft, Zap, Code2, Save, Rocket, Play, ChevronRight, AlertTriangle, Undo2, Redo2 } from 'lucide-react'
import { Button, StatusBadge, EmptyState, ConfirmDialog } from '../components/ui'
import CopilotPanel from '../components/flow/CopilotPanel'
import { useDebouncedValue } from '../hooks/useDebouncedValue'

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

/** 保存/发布前的流程级字段校验（B5 后半）：返回「节点.字段 问题」清单 */
function collectFlowSchemaErrors(
  nodes: Node<FlowNodeData>[],
  schemaByType: Map<string, JsonSchema>
): string[] {
  const errs: string[] = []
  for (const n of nodes) {
    const d = n.data as FlowNodeData
    const schema = schemaByType.get(d.type)
    if (!schema) continue
    const { output: _o, timeout: _t, ...rest } = d.fields
    void _o
    void _t
    for (const e of validateNodeFields(normalizeFieldKeys(rest, schema), schema)) {
      errs.push(`${n.id}.${e.key} ${e.message}`)
    }
  }
  return errs
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
  const [pendingAiIr, setPendingAiIr] = useState<Record<string, unknown> | null>(null)
  // C5-2：Copilot 输出待确认应用的整图 IR（画布 dirty 时先确认再覆盖）
  const [pendingCopilotIr, setPendingCopilotIr] = useState<Record<string, unknown> | null>(null)

  // 节点详情发起的「跳源码第 N 行」→ 打开源码面板并高亮
  const sourceLineRequest = useFlowEditor((s) => s.sourceLineRequest)
  useEffect(() => {
    if (sourceLineRequest != null) {
      setShowSource(true)
      setSourceHighlight(sourceLineRequest)
      useFlowEditor.setState({ sourceLineRequest: null })
    }
  }, [sourceLineRequest])
  const [showPublish, setShowPublish] = useState(false)
  const [publishDiff, setPublishDiff] = useState<VersionDiff | null>(null)
  // Copilot 面板默认展开，可随时收起（关闭后本会话不再自动弹出）

  const setFlowContext = useFlowEditor((s) => s.setFlowContext)
  const setGraph = useFlowEditor((s) => s.setGraph)
  const markDirty = useFlowEditor((s) => s.markDirty)
  const reset = useFlowEditor((s) => s.reset)
  const nodes = useFlowEditor((s) => s.nodes)
  const edges = useFlowEditor((s) => s.edges)
  // C5-3 拖拽静默快照：拖拽/连线期间 nodes/edges 每帧变化，热路径序列化
  // （copilot 上下文、源码/试跑面板）只依赖静默 300ms 后的快照
  const quietNodes = useDebouncedValue(nodes)
  const quietEdges = useDebouncedValue(edges)
  const dirty = useFlowEditor((s) => s.dirty)
  const graphStack = useFlowEditor((s) => s.graphStack)
  const subgraphWarning = useFlowEditor((s) => s.subgraphWarning)
  const exitToLevel = useFlowEditor((s) => s.exitToLevel)
  // 子图视图内撤销/重做禁用（历史快照按编辑层整图记录，跨层回退会错位）
  const inSubgraph = graphStack.length > 0
  const canUndo = useFlowEditor((s) => s.past.length > 0) && !inSubgraph
  const canRedo = useFlowEditor((s) => s.future.length > 0) && !inSubgraph

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
  // C5-1 乐观锁基准：画布所基于版本的 updated_at（载入/保存成功后刷新）。
  // 保存同版本时作为 base_updated_at 回传，服务端不一致即 409
  const baseVersionRef = useRef<{ version: string; updatedAt: string | null }>({
    version: '',
    updatedAt: null,
  })
  // 原定义的 metadata（含 @flow 源码）：保存/发布时透传，源码面板读取
  const metadataRef = useRef<Record<string, unknown> | undefined>(undefined)
  const [flowSource, setFlowSource] = useState('')
  // 源码视图数据来源：authoritative=metadata.source（仓内权威，行号可跳）；
  // decompiled=后端 emit_source 反编译兜底（画布 flow / 存量 JSON 无内嵌源码）
  const [sourceKind, setSourceKind] = useState<'authoritative' | 'decompiled' | null>(null)
  // 节点详情「查看源码」跳转：高亮行
  const [sourceHighlight, setSourceHighlight] = useState<number | null>(null)

  // 初始化画布
  useEffect(() => {
    if (!flowId) return
    // 反编译兜底的竞态防护：版本切换/卸载后，迟到的响应不得回写画布状态
    let sourceFetchCancelled: (() => void) | null = null
    if (versionParam && versionQuery.data) {
      try {
        const def = JSON.parse(versionQuery.data.definition || '{}') as Record<string, unknown>
        const layout = JSON.parse(versionQuery.data.layout || '{}') as Record<string, { x: number; y: number }>
        const { nodes: ns, edges: es } = jsonToFlow(def, layout)
        baseDefRef.current = def
        // C5-1：记录乐观锁基准（服务端最近保存时间）
        baseVersionRef.current = { version: versionParam, updatedAt: versionQuery.data.updated_at ?? null }
        // 原定义的 metadata（可能含 @flow 源码）原样透传：保存/发布不丢，源码面板可用
        const defMeta = (def.metadata as Record<string, unknown> | undefined) || undefined
        metadataRef.current = defMeta
        const src = defMeta && typeof defMeta.source === 'string' ? defMeta.source : ''
        setFlowSource(src)
        setSourceKind(src ? 'authoritative' : null)
        useFlowEditor.setState({ hasFlowSource: !!src, sourceKind: src ? 'authoritative' : null, sourceLineRequest: null })
        if (!src) {
          // 无内嵌源码（画布 flow / 存量 JSON）：后端 emit_source 反编译兜底。
          // 失败（不可表达的构造）保持无页签；竞态用 cancelled 防旧版本回写。
          let cancelled = false
          api.getFlowVersionSource(flowId, versionParam).then((res) => {
            if (cancelled) return
            if (res.source_kind === 'decompiled' && res.source) {
              setFlowSource(res.source)
              setSourceKind('decompiled')
              useFlowEditor.setState({ hasFlowSource: true, sourceKind: 'decompiled' })
            }
          }).catch(() => { /* 反编译不可用：如实不显示 @flow 页签 */ })
          sourceFetchCancelled = () => { cancelled = true }
        }
        setGraph(ns as Node[], es as Edge[])
        setDesc((def.desc as string) || '')
        setInputType(def.inputType ?? { dataType: 'object' })
        setVersion(versionParam)
        // inputType/globalContext 必须进 store meta：节点抽屉的变量目录
        // （$INPUT 分组）从 flowMeta 读取（2026-10 表单评审，此前整组消失）
        setFlowContext(flowId, versionParam, {
          flow_id: flowId,
          version: versionParam,
          desc: def.desc as string,
          inputType: def.inputType,
          ...(def.globalContext !== undefined
            ? { globalContext: def.globalContext as Record<string, unknown> }
            : {}),
        })
      } catch (e) {
        // 定义损坏时不静默：清空画布并把错误交给保存/发布前的序列化兜底
        baseDefRef.current = null
        baseVersionRef.current = { version: '', updatedAt: null }
        metadataRef.current = undefined
        setFlowSource('')
        // sourceKind 必须与 source 一起清：否则残留的 authoritative/decompiled
        // 会描述一个已经不存在的来源（源码页签按旧来源渲染）
        setSourceKind(null)
        useFlowEditor.setState({ hasFlowSource: false, sourceKind: null, sourceLineRequest: null })
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
      baseVersionRef.current = { version: '', updatedAt: null }
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
    // 反编译兜底请求的取消：版本切换/卸载后迟到的响应不得回写画布状态
    return () => { sourceFetchCancelled?.() }
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

  // 节点类型 schema：与节点面板/配置抽屉共用 ['nodes'] 缓存（B5 后半保存前校验用）
  const nodesSchemaQuery = useQuery({
    queryKey: ['nodes'],
    queryFn: () => api.getNodes(),
    staleTime: 5 * 60_000,
  })
  const schemaByType = useMemo(() => {
    const map = new Map<string, JsonSchema>()
    for (const d of nodesSchemaQuery.data?.nodes || []) {
      try {
        map.set(d.node_type, JSON.parse(d.schema_json || '{}') as JsonSchema)
      } catch {
        // 坏 schema 的类型按无 schema 处理
      }
    }
    return map
  }, [nodesSchemaQuery.data])

  // 成功提示自动消失，避免与新错误长期并存
  useEffect(() => {
    if (!msg) return
    const t = setTimeout(() => setMsg(null), 4000)
    return () => clearTimeout(t)
  }, [msg])

  // 保存参数（C5-1）：同版本覆盖带乐观锁基准；另存新版本走服务端原子分配
  interface SaveVars {
    targetVersion: string
    /** 目标版本即当前编辑版本时带上（服务端不一致返回 409 冲突） */
    baseUpdatedAt?: string | null
    force?: boolean
    allocateVersion?: boolean
    onSaved?: (saved: { version: string; updatedAt: string | null }) => void
  }
  const saveMutation = useMutation({
    mutationFn: async (v: SaveVars) => {
      const state = collapseToRoot()
      // B5 后半：保存前用同一份节点 schema 校验字段（required/type/enum），
      // 未通过即阻断并定位到 节点.字段——引擎 422 的报错现场远离编辑器
      const schemaErrs = collectFlowSchemaErrors(state.nodes as Node<FlowNodeData>[], schemaByType)
      if (schemaErrs.length) {
        throw new Error(
          `字段校验未通过，未保存：${schemaErrs.slice(0, 3).join('；')}` +
            (schemaErrs.length > 3 ? ` 等 ${schemaErrs.length} 项` : '')
        )
      }
      const meta = { flow_id: flowId, version: v.targetVersion, desc, inputType, metadata: metadataRef.current }
      const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], meta)
      const layout = extractLayout(state.nodes as Node[])
      return api.saveVersion(flowId!, v.targetVersion, {
        definition: JSON.stringify(def, null, 2),
        layout: JSON.stringify(layout),
        ...(v.baseUpdatedAt ? { base_updated_at: v.baseUpdatedAt } : {}),
        ...(v.force ? { force: true } : {}),
        ...(v.allocateVersion ? { allocate_version: true } : {}),
      })
    },
    onSuccess: (res, v) => {
      setSaveError(null)
      // allocate_version 时服务端可能分配了与请求不同的版本号：以响应为准
      setMsg(`已保存 ${flowId}@${res.version}`)
      // 保存成功后工作版本即目标版本；乐观锁基准随响应前进
      setVersion(res.version)
      baseVersionRef.current = { version: res.version, updatedAt: res.updated_at ?? null }
      useFlowEditor.setState({ dirty: false })
      qc.invalidateQueries({ queryKey: ['flow', flowId] })
      v.onSaved?.({ version: res.version, updatedAt: res.updated_at ?? null })
    },
    onError: (e: Error, v) => {
      const detail = (e as Error & { detail?: { conflict?: string } }).detail
      if (detail?.conflict === 'version_stale') {
        // C5-1：画布所基于的版本已被他人更新——给「加载最新 / 强制覆盖」选择
        setSaveError(null)
        setConflict({ retry: () => saveMutation.mutate({ ...v, force: true }) })
      } else {
        setSaveError(e.message)
      }
    },
  })

  // C5-1 冲突处理中转：retry 为「强制覆盖」重放；「加载最新」走 reloadLatest
  const [conflict, setConflict] = useState<{ retry: () => void } | null>(null)
  const reloadLatest = () => {
    setConflict(null)
    useFlowEditor.setState({ dirty: false })
    if (versionParam) qc.invalidateQueries({ queryKey: ['version', flowId, versionParam] })
  }

  const publishMutation = useMutation({
    mutationFn: async (v: { targetVersion: string; force?: boolean }) => {
      // 先保存再发布；后端保证已发布版本不可覆盖（409）
      const state = collapseToRoot()
      const schemaErrs = collectFlowSchemaErrors(state.nodes as Node<FlowNodeData>[], schemaByType)
      if (schemaErrs.length) {
        throw new Error(
          `字段校验未通过，未发布：${schemaErrs.slice(0, 3).join('；')}` +
            (schemaErrs.length > 3 ? ` 等 ${schemaErrs.length} 项` : '')
        )
      }
      const meta = { flow_id: flowId, version: v.targetVersion, desc, inputType, metadata: metadataRef.current }
      const def = flowToJson(state.nodes as Node<FlowNodeData>[], state.edges as Edge[], meta)
      const layout = extractLayout(state.nodes as Node[])
      await api.saveVersion(flowId!, v.targetVersion, {
        definition: JSON.stringify(def, null, 2),
        layout: JSON.stringify(layout),
        ...(baseVersionRef.current.version === v.targetVersion && baseVersionRef.current.updatedAt
          ? { base_updated_at: baseVersionRef.current.updatedAt }
          : {}),
        ...(v.force ? { force: true } : {}),
      })
      return api.publishFlow(flowId!, v.targetVersion)
    },
    onSuccess: (_res, v) => {
      setSaveError(null)
      setShowPublish(false)
      setMsg(`已发布 ${flowId}@${v.targetVersion}`)
      setVersion(v.targetVersion)
      baseVersionRef.current = { version: v.targetVersion, updatedAt: null }
      useFlowEditor.setState({ dirty: false })
      qc.invalidateQueries({ queryKey: ['flow', flowId] })
      qc.invalidateQueries({ queryKey: ['version', flowId] })
    },
    onError: (e: Error, v) => {
      setShowPublish(false)
      const detail = (e as Error & { detail?: { conflict?: string } }).detail
      if (detail?.conflict === 'version_stale') {
        setSaveError(null)
        setConflict({ retry: () => publishMutation.mutate({ ...v, force: true }) })
      } else {
        setSaveError(e.message)
      }
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

  /** 保存入口：基于已发布版本编辑时自动另存新版本（服务端原子分配，C5-1）；
   *  覆盖当前草稿版本时带乐观锁基准（409 冲突弹「加载最新/强制覆盖」） */
  const doSave = useCallback(() => {
    if (saveMutation.isPending || publishMutation.isPending) return
    const target = workingStatus === 'published' ? suggestedNext : version || suggestedNext
    setMsg(null)
    const updateInPlace = !!version && target === version
    saveMutation.mutate({
      targetVersion: target,
      ...(updateInPlace && baseVersionRef.current.version === target
        ? { baseUpdatedAt: baseVersionRef.current.updatedAt }
        : { allocateVersion: true }),
    })
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
    const updateInPlace = !!version && target === version
    saveMutation.mutate(
      {
        targetVersion: target,
        ...(updateInPlace && baseVersionRef.current.version === target
          ? { baseUpdatedAt: baseVersionRef.current.updatedAt }
          : { allocateVersion: true }),
      },
      { onSuccess: () => blocker.proceed?.() }
    )
  }

  // Cmd/Ctrl+S 保存；Cmd/Ctrl+Z / +Shift+Z（或 Ctrl+Y）撤销/重做。
  // 焦点在输入框时放行浏览器文本撤销；子图视图内历史禁用（见 store 注释）
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const mod = e.metaKey || e.ctrlKey
      if (!mod) return
      const key = e.key.toLowerCase()
      if (key === 's') {
        e.preventDefault()
        doSave()
        return
      }
      if (key === 'z' || key === 'y') {
        const t = e.target as HTMLElement | null
        const typing =
          !!t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.isContentEditable)
        if (typing) return
        e.preventDefault()
        const editor = useFlowEditor.getState()
        const redo = key === 'y' || e.shiftKey
        if (redo) editor.redo()
        else editor.undo()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [doSave])

  const status = workingStatus

  // Copilot 上下文：当前画布完整 flow + 状态，随每轮请求自动带最新值。
  // C5-3：依赖静默快照（quietNodes/quietEdges）——拖拽期间不再每帧
  // flowToJson + stringify，画布静默 300ms 后一次性对齐，最终值与画布一致
  const copilotContext = useMemo(() => {
    const def = flowToJson(quietNodes as Node<FlowNodeData>[], quietEdges as Edge[], {
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
  }, [quietNodes, quietEdges, flowId, version, desc, inputType, dirty, graphStack])

  // C5-3：试跑面板载荷（nodesByType + flowJson）与源码面板 flow 同样走静默快照
  const dryRunPayload = useMemo(() => {
    const nodesByType: Record<string, string[]> = {}
    for (const n of quietNodes) {
      const t = (n.data as FlowNodeData).type
      ;(nodesByType[t] ||= []).push(n.id)
    }
    const flowJson = JSON.stringify(
      flowToJson(quietNodes as Node<FlowNodeData>[], quietEdges as Edge[], {
        flow_id: flowId,
        version,
        desc,
        inputType,
      }),
      null,
      2
    )
    return { nodesByType, flowJson }
  }, [quietNodes, quietEdges, flowId, version, desc, inputType])

  const sourceFlow = useMemo(
    () =>
      flowToJson(quietNodes as Node<FlowNodeData>[], quietEdges as Edge[], {
        flow_id: flowId,
        version,
        desc,
        inputType,
      }),
    [quietNodes, quietEdges, flowId, version, desc, inputType]
  )

  // 应用 agent 的 plaita-flow 输出：归位主图 → 整图替换。C5-2：当前图先压入
  // 撤销栈（replaceGraph）而非清空历史——Cmd+Z 可回到应用前的画布
  const applyAiFlow = (ir: Record<string, unknown>) => {
    exitToLevel(0)
    const { nodes: ns, edges: es } = jsonToFlow(ir, {})
    useFlowEditor.getState().replaceGraph(ns as Node[], es as Edge[])
    markDirty()
    setMsg('已应用 AI 助手的画布修改（未保存，可撤销或继续编辑后保存草稿）')
  }

  // C5-2：应用前若画布有未保存修改，先弹确认（不再对 agent 回复静默整图覆盖）
  const requestApplyAiFlow = (ir: Record<string, unknown>) => {
    if (useFlowEditor.getState().dirty) setPendingCopilotIr(ir)
    else applyAiFlow(ir)
  }

  const applyAiImport = (ir: Record<string, unknown>) => {
    const { nodes: ns, edges: es } = jsonToFlow(ir as Parameters<typeof jsonToFlow>[0], {})
    setGraph(ns as Node[], es as Edge[])
    setDesc((ir.desc as string) || `AI 生成（${new Date().toLocaleString()}）`)
    // 落到下一个新草稿版本，避免与现有版本号撞车
    const target = workingStatus === 'published' ? suggestedNext : version || suggestedNext
    setVersion(target)
    setFlowContext(flowId!, target, { flow_id: flowId, version: target, inputType })
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
        <Button
          variant="secondary"
          size="sm"
          onClick={doSave}
          disabled={saveMutation.isPending || flowLoading}
          title={editingPublishedBase ? `基于已发布版本编辑：将另存为新草稿版本 ${suggestedNext}` : 'Cmd/Ctrl+S'}
        >
          <Save size={13} />
          保存草稿
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
        {/* 撤销/重做：画布与表单编辑共用一套历史（store 层）；子图视图内禁用 */}
        <div className="flex items-center rounded-md border border-line overflow-hidden">
          <button
            onClick={() => useFlowEditor.getState().undo()}
            disabled={!canUndo}
            title="撤销（Cmd/Ctrl+Z）"
            className="px-2 h-7 text-caption text-ink-secondary hover:bg-elevated hover:text-ink-primary transition-colors disabled:opacity-40 disabled:pointer-events-none"
          >
            <Undo2 size={13} />
          </button>
          <button
            onClick={() => useFlowEditor.getState().redo()}
            disabled={!canRedo}
            title="重做（Cmd/Ctrl+Shift+Z / Ctrl+Y）"
            className="px-2 h-7 text-caption text-ink-secondary hover:bg-elevated hover:text-ink-primary transition-colors border-l border-line disabled:opacity-40 disabled:pointer-events-none"
          >
            <Redo2 size={13} />
          </button>
        </div>
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
                // C5-4：布局走 replaceGraph（当前图入撤销栈）而非 setGraph
                // （清空历史）——布局后可 Cmd+Z 回布局前、重做恢复布局
                useFlowEditor.getState().replaceGraph(layouted as Node[], edges as Edge[])
                markDirty()
              }}
              className="px-2.5 h-7 text-caption text-ink-secondary hover:bg-elevated hover:text-ink-primary transition-colors"
            >
              {dir === 'TB' ? '纵向' : '横向'}
            </button>
          ))}
        </div>
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
          已发布版本不可修改（发布即不可变）：当前编辑基于 {versionQuery.data?.version}，
          「保存草稿」将创建新草稿版本 {suggestedNext}，满意后可再发布
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
            flowContext={copilotContext}
            flowId={flowId || ''}
            onApplyFlow={requestApplyAiFlow}
          />
          {showDryRun && (
            <DryRunPanel
              inputType={inputType}
              onRunStatus={(erroredIds) => {
                // 出错节点画布标红 / 新一轮清除（不置 dirty）
                useFlowEditor.getState().setRunErrorNodes(erroredIds)
              }}
              nodesByType={dryRunPayload.nodesByType}
              onErrorNodeId={(id) => useFlowEditor.setState({ selectedNodeId: id })}
              flowJson={dryRunPayload.flowJson}
              onClose={() => setShowDryRun(false)}
            />
          )}
          {showSource && (
            <SourceViewPanel
              flow={sourceFlow}
              source={flowSource || undefined}
              sourceKind={sourceKind}
              highlightLine={sourceKind === 'authoritative' ? sourceHighlight : null}
              onHighlightDone={() => setSourceHighlight(null)}
              onClose={() => {
                setShowSource(false)
                setSourceHighlight(null)
              }}
            />
          )}
        </div>
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
      {/* C5-2：画布 dirty 时，Copilot 的整图修改先确认再应用（应用后可撤销） */}
      <ConfirmDialog
        open={!!pendingCopilotIr}
        title="应用 AI 助手的画布修改？"
        variant="danger"
        confirmLabel="应用（可撤销）"
        cancelLabel="放弃"
        onCancel={() => setPendingCopilotIr(null)}
        onConfirm={() => {
          if (pendingCopilotIr) applyAiFlow(pendingCopilotIr)
          setPendingCopilotIr(null)
        }}
      >
        当前画布有未保存修改，AI 的修改会整体替换画布。应用后可用撤销（Cmd/Ctrl+Z）回到应用前。
      </ConfirmDialog>
      {/* C5-1：乐观锁冲突——画布所基于的版本已被他人更新 */}
      <ConfirmDialog
        open={!!conflict}
        title="版本已被他人更新"
        variant="danger"
        confirmLabel="强制覆盖"
        cancelLabel="加载最新"
        onCancel={reloadLatest}
        onConfirm={() => {
          conflict?.retry()
          setConflict(null)
        }}
      >
        <p>画布所基于的版本已被他人更新，直接保存会覆盖对方的修改。</p>
        <p className="text-caption">
          「加载最新」丢弃本地未保存修改并载入服务端最新内容；「强制覆盖」以当前画布内容覆盖他人修改。
        </p>
      </ConfirmDialog>
      <ConfirmDialog
        open={showPublish}
        title={`发布 ${flowId}@${version}`}
        confirmLabel={publishMutation.isPending ? '发布中…' : '确认发布'}
        cancelLabel="取消"
        busy={publishMutation.isPending}
        wide
        onCancel={() => setShowPublish(false)}
        onConfirm={() => publishMutation.mutate({ targetVersion: version })}
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
