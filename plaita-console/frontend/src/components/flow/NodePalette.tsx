import { useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronRight,
  PanelLeftClose,
  PanelLeftOpen,
} from 'lucide-react'
import { api } from '../../services/api'
import { resolveNodeTypeConfig } from './nodeTypes'

// 分类展示顺序（对齐后端 _CATEGORY_MAP 的分类命名）：常用类别置顶，
// 「调用」等低频类别靠后，未知分类排最后；组内按 node_type 稳定排序。
const CATEGORY_ORDER = ['控制', '数据', '子流程', '循环', '事件', '调用']
const FOLD_KEY = 'plaita-palette-folded'
const COLLAPSE_KEY = 'plaita-palette-collapsed'

interface NodeDesc {
  node_type: string
  node_name: string
  category: string
  schema_json: string
  is_builtin: boolean
}

// 节点面板：拖拽创建节点。手风琴分组 + 整体可收起 + 节点 hover 说明。
export default function NodePalette() {
  const { data, isLoading } = useQuery({
    queryKey: ['nodes'],
    queryFn: () => api.getNodes(),
  })
  const nodes = data?.nodes || []

  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(COLLAPSE_KEY) === '1'
  )
  const [folded, setFolded] = useState<Set<string>>(() => {
    try {
      return new Set(JSON.parse(localStorage.getItem(FOLD_KEY) || '[]'))
    } catch {
      return new Set()
    }
  })
  // hover 说明浮层：{节点描述, 图标锚点坐标}
  const [info, setInfo] = useState<{ desc: NodeDesc; x: number; y: number } | null>(null)
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const toggleFold = (cat: string) => {
    setFolded((prev) => {
      const next = new Set(prev)
      if (next.has(cat)) next.delete(cat)
      else next.add(cat)
      localStorage.setItem(FOLD_KEY, JSON.stringify([...next]))
      return next
    })
  }
  const toggleCollapsed = () => {
    setCollapsed((v) => {
      localStorage.setItem(COLLAPSE_KEY, v ? '0' : '1')
      return !v
    })
  }

  const openInfo = (desc: NodeDesc, el: HTMLElement) => {
    if (hoverTimer.current) clearTimeout(hoverTimer.current)
    hoverTimer.current = setTimeout(() => {
      const r = el.getBoundingClientRect()
      setInfo({ desc, x: r.right + 8, y: r.top })
    }, 300)
  }
  const closeInfo = () => {
    if (hoverTimer.current) clearTimeout(hoverTimer.current)
    setInfo(null)
  }

  // schema 顶层 description（引擎类 docstring 生成），按类型缓存
  const descById = useMemo(() => {
    const map = new Map<string, string>()
    for (const n of nodes) {
      try {
        map.set(n.node_type, JSON.parse(n.schema_json || '{}').description || '')
      } catch {
        map.set(n.node_type, '')
      }
    }
    return map
  }, [nodes])

  const byCategory = new Map<string, NodeDesc[]>()
  for (const n of nodes) {
    const cat = n.category || '其他'
    if (!byCategory.has(cat)) byCategory.set(cat, [])
    byCategory.get(cat)!.push(n)
  }
  const groups = Array.from(byCategory.entries())
    .map(([cat, list]) => [
      cat,
      [...list].sort((a, b) => a.node_type.localeCompare(b.node_type)),
    ] as const)
    .sort(([a], [b]) => {
      const ia = CATEGORY_ORDER.indexOf(a)
      const ib = CATEGORY_ORDER.indexOf(b)
      return (ia === -1 ? CATEGORY_ORDER.length : ia) - (ib === -1 ? CATEGORY_ORDER.length : ib)
    })

  const onDragStart = (e: React.DragEvent, nodeType: string, name: string) => {
    e.dataTransfer.setData('application/plaita-node', JSON.stringify({ nodeType, name }))
    e.dataTransfer.effectAllowed = 'move'
  }

  if (collapsed) {
    return (
      <div className="w-9 shrink-0 bg-surface border-r border-line flex flex-col items-center py-2">
        <button
          onClick={toggleCollapsed}
          title="展开节点面板"
          className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors"
        >
          <PanelLeftOpen size={15} />
        </button>
      </div>
    )
  }

  return (
    <div className="w-56 shrink-0 bg-surface border-r border-line overflow-y-auto p-3 relative">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-section text-ink-primary">节点面板</h3>
        <button
          onClick={toggleCollapsed}
          title="收起节点面板"
          className="p-1 rounded-md text-ink-faint hover:text-ink-primary hover:bg-elevated transition-colors"
        >
          <PanelLeftClose size={14} />
        </button>
      </div>
      {isLoading && <p className="text-caption text-ink-muted">加载中…</p>}
      {groups.map(([cat, list]) => {
        const open = !folded.has(cat)
        return (
          <div key={cat} className="mb-3">
            <button
              onClick={() => toggleFold(cat)}
              className="w-full flex items-center gap-1 text-micro uppercase tracking-wide text-ink-faint hover:text-ink-muted mb-1.5"
            >
              {open ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
              {cat}
              <span className="ml-auto font-mono text-[10px] opacity-70">{list.length}</span>
            </button>
            {open && (
              <div className="space-y-1">
                {list.map((n) => {
                  const cfg = resolveNodeTypeConfig(n.node_type)
                  return (
                    <div
                      key={n.node_type}
                      draggable
                      onDragStart={(e) => onDragStart(e, n.node_type, n.node_name || n.node_type)}
                      className="group relative flex items-center gap-2 px-2 py-1.5 rounded-md bg-surface hover:bg-elevated cursor-grab border border-line overflow-hidden transition-colors duration-150 active:cursor-grabbing"
                      title={n.node_type}
                    >
                      {/* 族别左色条（与画布节点同款配色） */}
                      <FamilyBar color={cfg.color} />
                      <span className="text-[13px] shrink-0">{cfg.icon}</span>
                      <span className="truncate text-caption text-ink-secondary">
                        {n.node_name || n.node_type}
                      </span>
                      {/* hover 说明触发区 */}
                      <span
                        className="ml-auto shrink-0 w-4 h-4 flex items-center justify-center rounded-full border border-line text-[9px] text-ink-faint opacity-0 group-hover:opacity-100 hover:text-ink-primary hover:border-ink-muted cursor-help transition-opacity"
                        onMouseEnter={(e) => openInfo(n, e.currentTarget)}
                        onMouseLeave={closeInfo}
                      >
                        ?
                      </span>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        )
      })}

      {/* 节点说明浮层（fixed 定位，跟随触发图标） */}
      {info && (
        <div
          className="fixed z-50 w-72 bg-elevated border border-line rounded-lg shadow-pop p-3 pointer-events-none"
          style={{
            left: Math.min(info.x, window.innerWidth - 310),
            top: Math.min(info.y, window.innerHeight - 180),
          }}
        >
          <div className="flex items-center gap-2 mb-1">
            <span className="text-caption font-medium text-ink-primary">
              {info.desc.node_name || info.desc.node_type}
            </span>
            <span className="font-mono text-[10px] text-ink-faint">{info.desc.node_type}</span>
            {info.desc.category && (
              <span className="ml-auto text-[10px] text-ink-faint">{info.desc.category}</span>
            )}
          </div>
          <p className="text-[11px] leading-[1.6] text-ink-secondary whitespace-pre-line">
            {descById.get(info.desc.node_type) || '（暂无说明）'}
          </p>
        </div>
      )}
    </div>
  )
}

/** 族别左色条：复用 nodeTypes 的标准色板（需要真实 class 以被 Tailwind 提取） */
const BAR_CLASSES: Record<string, string> = {
  violet: 'bg-violet-500', sky: 'bg-sky-500', amber: 'bg-amber-500', emerald: 'bg-emerald-500',
  slate: 'bg-slate-400', teal: 'bg-teal-500', cyan: 'bg-cyan-500', indigo: 'bg-indigo-500',
  rose: 'bg-rose-500', fuchsia: 'bg-fuchsia-500', purple: 'bg-purple-500', orange: 'bg-orange-500',
  yellow: 'bg-yellow-500', red: 'bg-red-500', green: 'bg-green-500', pink: 'bg-pink-500',
  gray: 'bg-gray-400', plaita: 'bg-plaita-500', blue: 'bg-blue-500',
}

function FamilyBar({ color }: { color: string }) {
  const cls = BAR_CLASSES[color] ?? 'bg-dark-500'
  return <span className={`absolute left-0 top-0 bottom-0 w-0.5 ${cls}`} />
}
