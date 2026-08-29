import { useQuery } from '@tanstack/react-query'
import { api } from '../../services/api'
import { resolveNodeTypeConfig } from './nodeTypes'

// 分类展示顺序（对齐后端 _CATEGORY_MAP 的分类命名）：常用类别置顶，
// 「调用」等低频类别靠后，未知分类排最后；组内按 node_type 稳定排序。
const CATEGORY_ORDER = ['控制', '数据', '子流程', '循环', '事件', '调用']

// 节点面板：拖拽创建节点。按 category 分组展示内置 + 自定义节点。
export default function NodePalette() {
  const { data, isLoading } = useQuery({
    queryKey: ['nodes'],
    queryFn: () => api.getNodes(),
  })

  const nodes = data?.nodes || []
  const byCategory = new Map<string, typeof nodes>()
  for (const n of nodes) {
    const cat = n.category || '其他'
    if (!byCategory.has(cat)) byCategory.set(cat, [])
    byCategory.get(cat)!.push(n)
  }
  const groups = Array.from(byCategory.entries()).map(([cat, list]) => [
    cat,
    [...list].sort((a, b) => a.node_type.localeCompare(b.node_type)),
  ] as const).sort(([a], [b]) => {
    const ia = CATEGORY_ORDER.indexOf(a)
    const ib = CATEGORY_ORDER.indexOf(b)
    return (ia === -1 ? CATEGORY_ORDER.length : ia) - (ib === -1 ? CATEGORY_ORDER.length : ib)
  })

  const onDragStart = (e: React.DragEvent, nodeType: string, name: string) => {
    e.dataTransfer.setData('application/plaita-node', JSON.stringify({ nodeType, name }))
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div className="w-56 shrink-0 bg-surface border-r border-line overflow-y-auto p-3">
      <h3 className="text-section text-ink-primary mb-3">节点面板</h3>
      {isLoading && <p className="text-caption text-ink-muted">加载中…</p>}
      {groups.map(([cat, list]) => (
        <div key={cat} className="mb-4">
          <div className="text-micro uppercase text-ink-faint mb-1.5">{cat}</div>
          <div className="space-y-1">
            {list.map((n) => {
              const cfg = resolveNodeTypeConfig(n.node_type)
              return (
                <div
                  key={n.node_type}
                  draggable
                  onDragStart={(e) => onDragStart(e, n.node_type, n.node_name || n.node_type)}
                  className="relative flex items-center gap-2 px-2 py-1.5 rounded-md bg-surface hover:bg-elevated cursor-grab border border-line overflow-hidden transition-colors duration-150 active:cursor-grabbing"
                  title={n.node_type}
                >
                  {/* 族别左色条（与画布节点同款配色，修复原先 var(--family-*) 未定义导致的失效） */}
                  <FamilyBar color={cfg.color} />
                  <span className="text-[13px] shrink-0">{cfg.icon}</span>
                  <span className="truncate text-caption text-ink-secondary">{n.node_name || n.node_type}</span>
                </div>
              )
            })}
          </div>
        </div>
      ))}
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
