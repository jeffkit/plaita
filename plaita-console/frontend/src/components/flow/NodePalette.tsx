import { useQuery } from '@tanstack/react-query'
import { api } from '../../services/api'
import { resolveNodeTypeConfig } from './nodeTypes'

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

  const onDragStart = (e: React.DragEvent, nodeType: string, name: string) => {
    e.dataTransfer.setData('application/plaita-node', JSON.stringify({ nodeType, name }))
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div className="w-56 bg-dark-900/80 border-r border-dark-700 overflow-y-auto p-3">
      <h3 className="text-sm font-semibold text-dark-200 mb-3">节点面板</h3>
      {isLoading && <p className="text-xs text-dark-400">加载中…</p>}
      {Array.from(byCategory.entries()).map(([cat, list]) => (
        <div key={cat} className="mb-4">
          <div className="text-xs text-dark-400 uppercase tracking-wide mb-1">{cat}</div>
          <div className="space-y-1">
            {list.map((n) => {
              const cfg = resolveNodeTypeConfig(n.node_type)
              return (
                <div
                  key={n.node_type}
                  draggable
                  onDragStart={(e) => onDragStart(e, n.node_type, n.node_name || n.node_type)}
                  className="flex items-center gap-2 px-2 py-1.5 rounded-md bg-dark-800 hover:bg-dark-700 cursor-grab border border-dark-700 border-l-2 text-sm"
                  title={n.node_type}
                  style={{ borderLeftColor: `var(--family-${cfg.color}, #475569)` }}
                >
                  <span>{cfg.icon}</span>
                  <span className="truncate">{n.node_name || n.node_type}</span>
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}
