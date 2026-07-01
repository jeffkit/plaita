import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../services/api'
import { nodeTypeConfig } from '../components/flow/nodeTypes'

const EMPTY_SCHEMA = `{
  "properties": {
    "url": { "type": "string" }
  }
}`

export default function Nodes() {
  const qc = useQueryClient()
  const [form, setForm] = useState({ node_type: '', node_name: '', category: '', schema_json: EMPTY_SCHEMA })
  const [error, setError] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)

  const nodesQuery = useQuery({
    queryKey: ['nodes'],
    queryFn: () => api.getNodes(),
  })

  const registerMut = useMutation({
    mutationFn: () =>
      api.registerNode({
        node_type: form.node_type,
        node_name: form.node_name,
        category: form.category,
        schema_json: form.schema_json,
      }),
    onSuccess: () => {
      setError(null)
      setMsg(`已注册节点 ${form.node_type}`)
      setForm({ node_type: '', node_name: '', category: '', schema_json: EMPTY_SCHEMA })
      qc.invalidateQueries({ queryKey: ['nodes'] })
    },
    onError: (e: Error) => { setError(e.message); setMsg(null) },
  })

  const nodes = nodesQuery.data?.nodes || []

  return (
    <div className="p-6 max-w-5xl">
      <h1 className="text-2xl font-bold text-dark-100 mb-1">节点管理</h1>
      <p className="text-dark-400 text-sm mb-6">查看内置节点、注册自定义节点描述（用于编排表单）</p>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* 注册表单 */}
        <div className="bg-dark-800 border border-dark-700 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-dark-200 mb-3">注册自定义节点</h2>
          <div className="space-y-2">
            <input
              value={form.node_type}
              onChange={(e) => setForm({ ...form, node_type: e.target.value })}
              placeholder="node_type（不可与内置冲突）"
              className="input w-full"
            />
            <div className="flex gap-2">
              <input
                value={form.node_name}
                onChange={(e) => setForm({ ...form, node_name: e.target.value })}
                placeholder="展示名"
                className="input flex-1"
              />
              <input
                value={form.category}
                onChange={(e) => setForm({ ...form, category: e.target.value })}
                placeholder="分类"
                className="input w-32"
              />
            </div>
            <textarea
              value={form.schema_json}
              onChange={(e) => setForm({ ...form, schema_json: e.target.value })}
              rows={10}
              className="input w-full font-mono text-xs"
              placeholder="节点字段 schema (JSON)"
            />
            <button
              onClick={() => registerMut.mutate()}
              disabled={!form.node_type || registerMut.isPending}
              className="bg-plaita-600 hover:bg-plaita-500 disabled:opacity-50 text-white px-4 py-2 rounded w-full"
            >
              注册
            </button>
            {error && <p className="text-xs text-red-400">{error}</p>}
            {msg && <p className="text-xs text-green-400">{msg}</p>}
          </div>
        </div>

        {/* 节点列表 */}
        <div className="bg-dark-800 border border-dark-700 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-dark-200 mb-3">节点清单（{nodes.length}）</h2>
          <div className="max-h-[28rem] overflow-y-auto space-y-1">
            {nodes.map((n) => {
              const cfg = nodeTypeConfig[n.node_type]
              return (
                <div
                  key={n.node_type}
                  className="flex items-center gap-2 px-2 py-1.5 rounded bg-dark-900/50 border border-dark-700 text-sm"
                >
                  <span>{cfg?.icon ?? '◆'}</span>
                  <span className="font-mono text-dark-100 flex-1 truncate">{n.node_type}</span>
                  <span className="text-xs text-dark-400">{n.category}</span>
                  {n.is_builtin ? (
                    <span className="text-xs px-1.5 py-0.5 rounded bg-dark-700 text-dark-300">内置</span>
                  ) : (
                    <button
                      onClick={async () => {
                        if (confirm(`删除自定义节点 ${n.node_type}？`)) {
                          await api.deleteNode(n.node_type)
                          qc.invalidateQueries({ queryKey: ['nodes'] })
                        }
                      }}
                      className="text-xs text-red-400 hover:text-red-300"
                    >
                      删除
                    </button>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      </div>
      <style>{`.input{background:#1e293b;border:1px solid #334155;border-radius:6px;padding:6px 10px;color:#e2e8f0}`}</style>
    </div>
  )
}
