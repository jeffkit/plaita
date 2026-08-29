import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../services/api'
import { nodeTypeConfig } from '../components/flow/nodeTypes'
import { Page, PageHeader, Card, Button, EmptyState } from '../components/ui'

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
    <Page className="max-w-5xl">
      <PageHeader title="节点管理" subtitle="查看内置节点、注册自定义节点描述（用于编排表单）" />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* 注册表单 */}
        <Card className="p-4">
          <h2 className="text-section text-ink-primary mb-3">注册自定义节点</h2>
          <div className="space-y-2">
            <input
              value={form.node_type}
              onChange={(e) => setForm({ ...form, node_type: e.target.value })}
              placeholder="node_type（不可与内置冲突）"
              className="input"
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
              className="input font-mono text-data-sm"
              placeholder="节点字段 schema (JSON)"
            />
            <Button
              variant="primary"
              className="w-full"
              onClick={() => registerMut.mutate()}
              disabled={!form.node_type || registerMut.isPending}
            >
              注册
            </Button>
            {error && <p className="text-caption text-status-error">{error}</p>}
            {msg && <p className="text-caption text-status-success">{msg}</p>}
          </div>
        </Card>

        {/* 节点清单 */}
        <Card className="p-4">
          <h2 className="text-section text-ink-primary mb-3">
            节点清单<span className="ml-1.5 font-mono text-data-sm text-ink-muted tabular-nums">{nodes.length}</span>
          </h2>
          {nodes.length === 0 ? (
            <EmptyState message="暂无节点" />
          ) : (
            <div className="max-h-[28rem] overflow-y-auto space-y-1">
              {nodes.map((n) => {
                const cfg = nodeTypeConfig[n.node_type]
                return (
                  <div
                    key={n.node_type}
                    className="flex items-center gap-2 px-2 py-1.5 rounded-md bg-inset border border-line text-caption"
                  >
                    <span>{cfg?.icon ?? '◆'}</span>
                    <span className="font-mono text-data-sm text-ink-primary flex-1 truncate">{n.node_type}</span>
                    <span className="text-ink-muted">{n.category}</span>
                    {n.is_builtin ? (
                      <span className="px-1.5 py-0.5 rounded bg-elevated border border-line text-ink-muted">内置</span>
                    ) : (
                      <button
                        onClick={async () => {
                          if (confirm(`删除自定义节点 ${n.node_type}？`)) {
                            await api.deleteNode(n.node_type)
                            qc.invalidateQueries({ queryKey: ['nodes'] })
                          }
                        }}
                        className="text-status-error hover:opacity-80 transition-opacity"
                      >
                        删除
                      </button>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </Card>
      </div>
    </Page>
  )
}
