import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../services/api'

export default function Flows() {
  const navigate = useNavigate()
  const qc = useQueryClient()
  const [newId, setNewId] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [error, setError] = useState<string | null>(null)

  const flowsQuery = useQuery({
    queryKey: ['flows'],
    queryFn: () => api.getFlows(),
  })

  const createMutation = useMutation({
    mutationFn: () => api.createFlow({ flow_id: newId, desc: newDesc }),
    onSuccess: () => {
      setError(null)
      setNewId('')
      setNewDesc('')
      qc.invalidateQueries({ queryKey: ['flows'] })
    },
    onError: (e: Error) => setError(e.message),
  })

  const flows = flowsQuery.data?.flows || []

  return (
    <div className="p-6 max-w-5xl">
      <h1 className="text-2xl font-bold text-dark-100 mb-1">流程编排</h1>
      <p className="text-dark-400 text-sm mb-6">可视化编排与版本管理 Plaita 流程定义</p>

      {/* 新建流程 */}
      <div className="bg-dark-800 border border-dark-700 rounded-lg p-4 mb-6">
        <h2 className="text-sm font-semibold text-dark-200 mb-3">新建流程</h2>
        <div className="flex gap-2 items-center">
          <input
            value={newId}
            onChange={(e) => setNewId(e.target.value)}
            placeholder="flow_id，如 echo"
            className="input w-56"
          />
          <input
            value={newDesc}
            onChange={(e) => setNewDesc(e.target.value)}
            placeholder="描述"
            className="input w-64"
          />
          <button
            onClick={() => createMutation.mutate()}
            disabled={!newId || createMutation.isPending}
            className="bg-plaita-600 hover:bg-plaita-500 disabled:opacity-50 text-white px-4 py-2 rounded"
          >
            创建
          </button>
        </div>
        {error && <p className="text-xs text-red-400 mt-2">{error}</p>}
      </div>

      {/* 流程列表 */}
      <div className="bg-dark-800 border border-dark-700 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-dark-900/60 text-dark-400">
            <tr>
              <th className="text-left px-4 py-2">flow_id</th>
              <th className="text-left px-4 py-2">描述</th>
              <th className="text-left px-4 py-2">作者</th>
              <th className="text-left px-4 py-2">更新时间</th>
              <th className="text-right px-4 py-2">操作</th>
            </tr>
          </thead>
          <tbody>
            {flows.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-dark-400">
                  暂无流程，先在上方创建
                </td>
              </tr>
            )}
            {flows.map((f) => (
              <tr key={f.flow_id} className="border-t border-dark-700 hover:bg-dark-700/40">
                <td className="px-4 py-2 text-dark-100 font-mono">{f.flow_id}</td>
                <td className="px-4 py-2 text-dark-300">{f.desc || '-'}</td>
                <td className="px-4 py-2 text-dark-300">{f.author || '-'}</td>
                <td className="px-4 py-2 text-dark-400">
                  {f.updated_at ? new Date(f.updated_at).toLocaleString() : '-'}
                </td>
                <td className="px-4 py-2 text-right">
                  <button
                    onClick={() => navigate(`/flows/${f.flow_id}/edit`)}
                    className="text-plaita-400 hover:text-plaita-300 mr-3"
                  >
                    编辑
                  </button>
                  <button
                    onClick={async () => {
                      if (confirm(`删除流程 ${f.flow_id} 及其全部版本？`)) {
                        await api.deleteFlow(f.flow_id)
                        qc.invalidateQueries({ queryKey: ['flows'] })
                      }
                    }}
                    className="text-red-400 hover:text-red-300"
                  >
                    删除
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <style>{`.input{background:#1e293b;border:1px solid #334155;border-radius:6px;padding:6px 10px;color:#e2e8f0}`}</style>
    </div>
  )
}
