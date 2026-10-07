import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus } from 'lucide-react'
import { api } from '../services/api'
import { Page, PageHeader, Card, Button, EmptyState, Table, Th, Tr, Td, TdData } from '../components/ui'

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
    <Page>
      <PageHeader title="流程编排" subtitle="可视化编排与版本管理 Plaita 流程定义" />

      {/* 新建流程 */}
      <Card className="p-4">
        <h2 className="text-section text-ink-primary mb-3">新建流程</h2>
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
          <Button
            variant="primary"
            onClick={() => createMutation.mutate()}
            disabled={!newId || createMutation.isPending}
          >
            <Plus size={14} />
            创建
          </Button>
        </div>
        {error && <p className="text-caption text-status-error mt-2">{error}</p>}
      </Card>

      {/* 流程列表 */}
      <Card className="overflow-hidden">
        <Table>
          <thead>
            <tr>
              <Th>flow_id</Th>
              <Th className="w-full">描述</Th>
              <Th className="whitespace-nowrap">作者</Th>
              <Th className="whitespace-nowrap">更新时间</Th>
              <Th className="text-right whitespace-nowrap">操作</Th>
            </tr>
          </thead>
          <tbody>
            {flows.length === 0 && (
              <tr>
                <td colSpan={5}>
                  <EmptyState message="暂无流程" hint="在上方创建第一个流程" />
                </td>
              </tr>
            )}
            {flows.map((f) => (
              <Tr key={f.flow_id}>
                <TdData className="text-ink-primary whitespace-nowrap">
                  <span className="block max-w-[220px] truncate" title={f.flow_id}>
                    {f.flow_id}
                  </span>
                </TdData>
                <Td className="break-words" title={f.desc || undefined}>
                  {f.desc || '-'}
                </Td>
                <Td className="whitespace-nowrap">
                  <span className="block max-w-[160px] truncate" title={f.author || undefined}>
                    {f.author || '-'}
                  </span>
                </Td>
                <TdData className="text-ink-muted whitespace-nowrap">
                  {f.updated_at ? new Date(f.updated_at).toLocaleString() : '-'}
                </TdData>
                <Td className="text-right whitespace-nowrap">
                  <button
                    onClick={() => navigate(`/flows/${f.flow_id}/edit`)}
                    className="text-plaita-400 hover:text-plaita-300 transition-colors mr-3"
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
                    className="text-status-error hover:opacity-80 transition-opacity"
                  >
                    删除
                  </button>
                </Td>
              </Tr>
            ))}
          </tbody>
        </Table>
      </Card>
    </Page>
  )
}
