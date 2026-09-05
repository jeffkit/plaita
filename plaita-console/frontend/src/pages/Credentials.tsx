import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { KeyRound, Plus, Trash2 } from 'lucide-react'
import { api } from '../services/api'
import { Page, PageHeader } from '../components/ui/Page'

// 凭据管理：外部服务机密的集中加密存储，流程节点按名引用。
// 明文仅在保存/编辑回填瞬间出现，列表不含任何机密内容。
export default function Credentials() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<string | null>(null) // null=列表态，''=新建，名字=编辑
  const list = useQuery({
    queryKey: ['credentials'],
    queryFn: () => api.getCredentials(),
  })

  const del = useMutation({
    mutationFn: (name: string) => api.deleteCredential(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['credentials'] }),
  })

  const items = list.data?.credentials ?? []
  return (
    <Page>
      <PageHeader
        title="凭据"
        subtitle="外部服务机密的集中加密存储；流程节点按名引用，定义里不落明文"
      />
      <div className="mb-3 flex justify-end">
        <button
          onClick={() => setEditing('')}
          className="flex items-center gap-1 text-caption bg-plaita-500 hover:bg-plaita-600 text-on-accent px-2.5 py-1.5 rounded-md"
        >
          <Plus size={13} />
          新建凭据
        </button>
      </div>

      {editing !== null && (
        <CredentialForm
          name={editing || null}
          onDone={() => {
            setEditing(null)
            qc.invalidateQueries({ queryKey: ['credentials'] })
          }}
        />
      )}

      {editing === null && (
        <div className="space-y-1.5">
          {items.map((c) => (
            <div
              key={c.name}
              className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-elevated border border-line"
            >
              <KeyRound size={14} className="text-plaita-400 shrink-0" />
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-caption text-ink-primary">{c.name}</span>
                  <span className="text-[10px] text-ink-faint border border-line rounded px-1">
                    {c.type || 'generic'}
                  </span>
                </div>
                {c.desc && <p className="text-[11px] text-ink-muted truncate">{c.desc}</p>}
              </div>
              <button
                onClick={() => setEditing(c.name)}
                className="text-caption text-plaita-400 hover:text-plaita-300 shrink-0"
              >
                编辑
              </button>
              <button
                onClick={() => del.mutate(c.name)}
                className="text-ink-faint hover:text-status-error shrink-0"
                title="删除凭据"
              >
                <Trash2 size={13} />
              </button>
            </div>
          ))}
          {items.length === 0 && !list.isLoading && (
            <p className="text-caption text-ink-faint py-6 text-center">
              还没有凭据。新建后可在连接器节点的 credential 字段按名引用。
            </p>
          )}
        </div>
      )}
    </Page>
  )
}

function CredentialForm({ name, onDone }: { name: string | null; onDone: () => void }) {
  const qc = useQueryClient()
  const existing = useQuery({
    queryKey: ['credential', name],
    queryFn: () => api.getCredential(name as string),
    enabled: !!name,
  })

  const [name_, setName] = useState(name ?? '')
  const [type, setType] = useState('generic')
  const [desc, setDesc] = useState('')
  const [dataText, setDataText] = useState('{\n  \n}')
  const [loaded, setLoaded] = useState(false)

  // 编辑态：回填一次
  useEffect(() => {
    if (existing.data && !loaded) {
      setName(name as string)
      setType(existing.data.type)
      setDesc(existing.data.desc)
      setDataText(JSON.stringify(existing.data.data, null, 2))
      setLoaded(true)
    }
  }, [existing.data, loaded, name])

  const save = useMutation({
    mutationFn: async () => {
      let data: Record<string, unknown>
      try {
        data = JSON.parse(dataText)
      } catch (e) {
        throw new Error(`数据 JSON 非法: ${(e as Error).message}`)
      }
      if (!name_.trim()) throw new Error('凭据名不能为空')
      return api.saveCredential({ name: name_.trim(), type: type || 'generic', desc, data })
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['credentials'] })
      onDone()
    },
  })

  return (
    <div className="mb-4 p-4 rounded-lg bg-elevated border border-line space-y-2.5">
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-24">凭据名</span>
        <input
          value={name_}
          disabled={!!name}
          onChange={(e) => setName(e.target.value)}
          placeholder="如 feishu-bot-1（节点按名引用）"
          className="input flex-1 font-mono text-[12px] disabled:opacity-60"
        />
      </div>
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-24">类型标签</span>
        <input
          value={type}
          onChange={(e) => setType(e.target.value)}
          placeholder="webhook-bearer / database / generic"
          className="input flex-1 font-mono text-[12px]"
        />
      </div>
      <div className="flex items-start gap-2">
        <span className="text-caption text-ink-muted w-24 pt-2">用途说明</span>
        <input
          value={desc}
          onChange={(e) => setDesc(e.target.value)}
          className="input flex-1"
        />
      </div>
      <div>
        <span className="text-caption text-ink-muted">数据（JSON，保存后加密）</span>
        <textarea
          value={dataText}
          onChange={(e) => setDataText(e.target.value)}
          rows={5}
          className="input w-full font-mono text-[12px] mt-1"
        />
      </div>
      {save.isError && (
        <p className="text-caption text-status-error">{(save.error as Error).message}</p>
      )}
      <div className="flex gap-2 justify-end">
        <button
          onClick={onDone}
          className="text-caption text-ink-muted hover:text-ink-primary px-3 py-1.5"
        >
          取消
        </button>
        <button
          onClick={() => save.mutate()}
          disabled={save.isPending}
          className="bg-plaita-500 hover:bg-plaita-600 text-on-accent text-caption px-3 py-1.5 rounded-md disabled:opacity-50"
        >
          {save.isPending ? '保存中…' : '保存'}
        </button>
      </div>
    </div>
  )
}
