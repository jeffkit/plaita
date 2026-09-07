import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { KeyRound, Plus, Trash2 } from 'lucide-react'
import { api } from '../services/api'
import type { CredentialTemplate } from '../services/api'
import { Page, PageHeader } from '../components/ui/Page'

// 凭据管理（2026-09 重设计）：模板化表单取代裸 JSON——选中类型模板 → 类型化
// 字段（secret 掩码）→ 序列化回现有 data JSON 载荷，存储格式零变更；
// 「自定义 (JSON)」保留为兜底。模板来自后端注册表（GET /api/credential-templates）。
const CUSTOM_MODE = '__custom__'

export default function Credentials() {
  const qc = useQueryClient()
  const [editing, setEditing] = useState<string | null>(null) // null=列表态，''=新建，名字=编辑
  const list = useQuery({
    queryKey: ['credentials'],
    queryFn: () => api.getCredentials(),
  })
  const templatesQuery = useQuery({
    queryKey: ['credential-templates'],
    queryFn: () => api.getCredentialTemplates(),
    staleTime: 5 * 60_000,
  })

  const del = useMutation({
    mutationFn: (name: string) => api.deleteCredential(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['credentials'] }),
  })

  const items = list.data?.credentials ?? []
  const templates = templatesQuery.data?.templates ?? []

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
          templates={templates}
          templatesLoading={templatesQuery.isLoading}
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

function CredentialForm({
  name,
  templates,
  templatesLoading,
  onDone,
}: {
  name: string | null
  templates: CredentialTemplate[]
  templatesLoading: boolean
  onDone: () => void
}) {
  const qc = useQueryClient()
  const existing = useQuery({
    queryKey: ['credential', name],
    queryFn: () => api.getCredential(name as string),
    enabled: !!name,
  })

  const [name_, setName] = useState(name ?? '')
  const [typeSel, setTypeSel] = useState<string | null>(null)
  const [desc, setDesc] = useState('')
  const [fieldValues, setFieldValues] = useState<Record<string, string>>({})
  const [customText, setCustomText] = useState('{\n  \n}')
  const [loaded, setLoaded] = useState(false)

  const knownTypes = new Set(templates.map((t) => t.type))
  // 类型解析：编辑态等 existing 回填后锁定；新建默认第一个模板；无模板时自定义
  const effectiveType =
    typeSel ??
    (name && existing.data && !knownTypes.has(existing.data.type ?? '')
      ? CUSTOM_MODE
      : templates[0]?.type ?? CUSTOM_MODE)
  const template = templates.find((t) => t.type === effectiveType) ?? null
  const isCustom = effectiveType === CUSTOM_MODE || template === null

  // 编辑态：回填一次（模板字段从 data 预填；非模板历史类型整体进自定义 JSON）
  useEffect(() => {
    if (existing.data && !loaded) {
      setName(name as string)
      setTypeSel(knownTypes.has(existing.data.type ?? '') ? existing.data.type : CUSTOM_MODE)
      setDesc(existing.data.desc ?? '')
      const data = (existing.data.data ?? {}) as Record<string, unknown>
      setFieldValues(Object.fromEntries(Object.entries(data).map(([k, v]) => [k, String(v)])))
      setCustomText(JSON.stringify(data ?? {}, null, 2))
      setLoaded(true)
    }
  }, [existing.data, loaded, name])

  const save = useMutation({
    mutationFn: async () => {
      let data: Record<string, unknown>
      if (isCustom) {
        try {
          data = JSON.parse(customText)
        } catch (e) {
          throw new Error(`数据 JSON 非法: ${(e as Error).message}`)
        }
      } else {
        data = {}
        for (const f of template!.fields) {
          const v = (fieldValues[f.key] ?? '').trim()
          if (f.required && v === '') throw new Error(`「${f.label}」为必填`)
          if (v !== '') data[f.key] = f.input_type === 'number' ? Number(v) : v
        }
        // 模板外历史字段原样保留（轮换不丢配置）
        for (const [k, v] of Object.entries(fieldValues)) {
          if (!(template!.fields ?? []).some((f) => f.key === k) && v !== '') {
            data[k] = v
          }
        }
      }
      if (!name_.trim()) throw new Error('凭据名不能为空')
      const type = isCustom ? (effectiveType === CUSTOM_MODE ? 'generic' : effectiveType) : template!.type
      return api.saveCredential({ name: name_.trim(), type, desc, data })
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['credentials'] })
      onDone()
    },
  })

  const setField = (key: string, v: string) => setFieldValues((s) => ({ ...s, [key]: v }))

  return (
    <div className="mb-4 p-4 rounded-lg bg-elevated border border-line space-y-2.5">
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-24 shrink-0">凭据名</span>
        <input
          value={name_}
          disabled={!!name}
          onChange={(e) => setName(e.target.value)}
          placeholder="如 feishu-bot-1（节点按名引用）"
          className="input flex-1 font-mono text-data-sm disabled:opacity-60"
        />
      </div>
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-24 shrink-0">类型</span>
        <select
          value={effectiveType}
          onChange={(e) => setTypeSel(e.target.value)}
          className="input flex-1"
        >
          {templates.map((t) => (
            <option key={t.type} value={t.type}>
              {t.label}（{t.type}）
            </option>
          ))}
          {!isCustom && effectiveType !== CUSTOM_MODE && !knownTypes.has(effectiveType) && (
            <option value={effectiveType}>{effectiveType}（历史类型）</option>
          )}
          <option value={CUSTOM_MODE}>自定义（JSON）</option>
        </select>
      </div>
      {template?.desc && <p className="ml-24 text-[11px] text-ink-faint">{template.desc}</p>}
      <div className="flex items-center gap-2">
        <span className="text-caption text-ink-muted w-24 shrink-0">用途说明</span>
        <input value={desc} onChange={(e) => setDesc(e.target.value)} className="input flex-1" />
      </div>

      {isCustom ? (
        <div>
          <span className="text-caption text-ink-muted">数据（JSON，保存后加密）</span>
          <textarea
            value={customText}
            onChange={(e) => setCustomText(e.target.value)}
            rows={5}
            className="input w-full font-mono text-data-sm mt-1"
          />
        </div>
      ) : (
        <div className="space-y-1.5">
          <span className="text-caption text-ink-muted">模板字段</span>
          {templatesLoading && <p className="text-caption text-ink-faint">模板加载中…</p>}
          {(template?.fields ?? []).map((f) => (
            <div key={f.key} className="flex items-center gap-2">
              <span className="text-caption text-ink-muted w-24 shrink-0 text-right">
                {f.label}
                {f.required && <span className="text-status-error ml-0.5">*</span>}
              </span>
              <input
                type={f.secret ? 'password' : f.input_type === 'number' ? 'number' : 'text'}
                value={fieldValues[f.key] ?? ''}
                onChange={(e) => setField(f.key, e.target.value)}
                autoComplete={f.secret ? 'new-password' : 'off'}
                className="input flex-1 font-mono text-data-sm"
              />
            </div>
          ))}
        </div>
      )}

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
