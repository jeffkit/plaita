import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Building2, KeyRound, Trash2 } from 'lucide-react'
import { api } from '../services/api'
import { Page, PageHeader } from '../components/ui/Page'

const ROLES = ['admin', 'editor', 'viewer']

interface SecretOnce {
  title: string
  secretId: string
  secretKey: string
}

// 租户管理（平台管理员）：租户 CRUD、成员管理、契约密钥轮换。
export default function Tenants() {
  const qc = useQueryClient()
  const list = useQuery({ queryKey: ['tenants'], queryFn: () => api.getTenants() })
  const users = useQuery({ queryKey: ['users'], queryFn: () => api.getUsers() })
  const [creating, setCreating] = useState(false)
  const [form, setForm] = useState({ id: '', name: '' })
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [memberForm, setMemberForm] = useState({ username: '', role: 'viewer' })
  const [secretOnce, setSecretOnce] = useState<SecretOnce | null>(null)

  const invalidate = () => qc.invalidateQueries({ queryKey: ['tenants'] })

  const create = useMutation({
    mutationFn: () => api.createTenant({ id: form.id, name: form.name || undefined }),
    onSuccess: (t) => {
      setCreating(false)
      setForm({ id: '', name: '' })
      setError(null)
      invalidate()
      setSecretOnce({
        title: `租户 ${t.id} 的契约密钥（仅显示一次）`,
        secretId: t.contract_secret_id,
        secretKey: t.contract_secret_key,
      })
    },
    onError: (e) => setError((e as Error).message),
  })

  const del = useMutation({
    mutationFn: (id: string) => api.deleteTenant(id),
    onSuccess: () => {
      setError(null)
      invalidate()
    },
    onError: (e) => setError((e as Error).message),
  })

  const toggleStatus = useMutation({
    mutationFn: ({ id, status }: { id: string; status: 'active' | 'disabled' }) =>
      api.setTenantStatus(id, status),
    onSuccess: invalidate,
    onError: (e) => setError((e as Error).message),
  })

  const rotate = useMutation({
    mutationFn: (id: string) => api.rotateTenantSecret(id),
    onSuccess: (s) => {
      setSecretOnce({
        title: `租户 ${s.tenant_id} 的新契约密钥（仅显示一次）`,
        secretId: s.contract_secret_id,
        secretKey: s.contract_secret_key,
      })
      invalidate()
    },
    onError: (e) => setError((e as Error).message),
  })

  const members = useQuery({
    queryKey: ['tenant-members', expanded],
    queryFn: () => api.getTenantMembers(expanded!),
    enabled: !!expanded,
  })

  const addMember = useMutation({
    mutationFn: () => api.addTenantMember(expanded!, memberForm.username, memberForm.role),
    onSuccess: () => {
      setMemberForm({ username: '', role: 'viewer' })
      setError(null)
      members.refetch()
      invalidate()
    },
    onError: (e) => setError((e as Error).message),
  })

  const removeMember = useMutation({
    mutationFn: ({ tenantId, username }: { tenantId: string; username: string }) =>
      api.removeTenantMember(tenantId, username),
    onSuccess: () => {
      members.refetch()
      invalidate()
    },
    onError: (e) => setError((e as Error).message),
  })

  return (
    <Page>
      <PageHeader
        title="租户"
        subtitle="平台管理员：创建租户、管理成员与对外契约密钥（PlaitaClient 按租户拉取流程）"
      />
      <div className="mb-3 flex justify-end">
        <button
          onClick={() => setCreating((v) => !v)}
          className="flex items-center gap-1 text-caption bg-plaita-500 hover:bg-plaita-600 text-on-accent px-2.5 py-1.5 rounded-md"
        >
          <Building2 size={13} />
          新建租户
        </button>
      </div>

      {creating && (
        <div className="mb-4 p-4 rounded-lg bg-elevated border border-line space-y-2.5">
          <input
            placeholder="租户 ID（小写字母/数字/连字符，如 acme）"
            value={form.id}
            onChange={(e) => setForm((f) => ({ ...f, id: e.target.value }))}
            className="input w-full font-mono text-[12px]"
          />
          <input
            placeholder="展示名（可选，缺省同 ID）"
            value={form.name}
            onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
            className="input w-full text-[12px]"
          />
          {error && <p className="text-caption text-status-error">{error}</p>}
          <div className="flex justify-end gap-2">
            <button
              onClick={() => setCreating(false)}
              className="text-caption text-ink-muted hover:text-ink-primary px-3 py-1.5"
            >
              取消
            </button>
            <button
              onClick={() => create.mutate()}
              disabled={create.isPending || !form.id}
              className="bg-plaita-500 hover:bg-plaita-600 text-on-accent text-caption px-3 py-1.5 rounded-md"
            >
              创建
            </button>
          </div>
        </div>
      )}

      {!creating && error && <p className="text-caption text-status-error mb-2">{error}</p>}

      <div className="space-y-1.5">
        {(list.data?.tenants ?? []).map((t) => (
          <div key={t.id} className="rounded-lg bg-elevated border border-line">
            <div className="flex items-center gap-3 px-3 py-2.5">
              <button
                onClick={() => setExpanded(expanded === t.id ? null : t.id)}
                className="min-w-0 flex-1 text-left"
                title="展开成员管理"
              >
                <span className="font-mono text-caption text-ink-primary">{t.id}</span>
                {t.name !== t.id && (
                  <span className="ml-2 text-caption text-ink-secondary">{t.name}</span>
                )}
                <span className="ml-2 text-micro text-ink-faint">
                  {t.member_count} 成员 · secret {t.contract_secret_id || '未签发'}
                </span>
              </button>
              <span
                className={`text-micro px-1.5 py-0.5 rounded ${
                  t.status === 'active'
                    ? 'bg-plaita-500/10 text-plaita-400'
                    : 'bg-status-error/10 text-status-error'
                }`}
              >
                {t.status}
              </span>
              <button
                onClick={() => toggleStatus.mutate({
                  id: t.id,
                  status: t.status === 'active' ? 'disabled' : 'active',
                })}
                className="text-caption text-ink-muted hover:text-ink-primary px-1.5 py-1"
                title={t.status === 'active' ? '停用' : '启用'}
              >
                {t.status === 'active' ? '停用' : '启用'}
              </button>
              <button
                onClick={() => rotate.mutate(t.id)}
                className="text-ink-faint hover:text-plaita-400 px-1"
                title="轮换契约密钥"
              >
                <KeyRound size={13} />
              </button>
              <button
                onClick={() => del.mutate(t.id)}
                className="text-ink-faint hover:text-status-error px-1"
                title="删除租户（有流程时拒绝）"
              >
                <Trash2 size={13} />
              </button>
            </div>

            {expanded === t.id && (
              <div className="px-3 pb-3 pt-1 border-t border-line space-y-2">
                {(members.data?.members ?? []).map((m) => (
                  <div key={m.username} className="flex items-center gap-2">
                    <span className="font-mono text-caption text-ink-primary flex-1">
                      {m.username}
                    </span>
                    <select
                      value={m.role}
                      onChange={(e) =>
                        api
                          .setTenantMemberRole(t.id, m.username, e.target.value)
                          .then(() => members.refetch())
                      }
                      className="input w-32 text-caption py-1"
                    >
                      {ROLES.map((r) => (
                        <option key={r} value={r}>{r}</option>
                      ))}
                    </select>
                    <button
                      onClick={() => removeMember.mutate({ tenantId: t.id, username: m.username })}
                      className="text-ink-faint hover:text-status-error"
                      title="移出租户"
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                ))}
                <div className="flex items-center gap-2 pt-1">
                  <select
                    value={memberForm.username}
                    onChange={(e) => setMemberForm((f) => ({ ...f, username: e.target.value }))}
                    className="input flex-1 text-caption py-1"
                  >
                    <option value="">选择用户…</option>
                    {(users.data?.users ?? [])
                      .filter((u) => !(members.data?.members ?? []).some((m) => m.username === u.username))
                      .map((u) => (
                        <option key={u.username} value={u.username}>{u.username}</option>
                      ))}
                  </select>
                  <select
                    value={memberForm.role}
                    onChange={(e) => setMemberForm((f) => ({ ...f, role: e.target.value }))}
                    className="input w-32 text-caption py-1"
                  >
                    {ROLES.map((r) => (
                      <option key={r} value={r}>{r}</option>
                    ))}
                  </select>
                  <button
                    onClick={() => addMember.mutate()}
                    disabled={!memberForm.username || addMember.isPending}
                    className="text-caption bg-plaita-500/10 text-plaita-400 hover:bg-plaita-500/20 px-2.5 py-1 rounded-md disabled:opacity-40"
                  >
                    添加成员
                  </button>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {secretOnce && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
          <div className="w-[420px] p-5 rounded-xl bg-elevated border border-line space-y-3">
            <div className="text-caption font-semibold text-ink-primary">{secretOnce.title}</div>
            <div>
              <p className="text-micro text-ink-faint mb-0.5">Secret ID</p>
              <code className="block font-mono text-caption text-ink-primary break-all bg-surface rounded p-2">
                {secretOnce.secretId}
              </code>
            </div>
            <div>
              <p className="text-micro text-ink-faint mb-0.5">Secret Key</p>
              <code className="block font-mono text-caption text-ink-primary break-all bg-surface rounded p-2">
                {secretOnce.secretKey}
              </code>
            </div>
            <p className="text-micro text-status-error">
              关闭后无法再次查看，请立即保存到密钥管理设施。
            </p>
            <div className="flex justify-end">
              <button
                onClick={() => setSecretOnce(null)}
                className="bg-plaita-500 hover:bg-plaita-600 text-on-accent text-caption px-3 py-1.5 rounded-md"
              >
                我已保存
              </button>
            </div>
          </div>
        </div>
      )}
    </Page>
  )
}
