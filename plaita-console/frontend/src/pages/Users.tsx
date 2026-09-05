import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Trash2, UserPlus } from 'lucide-react'
import { api } from '../services/api'
import { Page, PageHeader } from '../components/ui/Page'

const ROLES = ['admin', 'editor', 'viewer']

const ROLE_DESC: Record<string, string> = {
  admin: '全部权限（用户/凭据/集群/审计）',
  editor: '编排与执行（增删改流程、试跑、启动）',
  viewer: '只读（查看流程/执行/日志）',
}

// 用户管理（admin）：角色账号 CRUD。
export default function Users() {
  const qc = useQueryClient()
  const list = useQuery({ queryKey: ['users'], queryFn: () => api.getUsers() })
  const [creating, setCreating] = useState(false)
  const [form, setForm] = useState({ username: '', password: '', role: 'viewer' })
  const [error, setError] = useState<string | null>(null)

  const create = useMutation({
    mutationFn: () => api.createUser(form),
    onSuccess: () => {
      setCreating(false)
      setForm({ username: '', password: '', role: 'viewer' })
      setError(null)
      qc.invalidateQueries({ queryKey: ['users'] })
    },
    onError: (e) => setError((e as Error).message),
  })

  const del = useMutation({
    mutationFn: (username: string) => api.deleteUser(username),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['users'] }),
    onError: (e) => setError((e as Error).message),
  })

  const setRole = useMutation({
    mutationFn: ({ username, role }: { username: string; role: string }) =>
      api.setUserRole(username, role),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['users'] }),
    onError: (e) => setError((e as Error).message),
  })

  return (
    <Page>
      <PageHeader title="用户" subtitle="角色：admin 全部权限 / editor 编排与执行 / viewer 只读" />
      <div className="mb-3 flex justify-end">
        <button
          onClick={() => setCreating((v) => !v)}
          className="flex items-center gap-1 text-caption bg-plaita-500 hover:bg-plaita-600 text-on-accent px-2.5 py-1.5 rounded-md"
        >
          <UserPlus size={13} />
          新建用户
        </button>
      </div>

      {creating && (
        <div className="mb-4 p-4 rounded-lg bg-elevated border border-line space-y-2.5">
          <input
            placeholder="用户名"
            value={form.username}
            onChange={(e) => setForm((f) => ({ ...f, username: e.target.value }))}
            className="input w-full font-mono text-[12px]"
          />
          <input
            type="password"
            placeholder="密码（至少 8 位）"
            value={form.password}
            onChange={(e) => setForm((f) => ({ ...f, password: e.target.value }))}
            className="input w-full font-mono text-[12px]"
          />
          <select
            value={form.role}
            onChange={(e) => setForm((f) => ({ ...f, role: e.target.value }))}
            className="input w-full"
          >
            {ROLES.map((r) => (
              <option key={r} value={r}>
                {r} — {ROLE_DESC[r]}
              </option>
            ))}
          </select>
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
              disabled={create.isPending}
              className="bg-plaita-500 hover:bg-plaita-600 text-on-accent text-caption px-3 py-1.5 rounded-md"
            >
              创建
            </button>
          </div>
        </div>
      )}

      {!creating && error && <p className="text-caption text-status-error mb-2">{error}</p>}

      <div className="space-y-1.5">
        {(list.data?.users ?? []).map((u) => (
          <div
            key={u.username}
            className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-elevated border border-line"
          >
            <div className="min-w-0 flex-1">
              <span className="font-mono text-caption text-ink-primary">{u.username}</span>
            </div>
            <select
              value={u.role}
              onChange={(e) => setRole.mutate({ username: u.username, role: e.target.value })}
              className="input w-44 text-caption py-1"
              disabled={u.username === 'admin'}
            >
              {ROLES.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
            <button
              onClick={() => del.mutate(u.username)}
              disabled={u.username === 'admin'}
              className="text-ink-faint hover:text-status-error disabled:opacity-30"
              title={u.username === 'admin' ? '内置 admin 不可删除' : '删除用户'}
            >
              <Trash2 size={13} />
            </button>
          </div>
        ))}
      </div>
    </Page>
  )
}
