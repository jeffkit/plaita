import { useState } from 'react'
import { api, setSession, type MembershipInfo } from '../services/api'

// 登录页：未持会话 token 时的全屏入口。
// 多租户成员登录后若属于多个租户，进入租户选择步骤（单租户直进）。
export default function Login({ onSuccess }: { onSuccess: () => void }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)
  // 租户选择态：login 拿到 memberships 后进入
  const [pendingSession, setPendingSession] = useState<{
    token: string; username: string; role: string
    platformAdmin: boolean; memberships: MembershipInfo[]
  } | null>(null)

  const submit = async () => {
    if (!username || !password) {
      setError('请输入用户名和密码')
      return
    }
    setPending(true)
    try {
      const res = await api.login(username, password)
      const memberships = res.memberships || []
      if ((res.platform_admin && memberships.length === 0) || memberships.length <= 1) {
        // 单租户（或纯平台管理员）直进
        setSession(res.token, res.username, res.role, {
          tenantId: res.active_tenant || memberships[0]?.tenant_id || '',
          platformAdmin: res.platform_admin,
          memberships,
        })
        onSuccess()
      } else {
        setPendingSession({
          token: res.token,
          username: res.username,
          role: res.role,
          platformAdmin: !!res.platform_admin,
          memberships,
        })
      }
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setPending(false)
    }
  }

  const pickTenant = (tenantId: string) => {
    if (!pendingSession) return
    const m = pendingSession.memberships.find((x) => x.tenant_id === tenantId)
    setSession(pendingSession.token, pendingSession.username, m?.role || pendingSession.role, {
      tenantId,
      platformAdmin: pendingSession.platformAdmin,
      memberships: pendingSession.memberships,
    })
    onSuccess()
  }

  if (pendingSession) {
    return (
      <div className="h-screen flex items-center justify-center bg-surface">
        <div className="w-80 p-6 rounded-xl bg-elevated border border-line space-y-4">
          <div>
            <div className="text-[15px] font-semibold tracking-tight text-ink-primary">
              选择租户
            </div>
            <p className="text-caption text-ink-muted">
              {pendingSession.username} 属于 {pendingSession.memberships.length} 个租户
            </p>
          </div>
          <div className="space-y-2">
            {pendingSession.memberships.map((m) => (
              <button
                key={m.tenant_id}
                onClick={() => pickTenant(m.tenant_id)}
                className="w-full flex items-center justify-between px-3 py-2 rounded-md border border-line hover:border-plaita-400 text-caption"
              >
                <span className="text-ink-primary">{m.tenant_id}</span>
                <span className="text-ink-muted">{m.role}</span>
              </button>
            ))}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="h-screen flex items-center justify-center bg-surface">
      <div className="w-80 p-6 rounded-xl bg-elevated border border-line space-y-4">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-plaita-400" />
          <div>
            <div className="text-[15px] font-semibold tracking-tight text-ink-primary">
              Plaita Console
            </div>
            <p className="text-caption text-ink-muted">流程引擎管理台</p>
          </div>
        </div>

        <div>
          <label className="text-caption text-ink-muted mb-1 block">用户名</label>
          <input
            autoFocus
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && submit()}
            className="input w-full"
            placeholder="用户名"
          />
        </div>
        <div>
          <label className="text-caption text-ink-muted mb-1 block">密码</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && submit()}
            className="input w-full"
            placeholder="密码"
          />
        </div>

        {error && <p className="text-caption text-status-error">{error}</p>}

        <button
          onClick={submit}
          disabled={pending}
          className="w-full bg-plaita-500 hover:bg-plaita-600 disabled:opacity-50 text-on-accent text-caption py-2 rounded-md"
        >
          {pending ? '登录中…' : '登录'}
        </button>
      </div>
    </div>
  )
}
