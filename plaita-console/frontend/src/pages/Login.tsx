import { useState } from 'react'
import { api, ApiError, errorUserMessage, setSession, type MembershipInfo } from '../services/api'

// 登录页：未持会话 token 时的全屏入口。
// 多租户成员登录后若属于多个租户，进入租户选择步骤（单租户直进）。
export default function Login({ onSuccess }: { onSuccess: () => void }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)
  // 租户选择态：login 拿到 memberships 后进入；token 只存组件内存
  const [pendingSession, setPendingSession] = useState<{
    token: string; username: string; role: string
    platformAdmin: boolean; memberships: MembershipInfo[]
  } | null>(null)
  const [picking, setPicking] = useState<string | null>(null)
  const [pickError, setPickError] = useState<string | null>(null)

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
      setError(errorUserMessage(e))
    } finally {
      setPending(false)
    }
  }

  /**
   * S17-01：租户选择必须先在服务端切换成功，才把会话落到 localStorage。
   * 登录期 token 还只在内存里，必须显式传；401 也不能触发整页跳登录
   * （authRedirect:false），否则选择页会被冲掉。
   */
  const pickTenant = async (tenantId: string) => {
    if (!pendingSession || picking) return
    setPicking(tenantId)
    setPickError(null)
    try {
      const res = await api.switchTenant(tenantId, {
        token: pendingSession.token,
        authRedirect: false,
      })
      setSession(pendingSession.token, pendingSession.username, res.role, {
        tenantId: res.tenant_id ?? tenantId,
        platformAdmin: res.platform_admin ?? pendingSession.platformAdmin,
        memberships: pendingSession.memberships,
      })
      onSuccess()
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) {
        // token 只在内存：清掉选择态回凭据表单，不写任何 localStorage
        setPendingSession(null)
        setPickError(null)
        setError('登录已过期，请重新登录')
      } else {
        // 400/超时/网络/5xx：留在选择页可重选
        setPickError(errorUserMessage(e))
        console.warn('[login] 租户切换失败:', errorUserMessage(e), e)
      }
    } finally {
      setPicking(null)
    }
  }

  const backToLogin = () => {
    setPendingSession(null)
    setPickError(null)
    setPassword('')
    setError(null)
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
                disabled={picking !== null}
                className="w-full flex items-center justify-between px-3 py-2 rounded-md border border-line hover:border-plaita-400 disabled:opacity-50 disabled:cursor-not-allowed text-caption"
              >
                <span className="text-ink-primary">{m.tenant_id}</span>
                <span className="text-ink-muted">
                  {picking === m.tenant_id ? '选择中…' : m.role}
                </span>
              </button>
            ))}
          </div>
          {pickError && <p className="text-caption text-status-error">{pickError}</p>}
          <button
            onClick={backToLogin}
            disabled={picking !== null}
            className="w-full text-caption text-ink-muted hover:text-ink-primary disabled:opacity-50"
          >
            返回登录
          </button>
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
