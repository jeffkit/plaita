import { useState } from 'react'
import { api, setSession } from '../services/api'

// 登录页：未持会话 token 时的全屏入口。
export default function Login({ onSuccess }: { onSuccess: () => void }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)

  const submit = async () => {
    if (!username || !password) {
      setError('请输入用户名和密码')
      return
    }
    setPending(true)
    try {
      const res = await api.login(username, password)
      setSession(res.token, res.username, res.role)
      onSuccess()
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setPending(false)
    }
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
