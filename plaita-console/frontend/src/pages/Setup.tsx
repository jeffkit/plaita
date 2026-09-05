import { useState } from 'react'
import { api, setSession } from '../services/api'

// 首次启动向导：users 表为空时创建管理员（成功即自动登录）。
export default function Setup({ onSuccess }: { onSuccess: () => void }) {
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)

  const submit = async () => {
    if (!username.trim()) return setError('请输入管理员用户名')
    if (password.length < 8) return setError('密码至少 8 位')
    if (password !== confirm) return setError('两次输入的密码不一致')
    setPending(true)
    try {
      const res = await api.setup({ username: username.trim(), password })
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
      <div className="w-96 p-6 rounded-xl bg-elevated border border-line space-y-4">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-plaita-400" />
          <div>
            <div className="text-[15px] font-semibold tracking-tight text-ink-primary">
              初始化 Plaita Console
            </div>
            <p className="text-caption text-ink-muted">创建管理员账号，完成首次部署</p>
          </div>
        </div>

        <div>
          <label className="text-caption text-ink-muted mb-1 block">管理员用户名</label>
          <input
            autoFocus
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="input w-full font-mono"
          />
        </div>
        <div>
          <label className="text-caption text-ink-muted mb-1 block">密码（至少 8 位）</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="input w-full"
          />
        </div>
        <div>
          <label className="text-caption text-ink-muted mb-1 block">确认密码</label>
          <input
            type="password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && submit()}
            className="input w-full"
          />
        </div>

        {error && <p className="text-caption text-status-error">{error}</p>}

        <button
          onClick={submit}
          disabled={pending}
          className="w-full bg-plaita-500 hover:bg-plaita-600 disabled:opacity-50 text-on-accent text-caption py-2 rounded-md"
        >
          {pending ? '创建中…' : '创建管理员并进入'}
        </button>
        <p className="text-[11px] text-ink-faint leading-4">
          无人值守部署可改用环境变量 PLAITA_CONSOLE_ADMIN_PASSWORD 跳过本向导。
        </p>
      </div>
    </div>
  )
}
