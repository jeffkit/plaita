import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'
import { Page, PageHeader } from '../components/ui/Page'

// 审计日志（admin）：管理面敏感操作留痕。
export default function Audit() {
  const list = useQuery({ queryKey: ['audit'], queryFn: () => api.getAudit() })
  const logs = list.data?.logs ?? []

  return (
    <Page>
      <PageHeader title="审计" subtitle="管理面敏感操作留痕（最近 200 条）" />
      <div className="rounded-lg border border-line overflow-hidden">
        <table className="w-full text-caption">
          <thead>
            <tr className="bg-elevated text-ink-muted text-left">
              <th className="px-3 py-2">时间</th>
              <th className="px-3 py-2">操作人</th>
              <th className="px-3 py-2">动作</th>
              <th className="px-3 py-2">对象</th>
              <th className="px-3 py-2">详情</th>
              <th className="px-3 py-2">IP</th>
            </tr>
          </thead>
          <tbody>
            {logs.map((l, i) => (
              <tr key={i} className="border-t border-line text-ink-secondary">
                <td className="px-3 py-1.5 font-mono text-[11px] whitespace-nowrap">{l.ts?.slice(0, 19)}</td>
                <td className="px-3 py-1.5 font-mono">{l.actor}</td>
                <td className="px-3 py-1.5 font-mono text-plaita-300">{l.action}</td>
                <td className="px-3 py-1.5 font-mono truncate max-w-48">{l.resource_id}</td>
                <td className="px-3 py-1.5 font-mono text-[11px] truncate max-w-56">
                  {Object.keys(l.detail || {}).length ? JSON.stringify(l.detail) : '—'}
                </td>
                <td className="px-3 py-1.5 font-mono text-[11px]">{l.ip || '—'}</td>
              </tr>
            ))}
            {logs.length === 0 && (
              <tr><td colSpan={6} className="px-3 py-6 text-center text-ink-faint">暂无审计记录</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </Page>
  )
}
