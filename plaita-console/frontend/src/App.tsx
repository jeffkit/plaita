import { KeyRound, LogOut, UserCog, Building2 } from 'lucide-react'
import Credentials from './pages/Credentials'
import Users from './pages/Users'
import Tenants from './pages/Tenants'
import Audit from './pages/Audit'
import Login from './pages/Login'
import Setup from './pages/Setup'
import {
  api,
  clearSession,
  getRole,
  getTenant,
  getMemberships,
  isPlatformAdmin,
  setTenant,
} from './services/api'
import { createBrowserRouter, RouterProvider, NavLink, Outlet, useNavigate } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  LayoutGrid,
  GitBranch,
  Play,
  ScrollText,
  Inbox,
  Server,
  Zap,
  Workflow,
  Boxes,
  Clock,
  PanelLeftClose,
  PanelLeftOpen,
} from 'lucide-react'

// 页面组件
import Dashboard from './pages/Dashboard'
import Topology from './pages/Topology'
import Executions from './pages/Executions'
import ExecutionDetail from './pages/ExecutionDetail'
import Logs from './pages/Logs'
import Queues from './pages/Queues'
import Cluster from './pages/Cluster'
import Events from './pages/Events'
import Flows from './pages/Flows'
import FlowEditor from './pages/FlowEditor'
import Nodes from './pages/Nodes'
import Schedules from './pages/Schedules'

// 组件
import ClusterSwitcher from './components/ClusterSwitcher'
import ThemeToggle from './components/ThemeToggle'

// 侧边导航分组（2026-09 信息架构定案·方案 A）：
// 编排域=设计时（流程/触发器/节点），运行域=运行态只读观测（执行/事件/日志/
// 队列/拓扑/审计），管理域=admin 供给的共享资源（凭据/用户/集群）。
// 审计归观测是域归属（只读事件流），admin-only 是权限（ADMIN_ONLY_PATHS），
// 二者正交。定义与运行的存储本就分离（定义库 SQLite / 运行态 Redis）。
interface NavItem {
  to: string
  icon: React.ReactNode
  label: string
  end?: boolean
}

const ADMIN_ONLY_PATHS = new Set(['/credentials', '/cluster', '/users', '/audit', '/tenants'])

const NAV_GROUPS: Array<{ label: string; items: NavItem[] }> = [
  {
    label: '总览',
    items: [
      { to: '/', icon: <LayoutGrid size={16} />, label: '仪表盘', end: true },
    ],
  },
  {
    label: '编排 · 定义',
    items: [
      { to: '/flows', icon: <Workflow size={16} />, label: '流程编排' },
      { to: '/schedules', icon: <Clock size={16} />, label: '触发器' },
      { to: '/nodes', icon: <Boxes size={16} />, label: '节点管理' },
    ],
  },
  {
    label: '运行 · 观测',
    items: [
      { to: '/executions', icon: <Play size={16} />, label: '执行实例' },
      { to: '/events', icon: <Zap size={16} />, label: '事件管理' },
      { to: '/logs', icon: <ScrollText size={16} />, label: '日志查看' },
      { to: '/queues', icon: <Inbox size={16} />, label: '任务队列' },
      { to: '/topology', icon: <GitBranch size={16} />, label: '服务拓扑' },
      { to: '/audit', icon: <ScrollText size={16} />, label: '审计' },
    ],
  },
  {
    label: '平台 · 管理',
    items: [
      { to: '/credentials', icon: <KeyRound size={16} />, label: '凭据' },
      { to: '/users', icon: <UserCog size={16} />, label: '用户' },
      { to: '/tenants', icon: <Building2 size={16} />, label: '租户' },
      { to: '/cluster', icon: <Server size={16} />, label: '集群管理' },
    ],
  },
]

// 侧边栏折叠状态持久化键
const NAV_COLLAPSED_KEY = 'plaita-nav-collapsed'

// data router：FlowEditor 的未保存拦截（useBlocker）依赖它
const router = createBrowserRouter([
  {
    path: '/',
    element: <Layout />,
    children: [
      { index: true, element: <Dashboard /> },
      { path: 'cluster', element: <Cluster /> },
      { path: 'topology', element: <Topology /> },
      { path: 'executions', element: <Executions /> },
      { path: 'executions/:executionId', element: <ExecutionDetail /> },
      { path: 'events', element: <Events /> },
      { path: 'logs', element: <Logs /> },
      { path: 'queues', element: <Queues /> },
      { path: 'flows', element: <Flows /> },
      { path: 'schedules', element: <Schedules /> },
      { path: 'flows/:flowId/edit', element: <FlowEditor /> },
      { path: 'nodes', element: <Nodes /> },
      { path: 'credentials', element: <Credentials /> },
      { path: 'users', element: <Users /> },
      { path: 'tenants', element: <Tenants /> },
      { path: 'audit', element: <Audit /> },
    ],
  },
])

function Layout() {
  // 侧边栏可折叠：展开 w-52，折叠 w-14 仅图标（状态存 localStorage）
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(NAV_COLLAPSED_KEY) === '1'
  )
  const isAdmin = getRole() === 'admin'
  const username = localStorage.getItem('plaita_username') || ''
  const logout = async () => {
    try {
      await api.logout()
    } catch {
      /* 会话已无效也照常退出 */
    }
    clearSession()
    window.location.assign('/login')
  }
  const toggleCollapsed = () =>
    setCollapsed((v) => {
      localStorage.setItem(NAV_COLLAPSED_KEY, v ? '0' : '1')
      return !v
    })

  return (
    <div className="flex h-screen">
      {/* 侧边导航 */}
      <nav
        className={`${
          collapsed ? 'w-14' : 'w-52'
        } shrink-0 bg-surface border-r border-line flex flex-col transition-[width] duration-200`}
      >
        {/* Logo：品牌绿只留一颗指示点（DESIGN.md §2.4） */}
        <div
          className={`${
            collapsed ? 'px-2 justify-center' : 'px-4'
          } pt-4 pb-3 border-b border-line flex items-center justify-between gap-2`}
        >
          <div className="flex items-center gap-2 min-w-0">
            <span className="w-2 h-2 rounded-full bg-plaita-400 shrink-0" />
            {!collapsed && (
              <div className="min-w-0">
                <div className="text-[15px] font-semibold tracking-tight text-ink-primary whitespace-nowrap">
                  Plaita Console
                </div>
                <p className="text-caption text-ink-muted whitespace-nowrap">流程引擎管理台</p>
              </div>
            )}
          </div>
          {!collapsed && <ThemeToggle />}
          <button
            onClick={toggleCollapsed}
            title={collapsed ? '展开菜单' : '收起菜单'}
            className="p-1.5 rounded-md text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors shrink-0"
          >
            {collapsed ? <PanelLeftOpen size={15} /> : <PanelLeftClose size={15} />}
          </button>
        </div>

        {/* 集群上下文：全局切换器置顶——切换影响整个 console（执行/拓扑/服务视图） */}
        <div className="p-2 border-b border-line">
          <ClusterSwitcher collapsed={collapsed} placement="top" />
        </div>

        {/* 租户上下文：多租户成员的当前租户切换器（平台管理员可切到任意租户/全量） */}
        <TenantSwitcher collapsed={collapsed} />

        {/* 导航分组 */}
        <div className="flex-1 overflow-y-auto py-1">
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className={group.label === '平台 · 管理' && !isAdmin ? 'hidden' : ''}>
              {!collapsed && (
                <p className="px-5 pt-4 pb-1.5 text-micro uppercase text-ink-faint">
                  {group.label}
                </p>
              )}
              {collapsed && <div className="h-2" />}
              {group.items
                .filter((item) => isAdmin || !ADMIN_ONLY_PATHS.has(item.to))
                .map((item) => (
                  <NavItem
                    key={item.to}
                    to={item.to}
                    end={item.end}
                    icon={item.icon}
                    label={item.label}
                    collapsed={collapsed}
                  />
                ))}
            </div>
          ))}
        </div>

        {/* 用户菜单 */}
        <div className={`p-2 border-t border-line ${collapsed ? 'flex justify-center' : 'flex items-center justify-between gap-2'}`}>
          {!collapsed && (
            <span className="text-caption text-ink-muted truncate" title={username}>
              {username || '未登录'}
            </span>
          )}
          <button
            onClick={logout}
            className="text-ink-faint hover:text-status-error shrink-0"
            title="退出登录"
          >
            <LogOut size={14} />
          </button>
        </div>

      </nav>

      {/* 主内容区 */}
      <main className="flex-1 overflow-auto">
        <Outlet />
      </main>
    </div>
  )
}

// 租户切换器：列出当前用户的全部租户成员关系；切换后清空查询缓存整页刷新。
// 平台管理员额外提供「全量视角」选项（不带租户上下文，跨租户读）。
function TenantSwitcher({ collapsed }: { collapsed: boolean }) {
  const memberships = getMemberships()
  const platformAdmin = isPlatformAdmin()
  const current = getTenant()
  const navigate = useNavigate()
  const qc = useQueryClient()
  if (!platformAdmin && memberships.length <= 1) return null

  const switchTo = async (tenantId: string) => {
    if (tenantId === current) return
    if (tenantId === '') {
      // 平台管理员的「全量视角」：纯本地状态（服务端以无 X-Tenant-ID 表示）
      setTenant('')
    } else {
      try {
        const res = await api.switchTenant(tenantId)
        setTenant(tenantId, res.role)
      } catch {
        setTenant(tenantId)
      }
    }
    qc.clear()
    navigate('/')
    window.location.reload()
  }

  const options = memberships.map((m) => ({ id: m.tenant_id, role: m.role }))

  return (
    <div className="p-2 border-b border-line">
      {collapsed ? (
        <div
          className="flex justify-center py-1.5 text-micro text-ink-faint"
          title={`租户: ${current || '全量'}`}
        >
          <Building2 size={14} />
        </div>
      ) : (
        <select
          value={current}
          onChange={(e) => switchTo(e.target.value)}
          className="input w-full text-caption py-1.5"
          title="切换租户"
        >
          {platformAdmin && <option value="">全量视角（平台）</option>}
          {options.map((m) => (
            <option key={m.id} value={m.id}>
              {m.id}（{m.role}）
            </option>
          ))}
          {current && !options.some((m) => m.id === current) && (
            <option value={current}>{current}</option>
          )}
        </select>
      )}
    </div>
  )
}

// 导航项组件
function NavItem({
  to,
  end,
  icon,
  label,
  collapsed = false
}: {
  to: string
  end?: boolean
  icon: React.ReactNode
  label: string
  collapsed?: boolean
}) {
  return (
    <NavLink
      to={to}
      end={end}
      title={collapsed ? label : undefined}
      className={({ isActive }) =>
        `flex items-center gap-2.5 rounded-lg text-body transition-colors duration-150 ${
          collapsed ? 'justify-center mx-2 px-0 py-2' : 'px-3 py-2 mx-3'
        } ${
          isActive
            ? 'bg-plaita-500/10 text-plaita-400'
            : 'text-ink-secondary hover:bg-elevated hover:text-ink-primary'
        }`
      }
    >
      <span className="shrink-0">{icon}</span>
      {!collapsed && <span>{label}</span>}
    </NavLink>
  )
}

export default function App() {
  const [token, setToken] = useState(() => localStorage.getItem('plaita_token'))
  const [needsSetup, setNeedsSetup] = useState<boolean | null>(null)

  // 无会话时探测：users 为空 → 首次启动向导；否则登录页
  useEffect(() => {
    if (!token) {
      api.setupStatus()
        .then((r) => setNeedsSetup(r.needs_setup))
        .catch(() => setNeedsSetup(false))
    }
  }, [token])

  if (!token) {
    if (needsSetup === null) {
      return <div className="h-screen bg-surface" />
    }
    return needsSetup ? (
      <Setup onSuccess={() => setToken(localStorage.getItem('plaita_token'))} />
    ) : (
      <Login onSuccess={() => setToken(localStorage.getItem('plaita_token'))} />
    )
  }
  return <RouterProvider router={router} />
}
