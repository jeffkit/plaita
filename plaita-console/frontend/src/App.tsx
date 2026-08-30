import { createBrowserRouter, RouterProvider, NavLink, Outlet } from 'react-router-dom'
import { useState } from 'react'
import {
  LayoutGrid,
  GitBranch,
  Play,
  ScrollText,
  Inbox,
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

// 侧边导航分组（DESIGN.md §1：micro 大写分组标签 + body 字号导航项）
const NAV_GROUPS = [
  {
    label: '总览',
    items: [
      { to: '/', icon: <LayoutGrid size={16} />, label: '仪表盘', end: true },
      { to: '/topology', icon: <GitBranch size={16} />, label: '服务拓扑' },
    ],
  },
  {
    label: '编排',
    items: [
      { to: '/flows', icon: <Workflow size={16} />, label: '流程编排' },
      { to: '/schedules', icon: <Clock size={16} />, label: '触发器' },
      { to: '/nodes', icon: <Boxes size={16} />, label: '节点管理' },
    ],
  },
  {
    label: '运行',
    items: [
      { to: '/executions', icon: <Play size={16} />, label: '执行实例' },
      { to: '/events', icon: <Zap size={16} />, label: '事件管理' },
      { to: '/logs', icon: <ScrollText size={16} />, label: '日志查看' },
      { to: '/queues', icon: <Inbox size={16} />, label: '任务队列' },
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
    ],
  },
])

function Layout() {
  // 侧边栏可折叠：展开 w-52，折叠 w-14 仅图标（状态存 localStorage）
  const [collapsed, setCollapsed] = useState(
    () => localStorage.getItem(NAV_COLLAPSED_KEY) === '1'
  )
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

        {/* 导航分组 */}
        <div className="flex-1 overflow-y-auto py-1">
          {NAV_GROUPS.map((group) => (
            <div key={group.label}>
              {!collapsed && (
                <p className="px-5 pt-4 pb-1.5 text-micro uppercase text-ink-faint">
                  {group.label}
                </p>
              )}
              {collapsed && <div className="h-2" />}
              {group.items.map((item) => (
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

      </nav>

      {/* 主内容区 */}
      <main className="flex-1 overflow-auto">
        <Outlet />
      </main>
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
  return <RouterProvider router={router} />
}
