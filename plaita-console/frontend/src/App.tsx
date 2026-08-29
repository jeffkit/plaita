import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import { useState } from 'react'
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

// 组件
import ClusterSwitcher from './components/ClusterSwitcher'
import ThemeToggle from './components/ThemeToggle'

// 侧边导航分组（DESIGN.md §1：micro 大写分组标签 + body 字号导航项）
const NAV_GROUPS = [
  {
    label: '总览',
    items: [
      { to: '/', icon: <LayoutGrid size={16} />, label: '仪表盘', end: true },
      { to: '/cluster', icon: <Server size={16} />, label: '集群管理' },
      { to: '/topology', icon: <GitBranch size={16} />, label: '服务拓扑' },
    ],
  },
  {
    label: '编排',
    items: [
      { to: '/flows', icon: <Workflow size={16} />, label: '流程编排' },
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

function App() {
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
    <BrowserRouter>
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

          {/* 底部 - 集群切换器 */}
          <div className="p-2 border-t border-line">
            <ClusterSwitcher collapsed={collapsed} />
          </div>
        </nav>

        {/* 主内容区 */}
        <main className="flex-1 overflow-auto">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/cluster" element={<Cluster />} />
            <Route path="/topology" element={<Topology />} />
            <Route path="/executions" element={<Executions />} />
            <Route path="/executions/:executionId" element={<ExecutionDetail />} />
            <Route path="/events" element={<Events />} />
            <Route path="/logs" element={<Logs />} />
            <Route path="/queues" element={<Queues />} />
            <Route path="/flows" element={<Flows />} />
            <Route path="/flows/:flowId/edit" element={<FlowEditor />} />
            <Route path="/nodes" element={<Nodes />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
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

export default App
