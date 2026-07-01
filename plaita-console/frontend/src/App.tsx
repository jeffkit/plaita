import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
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

function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen">
        {/* 侧边导航 */}
        <nav className="w-64 bg-dark-900/80 backdrop-blur-sm border-r border-dark-700 flex flex-col">
          {/* Logo */}
          <div className="p-6 border-b border-dark-700">
            <h1 className="text-2xl font-bold gradient-text">Plaita Console</h1>
            <p className="text-dark-400 text-sm mt-1">流程引擎管理台</p>
          </div>
          
          {/* 导航链接 */}
          <div className="flex-1 py-4">
            <NavItem to="/" icon={<LayoutGrid size={20} />} label="仪表盘" />
            <NavItem to="/cluster" icon={<Server size={20} />} label="集群管理" />
            <NavItem to="/topology" icon={<GitBranch size={20} />} label="服务拓扑" />
            <NavItem to="/flows" icon={<Workflow size={20} />} label="流程编排" />
            <NavItem to="/nodes" icon={<Boxes size={20} />} label="节点管理" />
            <NavItem to="/executions" icon={<Play size={20} />} label="执行实例" />
            <NavItem to="/events" icon={<Zap size={20} />} label="事件管理" />
            <NavItem to="/logs" icon={<ScrollText size={20} />} label="日志查看" />
            <NavItem to="/queues" icon={<Inbox size={20} />} label="任务队列" />
          </div>
          
          {/* 底部 - 集群切换器 */}
          <div className="p-3 border-t border-dark-700">
            <ClusterSwitcher />
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
  icon, 
  label 
}: { 
  to: string
  icon: React.ReactNode
  label: string 
}) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `flex items-center gap-3 px-6 py-3 mx-2 rounded-lg transition-all ${
          isActive
            ? 'bg-plaita-500/20 text-plaita-400 border-l-2 border-plaita-500'
            : 'text-dark-300 hover:bg-dark-700 hover:text-dark-100'
        }`
      }
    >
      {icon}
      <span>{label}</span>
    </NavLink>
  )
}

export default App

