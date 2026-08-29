/**
 * 集群切换器组件
 * 在侧边栏底部显示当前集群，支持切换和管理多个集群
 */

import { useState, useRef, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { 
  Server, 
  ChevronUp, 
  Check, 
  Plus, 
  Settings, 
  Trash2,
  X,
  Loader2
} from 'lucide-react'
import { api, ClusterInfo, CreateClusterRequest } from '../services/api'

// 创建集群对话框
function CreateClusterDialog({ 
  isOpen, 
  onClose, 
  onCreated 
}: { 
  isOpen: boolean
  onClose: () => void
  onCreated: (cluster: ClusterInfo) => void
}) {
  const [formData, setFormData] = useState<CreateClusterRequest>({
    id: '',
    name: '',
    description: '',
    redis_url: 'redis://localhost:6379/0'
  })
  const [error, setError] = useState('')
  
  const createMutation = useMutation({
    mutationFn: api.createCluster,
    onSuccess: (cluster) => {
      onCreated(cluster)
      onClose()
      setFormData({ id: '', name: '', description: '', redis_url: 'redis://localhost:6379/0' })
    },
    onError: (err: Error) => {
      setError(err.message)
    }
  })

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-dark-800 rounded-lg border border-dark-700 w-[480px] max-w-[90vw]">
        <div className="flex items-center justify-between p-4 border-b border-dark-700">
          <h3 className="text-lg font-semibold">创建新集群</h3>
          <button onClick={onClose} className="p-1 hover:bg-dark-700 rounded">
            <X className="w-4 h-4" />
          </button>
        </div>
        
        <form onSubmit={(e) => { e.preventDefault(); createMutation.mutate(formData) }}>
          <div className="p-4 space-y-4">
            {error && (
              <div className="p-3 rounded bg-red-500/20 text-red-400 text-sm">{error}</div>
            )}
            
            <div>
              <label className="block text-sm text-dark-400 mb-1">集群 ID *</label>
              <input
                type="text"
                value={formData.id}
                onChange={(e) => setFormData({ ...formData, id: e.target.value })}
                placeholder="例如: production, staging"
                className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-700 
                           focus:border-plaita-500 focus:outline-none"
                required
              />
            </div>
            
            <div>
              <label className="block text-sm text-dark-400 mb-1">显示名称 *</label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                placeholder="例如: 生产环境集群"
                className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-700 
                           focus:border-plaita-500 focus:outline-none"
                required
              />
            </div>
            
            <div>
              <label className="block text-sm text-dark-400 mb-1">描述</label>
              <input
                type="text"
                value={formData.description}
                onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                placeholder="集群用途描述"
                className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-700 
                           focus:border-plaita-500 focus:outline-none"
              />
            </div>
            
            <div>
              <label className="block text-sm text-dark-400 mb-1">Redis URL</label>
              <input
                type="text"
                value={formData.redis_url}
                onChange={(e) => setFormData({ ...formData, redis_url: e.target.value })}
                placeholder="redis://localhost:6379/0"
                className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-700 
                           focus:border-plaita-500 focus:outline-none font-mono text-sm"
              />
            </div>
          </div>
          
          <div className="flex justify-end gap-2 p-4 border-t border-dark-700">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 rounded bg-dark-700 hover:bg-dark-600"
            >
              取消
            </button>
            <button
              type="submit"
              disabled={createMutation.isPending || !formData.id || !formData.name}
              className="px-4 py-2 rounded bg-plaita-500 hover:bg-plaita-600 
                         disabled:opacity-50 disabled:cursor-not-allowed
                         flex items-center gap-2"
            >
              {createMutation.isPending && <Loader2 className="w-4 h-4 animate-spin" />}
              创建
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

export default function ClusterSwitcher({ collapsed = false }: { collapsed?: boolean }) {
  const [isOpen, setIsOpen] = useState(false)
  const [showCreateDialog, setShowCreateDialog] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const queryClient = useQueryClient()

  // 获取集群列表
  const { data: clustersData, isLoading } = useQuery({
    queryKey: ['clusters'],
    queryFn: api.getClusters,
    refetchInterval: 30000,
  })

  // 切换集群
  const switchMutation = useMutation({
    mutationFn: api.switchCluster,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['clusters'] })
      queryClient.invalidateQueries({ queryKey: ['cluster-info'] })
      queryClient.invalidateQueries({ queryKey: ['service-types'] })
      queryClient.invalidateQueries({ queryKey: ['managed-instances'] })
      queryClient.invalidateQueries({ queryKey: ['topology'] })
      setIsOpen(false)
    }
  })

  // 删除集群
  const deleteMutation = useMutation({
    mutationFn: api.deleteCluster,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['clusters'] })
    }
  })

  // 点击外部关闭下拉菜单
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const activeCluster = clustersData?.clusters.find(c => c.is_active)
  const clusters = clustersData?.clusters || []

  return (
    <>
      <div className="relative" ref={dropdownRef}>
        {/* 当前集群显示：折叠态仅图标 + 活跃指示点 */}
        <button
          onClick={() => setIsOpen(!isOpen)}
          title={collapsed ? `集群：${activeCluster?.name || '未选择'}` : undefined}
          className={
            collapsed
              ? 'w-full flex justify-center p-2 rounded-lg bg-dark-800/50 hover:bg-dark-800 border border-dark-700 transition-colors'
              : 'w-full flex items-center gap-3 p-3 rounded-lg \n                     bg-dark-800/50 hover:bg-dark-800 border border-dark-700\n                     transition-colors group'
          }
        >
          <div className="relative p-2 rounded-lg bg-plaita-500/20">
            <Server className="w-4 h-4 text-plaita-400" />
            {collapsed && activeCluster && (
              <span className="absolute -top-0.5 -right-0.5 w-2 h-2 rounded-full bg-plaita-400" />
            )}
          </div>
          {!collapsed && (
            <>
              <div className="flex-1 text-left min-w-0">
                {isLoading ? (
                  <div className="text-sm text-dark-500">加载中...</div>
                ) : activeCluster ? (
                  <>
                    <div className="text-sm font-medium truncate">{activeCluster.name}</div>
                    <div className="text-xs text-dark-500 truncate">{activeCluster.id}</div>
                  </>
                ) : (
                  <div className="text-sm text-dark-500">未选择集群</div>
                )}
              </div>
              <ChevronUp className={`w-4 h-4 text-dark-500 transition-transform ${isOpen ? '' : 'rotate-180'}`} />
            </>
          )}
        </button>

        {/* 下拉菜单：折叠态用固定宽度，避免被窄容器压缩 */}
        {isOpen && (
          <div className={`absolute bottom-full left-0 mb-2 
                          bg-dark-800 border border-dark-700 rounded-lg shadow-xl
                          max-h-[300px] overflow-y-auto z-50 ${
                            collapsed ? 'w-60' : 'right-0'
                          }`}>
            {/* 集群列表 */}
            <div className="p-1">
              {clusters.map((cluster) => (
                <div
                  key={cluster.id}
                  className={`flex items-center gap-2 p-2 rounded-md cursor-pointer
                              ${cluster.is_active 
                                ? 'bg-plaita-500/20 text-plaita-400' 
                                : 'hover:bg-dark-700'}`}
                  onClick={() => {
                    if (!cluster.is_active) {
                      switchMutation.mutate(cluster.id)
                    }
                  }}
                >
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium truncate">{cluster.name}</div>
                    <div className="text-xs text-dark-500 truncate">
                      {cluster.description || cluster.redis_url}
                    </div>
                  </div>
                  {cluster.is_active && <Check className="w-4 h-4 flex-shrink-0" />}
                  {!cluster.is_active && clusters.length > 1 && (
                    <button
                      onClick={(e) => {
                        e.stopPropagation()
                        if (confirm(`确定删除集群 "${cluster.name}" 吗？`)) {
                          deleteMutation.mutate(cluster.id)
                        }
                      }}
                      className="p-1 hover:bg-red-500/20 rounded text-dark-500 hover:text-red-400"
                    >
                      <Trash2 className="w-3 h-3" />
                    </button>
                  )}
                </div>
              ))}
            </div>

            {/* 分隔线 */}
            <div className="border-t border-dark-700 my-1" />

            {/* 操作按钮 */}
            <div className="p-1">
              <button
                onClick={() => {
                  setIsOpen(false)
                  setShowCreateDialog(true)
                }}
                className="w-full flex items-center gap-2 p-2 rounded-md 
                           text-sm text-dark-400 hover:bg-dark-700 hover:text-ink-primary"
              >
                <Plus className="w-4 h-4" />
                创建新集群
              </button>
              
              {activeCluster && (
                <button
                  onClick={() => {
                    setIsOpen(false)
                    // 导航到集群配置页面
                    window.location.href = `/cluster?tab=config`
                  }}
                  className="w-full flex items-center gap-2 p-2 rounded-md 
                             text-sm text-dark-400 hover:bg-dark-700 hover:text-ink-primary"
                >
                  <Settings className="w-4 h-4" />
                  管理集群配置
                </button>
              )}
            </div>
          </div>
        )}
      </div>

      {/* 创建集群对话框 */}
      <CreateClusterDialog
        isOpen={showCreateDialog}
        onClose={() => setShowCreateDialog(false)}
        onCreated={() => {
          queryClient.invalidateQueries({ queryKey: ['clusters'] })
        }}
      />
    </>
  )
}

