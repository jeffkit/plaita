import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { api, ServiceTypeInfo, ManagedInstance, InfrastructureInfo, InfrastructureTemplate, CreateInfrastructureRequest, QuickTestResponse } from '../services/api'
import {
  Play,
  Square,
  RefreshCw,
  Server,
  Box,
  CheckCircle,
  XCircle,
  AlertCircle,
  Trash2,
  Settings,
  Save,
  FileCode,
  FileText,
  Database,
  HardDrive,
  X,
  Plus,
  Edit,
  Eye,
  EyeOff,
  Zap,
  Clock,
  UserCheck
} from 'lucide-react'
import { Button, ConfirmDialog } from '../components/ui'

// 状态图标
const StatusIcon = ({ status }: { status: string }) => {
  switch (status) {
    case 'running':
      return <CheckCircle className="w-5 h-5 text-plaita-400" />
    case 'stopped':
      return <Square className="w-5 h-5 text-dark-400" />
    case 'error':
      return <XCircle className="w-5 h-5 text-status-error" />
    case 'starting':
      return <RefreshCw className="w-5 h-5 text-status-warning animate-spin" />
    default:
      return <AlertCircle className="w-5 h-5 text-status-warning" />
  }
}

// 服务类型卡片
function ServiceTypeCard({ 
  serviceType,
  onStart,
  isStarting 
}: { 
  serviceType: ServiceTypeInfo
  onStart: () => void
  isStarting: boolean
}) {
  const canStart = serviceType.running_count < serviceType.max_instances

  return (
    <div className="bg-dark-800 rounded-lg border border-dark-700 p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Server className="w-5 h-5 text-plaita-400" />
          <span className="font-medium">{serviceType.display_name}</span>
        </div>
        <span className="text-xs text-dark-400">
          {serviceType.running_count} / {serviceType.max_instances}
        </span>
      </div>
      
      <p className="text-sm text-dark-400 mb-3">
        类型: <code className="text-plaita-400">{serviceType.service_type}</code>
      </p>
      
      <div className="flex items-center gap-2">
        <Button
          variant="primary"
          size="sm"
          onClick={onStart}
          disabled={!canStart || isStarting}
        >
          {isStarting ? (
            <>
              <RefreshCw className="w-3.5 h-3.5 animate-spin" />
              启动中…
            </>
          ) : (
            <>
              <Play className="w-3.5 h-3.5" />
              启动实例
            </>
          )}
        </Button>
      </div>
    </div>
  )
}

// 托管实例行
function ManagedInstanceRow({ 
  instance,
  onStop,
  isStopping,
  onViewError,
  onRemove,
  isRemoving,
  onViewLogs
}: { 
  instance: ManagedInstance
  onStop: () => void
  isStopping: boolean
  onViewError: (error: string) => void
  onRemove: () => void
  isRemoving: boolean
  onViewLogs: () => void
}) {
  const isExternal = instance.managed_by === 'external'
  
  return (
    <tr className={`border-b border-dark-700 hover:bg-dark-800/50 ${isExternal ? 'opacity-80' : ''}`}>
      <td className="py-3 px-4">
        <div className="flex items-center gap-2">
          <StatusIcon status={instance.status} />
          <span className="font-mono text-sm">{instance.instance_id}</span>
          {isExternal && (
            <span className="px-1.5 py-0.5 rounded text-[10px] bg-blue-500/20 text-blue-400">
              外部
            </span>
          )}
        </div>
      </td>
      <td className="py-3 px-4">
        <span className="px-2 py-0.5 rounded bg-dark-700 text-sm">
          {instance.service_type}
        </span>
      </td>
      <td className="py-3 px-4 text-sm text-dark-400">
        {instance.pid ? `PID: ${instance.pid}` : instance.container_id || '-'}
      </td>
      <td className="py-3 px-4">
        <span className={`
          px-2 py-0.5 rounded text-xs font-medium
          ${instance.status === 'running' ? 'bg-plaita-500/20 text-plaita-400' : ''}
          ${instance.status === 'stopped' ? 'bg-dark-700 text-dark-400' : ''}
          ${instance.status === 'error' ? 'bg-status-error-dim text-status-error' : ''}
          ${instance.status === 'starting' ? 'bg-status-warning-dim text-status-warning' : ''}
        `}>
          {instance.status}
        </span>
      </td>
      <td className="py-3 px-4 text-sm text-dark-400">
        {instance.start_time ? new Date(instance.start_time).toLocaleString() : '-'}
      </td>
      <td className="py-3 px-4">
        <div className="flex items-center gap-2">
          {/* 查看日志按钮 - 所有实例都可以查看 */}
          <button
            onClick={onViewLogs}
            className="flex items-center gap-1 px-2 py-1 rounded text-xs 
                       bg-dark-700 text-dark-300 hover:bg-dark-600 hover:text-ink-primary"
            title="查看日志"
          >
            <FileText className="w-3 h-3" />
            日志
          </button>
          {/* 外部实例显示提示 */}
          {isExternal && (
            <span className="text-xs text-dark-500">外部管理</span>
          )}
          {/* 控制台托管的实例可以停止 */}
          {!isExternal && instance.status === 'running' && (
            <button
              onClick={onStop}
              disabled={isStopping}
              className="flex items-center gap-1 px-2 py-1 rounded text-xs 
                         bg-status-error-dim text-status-error hover:bg-status-error/20 
                         disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isStopping ? (
                <RefreshCw className="w-3 h-3 animate-spin" />
              ) : (
                <Square className="w-3 h-3" />
              )}
              停止
            </button>
          )}
          {instance.error_message && (
            <button 
              onClick={() => onViewError(instance.error_message!)}
              className="text-xs text-status-error hover:text-red-300 underline cursor-pointer"
            >
              查看错误
            </button>
          )}
          {/* 控制台托管且非运行状态的实例可以移除 */}
          {!isExternal && instance.status !== 'running' && (
            <button
              onClick={onRemove}
              disabled={isRemoving}
              className="flex items-center gap-1 px-2 py-1 rounded text-xs 
                         bg-dark-700 text-dark-400 hover:bg-dark-600 hover:text-ink-primary
                         disabled:opacity-50 disabled:cursor-not-allowed"
              title="移除记录"
            >
              {isRemoving ? (
                <RefreshCw className="w-3 h-3 animate-spin" />
              ) : (
                <Trash2 className="w-3 h-3" />
              )}
              移除
            </button>
          )}
        </div>
      </td>
    </tr>
  )
}

// 错误详情弹窗
function ErrorDialog({ 
  error, 
  onClose 
}: { 
  error: string
  onClose: () => void 
}) {
  return (
    <div className="fixed inset-0 animate-fade bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className="bg-dark-800 rounded-lg border border-dark-700 p-6 max-w-2xl w-full mx-4 max-h-[80vh] overflow-auto"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-section text-ink-primary text-status-error">错误详情</h3>
          <button 
            onClick={onClose}
            className="text-dark-400 hover:text-ink-primary"
          >
            ✕
          </button>
        </div>
        <pre className="bg-dark-900 p-4 rounded text-sm text-red-300 whitespace-pre-wrap overflow-auto">
          {error}
        </pre>
        <div className="mt-4 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded bg-dark-700 hover:bg-dark-600 text-sm"
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  )
}

// 日志级别颜色
const levelColors: Record<string, string> = {
  DEBUG: 'text-gray-400',
  INFO: 'text-blue-400',
  WARNING: 'text-status-warning',
  ERROR: 'text-status-error',
  CRITICAL: 'text-red-500 font-bold',
}

// 日志查看弹窗
function LogDialog({ 
  instanceId,
  serviceType,
  onClose 
}: { 
  instanceId: string
  serviceType: string
  onClose: () => void 
}) {
  const [level, setLevel] = useState<string>('')
  
  const { data: logsData, isLoading, refetch } = useQuery({
    queryKey: ['instanceLogs', instanceId, level],
    queryFn: () => api.getInstanceLogs(instanceId, { level: level || undefined, limit: 100 }),
    refetchInterval: 5000,
  })

  return (
    <div className="fixed inset-0 animate-fade bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className="bg-dark-800 rounded-lg border border-dark-700 p-6 max-w-4xl w-full mx-4 max-h-[85vh] flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <FileText className="w-5 h-5 text-plaita-400" />
            <h3 className="text-section text-ink-primary">服务日志</h3>
            <span className="text-sm text-dark-400">
              {serviceType} / {instanceId}
            </span>
          </div>
          <button 
            onClick={onClose}
            className="text-dark-400 hover:text-ink-primary"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        
        {/* 筛选器 */}
        <div className="flex items-center gap-4 mb-4">
          <select
            value={level}
            onChange={e => setLevel(e.target.value)}
            className="px-3 py-1.5 rounded bg-dark-700 border border-dark-600 text-sm"
          >
            <option value="">全部级别</option>
            <option value="DEBUG">DEBUG</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
          </select>
          <button
            onClick={() => refetch()}
            className="flex items-center gap-1 px-3 py-1.5 rounded bg-dark-700 hover:bg-dark-600 text-sm"
          >
            <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            刷新
          </button>
          <span className="text-sm text-dark-400 ml-auto">
            共 {logsData?.total || 0} 条日志
          </span>
        </div>
        
        {/* 日志列表 */}
        <div className="flex-1 overflow-auto bg-dark-900 rounded border border-dark-700">
          {isLoading ? (
            <div className="flex items-center justify-center h-48">
              <RefreshCw className="w-8 h-8 text-plaita-400 animate-spin" />
            </div>
          ) : logsData && logsData.logs.length > 0 ? (
            <div className="font-mono text-sm">
              {logsData.logs.map((log, idx) => (
                <div 
                  key={idx}
                  className="flex items-start gap-2 px-3 py-1.5 hover:bg-dark-800 border-b border-dark-800"
                >
                  <span className="text-dark-500 text-xs whitespace-nowrap">
                    {new Date(log.timestamp).toLocaleTimeString()}
                  </span>
                  <span className={`text-xs font-medium w-16 ${levelColors[log.level] || 'text-gray-400'}`}>
                    {log.level}
                  </span>
                  <span className="flex-1 text-gray-200 break-all">
                    {log.message}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-48 text-dark-400">
              <FileText className="w-12 h-12 mb-2 opacity-50" />
              <p>暂无日志记录</p>
              <p className="text-sm mt-1">日志会在服务运行时自动产生</p>
            </div>
          )}
        </div>
        
        <div className="mt-4 flex justify-between items-center">
          <a 
            href={`/logs?instance_id=${instanceId}`}
            className="text-sm text-plaita-400 hover:text-plaita-300"
          >
            在日志页面查看更多 →
          </a>
          <button
            onClick={onClose}
            className="px-4 py-2 rounded bg-dark-700 hover:bg-dark-600 text-sm"
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  )
}

// 基础设施状态图标
const InfraStatusIcon = ({ status }: { status: string }) => {
  switch (status) {
    case 'healthy':
      return <CheckCircle className="w-5 h-5 text-plaita-400" />
    case 'unhealthy':
      return <XCircle className="w-5 h-5 text-status-error" />
    case 'disabled':
      return <Square className="w-5 h-5 text-dark-400" />
    default:
      return <AlertCircle className="w-5 h-5 text-status-warning" />
  }
}

// 基础设施类型图标
const InfraTypeIcon = ({ type }: { type: string }) => {
  switch (type) {
    case 'redis':
      return <Database className="w-5 h-5 text-status-error" />
    case 'kafka':
      return <Server className="w-5 h-5 text-orange-400" />
    case 'database':
      return <HardDrive className="w-5 h-5 text-blue-400" />
    default:
      return <Box className="w-5 h-5 text-gray-400" />
  }
}

// 基础设施卡片
function InfrastructureCard({ 
  infra,
  onRefresh,
  onEdit,
  onDelete: _onDelete
}: { 
  infra: InfrastructureInfo
  onRefresh: () => void
  onEdit: () => void
  onDelete: () => void
}) {
  const checkMutation = useMutation({
    mutationFn: () => api.checkInfrastructureHealth(infra.name),
    onSuccess: onRefresh,
  })

  const [actionMsg, setActionMsg] = useState<string | null>(null)

  const startMutation = useMutation({
    mutationFn: () => api.startInfrastructure(infra.name),
    onSuccess: (res) => {
      onRefresh()
      setActionMsg(res.message)
    },
    onError: (e: Error) => setActionMsg(e.message),
  })

  const stopMutation = useMutation({
    mutationFn: () => api.stopInfrastructure(infra.name),
    onSuccess: (res) => {
      onRefresh()
      setActionMsg(res.message)
    },
    onError: (e: Error) => setActionMsg(e.message),
  })

  const deleteMutation = useMutation({
    mutationFn: () => api.deleteInfrastructure(infra.name),
    onSuccess: onRefresh,
  })

  return (
    <div className={`bg-dark-800 rounded-lg border p-4 ${
      infra.status === 'healthy' ? 'border-plaita-500/30' : 
      infra.status === 'unhealthy' ? 'border-status-error/30' :
      'border-dark-700'
    }`}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <InfraTypeIcon type={infra.type} />
          <span className="font-medium">{infra.display_name}</span>
        </div>
        <div className="flex items-center gap-2">
          <InfraStatusIcon status={infra.status} />
          <button
            onClick={onEdit}
            className="p-1 rounded hover:bg-dark-600 text-dark-400 hover:text-ink-primary"
            title="编辑配置"
          >
            <Edit className="w-4 h-4" />
          </button>
        </div>
      </div>
      
      <div className="text-sm text-dark-400 space-y-1 mb-3">
        <p>类型: <code className="text-plaita-400">{infra.type}</code></p>
        {infra.url && (
          <p className="truncate" title={infra.url}>
            URL: <code className="text-dark-300">{infra.url}</code>
          </p>
        )}
        {infra.bootstrap_servers && (
          <p>
            Servers: <code className="text-dark-300">{infra.bootstrap_servers}</code>
          </p>
        )}
        {!infra.enabled && (
          <p className="text-status-warning">已禁用</p>
        )}
      </div>

      {infra.details && typeof infra.details === 'object' && (
        <div className="text-xs text-dark-500 bg-dark-900 rounded p-2 mb-3">
          {Object.entries(infra.details).map(([key, value]) => (
            <div key={key} className="flex justify-between">
              <span>{key}:</span>
              <span className="text-dark-300">{String(value)}</span>
            </div>
          ))}
        </div>
      )}
      
      {actionMsg && (
        <div className="text-xs text-dark-300 bg-dark-900 rounded p-2 mb-2 break-all">{actionMsg}</div>
      )}

      <div className="flex gap-2">
        {infra.status === 'healthy' ? (
          <button
            onClick={() => stopMutation.mutate()}
            disabled={stopMutation.isPending}
            className="flex items-center gap-1 px-3 py-1.5 rounded text-sm flex-1 justify-center
                       bg-dark-700 hover:bg-status-error-dim hover:text-status-error"
            title="停止容器（保留，可再次启动）"
          >
            {stopMutation.isPending ? (
              <RefreshCw className="w-4 h-4 animate-spin" />
            ) : (
              <Square className="w-4 h-4" />
            )}
            停止
          </button>
        ) : (
          <button
            onClick={() => startMutation.mutate()}
            disabled={startMutation.isPending || !infra.enabled}
            className={`flex items-center gap-1 px-3 py-1.5 rounded text-sm flex-1 justify-center
                        ${infra.enabled
                          ? 'bg-plaita-500/20 hover:bg-plaita-500/30 text-plaita-400 border border-plaita-500/30'
                          : 'bg-dark-800 text-dark-500 cursor-not-allowed border border-dark-700'}`}
            title={infra.enabled
              ? (infra.docker ? '用配置的 docker 镜像拉起并等待健康' : '未配置 docker 镜像，无法容器化拉起')
              : '基础设施已禁用，启用后方可启动'}
          >
            {startMutation.isPending ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                启动中…
              </>
            ) : (
              <>
                <Play className="w-4 h-4" />
                启动
              </>
            )}
          </button>
        )}
        <button
          onClick={() => checkMutation.mutate()}
          disabled={checkMutation.isPending || !infra.enabled}
          className={`
            flex items-center gap-1 px-3 py-1.5 rounded text-sm flex-1 justify-center
            ${infra.enabled
              ? 'bg-dark-700 hover:bg-dark-600 text-white'
              : 'bg-dark-800 text-dark-500 cursor-not-allowed'
            }
          `}
        >
          {checkMutation.isPending ? (
            <>
              <RefreshCw className="w-4 h-4 animate-spin" />
              检测中…
            </>
          ) : (
            <>
              <RefreshCw className="w-4 h-4" />
              检测
            </>
          )}
        </button>
        <button
          onClick={() => {
            if (confirm(`确定要删除 ${infra.display_name} 吗？`)) {
              deleteMutation.mutate()
            }
          }}
          disabled={deleteMutation.isPending}
          className="flex items-center gap-1 px-3 py-1.5 rounded text-sm
                     bg-dark-700 hover:bg-status-error-dim text-dark-400 hover:text-status-error"
          title="删除"
        >
          <Trash2 className="w-4 h-4" />
        </button>
      </div>
    </div>
  )
}

// 基础设施配置表单弹窗
function InfrastructureFormDialog({
  infra,
  templates,
  onClose,
  onSave
}: {
  infra?: InfrastructureInfo
  templates: InfrastructureTemplate[]
  onClose: () => void
  onSave: () => void
}) {
  const queryClient = useQueryClient()
  const isEdit = !!infra
  
  const [formData, setFormData] = useState<CreateInfrastructureRequest>({
    name: infra?.name || '',
    display_name: infra?.display_name || '',
    type: infra?.type || 'redis',
    enabled: infra?.enabled ?? true,
    url: infra?.url || '',
    bootstrap_servers: infra?.bootstrap_servers || '',
    docker: infra?.docker || {}
  })
  
  const [selectedTemplate, setSelectedTemplate] = useState<string>(infra?.name || '')
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [isCustom, setIsCustom] = useState(isEdit && !templates.find(t => t.name === infra?.name))

  const createMutation = useMutation({
    mutationFn: (data: CreateInfrastructureRequest) => api.createInfrastructure(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['infrastructure'] })
      onSave()
      onClose()
    },
  })

  const updateMutation = useMutation({
    mutationFn: (data: CreateInfrastructureRequest) => 
      api.updateInfrastructure(infra!.name, {
        display_name: data.display_name,
        enabled: data.enabled,
        url: data.url,
        bootstrap_servers: data.bootstrap_servers,
        docker: data.docker
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['infrastructure'] })
      onSave()
      onClose()
    },
  })

  const handleTemplateSelect = (templateName: string) => {
    const template = templates.find(t => t.name === templateName)
    if (template) {
      setFormData({
        name: template.name,
        display_name: template.display_name,
        type: template.type,
        enabled: true,
        url: template.url || '',
        bootstrap_servers: template.bootstrap_servers || '',
        docker: template.docker
      })
      setSelectedTemplate(templateName)
      setIsCustom(false)
    }
  }
  
  const handleCustomSelect = () => {
    setIsCustom(true)
    setSelectedTemplate('')
    setFormData({
      name: '',
      display_name: '',
      type: 'redis',
      enabled: true,
      url: '',
      bootstrap_servers: '',
      docker: {}
    })
  }

  const handleSubmit = () => {
    if (!formData.name || !formData.display_name) {
      alert('请填写名称和显示名称')
      return
    }
    
    if (isEdit) {
      updateMutation.mutate(formData)
    } else {
      createMutation.mutate(formData)
    }
  }

  const isPending = createMutation.isPending || updateMutation.isPending

  return (
    <div className="fixed inset-0 animate-fade bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className="bg-dark-800 rounded-lg border border-dark-700 p-6 max-w-xl w-full mx-4 max-h-[85vh] overflow-auto"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-section text-ink-primary flex items-center gap-2">
            <Database className="w-5 h-5 text-plaita-400" />
            {isEdit ? '编辑基础设施服务' : '添加基础设施服务'}
          </h3>
          <button onClick={onClose} className="text-dark-400 hover:text-ink-primary">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* 模板选择（仅新建时显示） */}
        {!isEdit && (
          <div className="mb-6">
            <label className="block text-sm font-medium mb-3 text-dark-300">
              选择服务类型
            </label>
            <div className="grid grid-cols-1 gap-2">
              {templates.map(template => (
                <button
                  key={template.name}
                  onClick={() => handleTemplateSelect(template.name)}
                  className={`p-3 rounded border text-left transition-colors ${
                    selectedTemplate === template.name
                      ? 'border-plaita-500 bg-plaita-500/10'
                      : 'border-dark-600 hover:border-dark-500 bg-dark-900'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <InfraTypeIcon type={template.type} />
                      <span className="font-medium text-sm">{template.display_name}</span>
                      <span className="text-xs text-dark-500 font-mono">({template.name})</span>
                    </div>
                    {selectedTemplate === template.name && (
                      <CheckCircle className="w-4 h-4 text-plaita-400" />
                    )}
                  </div>
                  <p className="text-xs text-dark-400 mt-1 ml-7">{template.description}</p>
                </button>
              ))}
              
              {/* 自定义选项 */}
              <button
                onClick={handleCustomSelect}
                className={`p-3 rounded border text-left transition-colors ${
                  isCustom
                    ? 'border-plaita-500 bg-plaita-500/10'
                    : 'border-dark-600 hover:border-dark-500 bg-dark-900'
                }`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Plus className="w-4 h-4 text-dark-400" />
                    <span className="font-medium text-sm">自定义服务</span>
                  </div>
                  {isCustom && <CheckCircle className="w-4 h-4 text-plaita-400" />}
                </div>
                <p className="text-xs text-dark-400 mt-1 ml-6">手动配置服务参数</p>
              </button>
            </div>
          </div>
        )}

        {/* 已选择服务后显示配置 */}
        {(selectedTemplate || isCustom || isEdit) && (
          <div className="space-y-4">
            {/* 选中模板后显示简化信息 */}
            {!isCustom && selectedTemplate && !isEdit && (
              <div className="p-3 rounded bg-dark-900 border border-dark-700">
                <div className="flex items-center gap-2 mb-1">
                  <InfraTypeIcon type={formData.type} />
                  <span className="font-medium">{formData.display_name}</span>
                </div>
                <p className="text-xs text-dark-400 ml-6">
                  类型: {formData.name}
                </p>
              </div>
            )}
            
            {/* 自定义服务需要填写 ID 和名称 */}
            {(isCustom || isEdit) && (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1 text-dark-300">
                    服务 ID <span className="text-status-error">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={e => setFormData({ ...formData, name: e.target.value })}
                    disabled={isEdit}
                    placeholder="如: redis, kafka"
                    className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                               focus:border-plaita-500 focus:outline-none disabled:opacity-50"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1 text-dark-300">
                    显示名称 <span className="text-status-error">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.display_name}
                    onChange={e => setFormData({ ...formData, display_name: e.target.value })}
                    placeholder="如: Redis 缓存服务"
                    className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                               focus:border-plaita-500 focus:outline-none"
                  />
                </div>
              </div>
            )}
            
            {/* 自定义服务选择类型 */}
            {isCustom && (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1 text-dark-300">
                    服务类型
                  </label>
                  <select
                    value={formData.type}
                    onChange={e => setFormData({ ...formData, type: e.target.value })}
                    className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                               focus:border-plaita-500 focus:outline-none"
                  >
                    <option value="redis">Redis</option>
                    <option value="kafka">Kafka</option>
                    <option value="database">数据库</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1 text-dark-300">
                    状态
                  </label>
                  <label className="flex items-center gap-2 px-3 py-2">
                    <input
                      type="checkbox"
                      checked={formData.enabled}
                      onChange={e => setFormData({ ...formData, enabled: e.target.checked })}
                      className="w-4 h-4 rounded"
                    />
                    <span className="text-sm">{formData.enabled ? '已启用' : '已禁用'}</span>
                  </label>
                </div>
              </div>
            )}

            {/* 连接信息 - 核心配置 */}
            {(formData.type === 'redis' || formData.type === 'database') && (
              <div>
                <label className="block text-sm font-medium mb-1 text-dark-300">
                  连接 URL
                </label>
                <input
                  type="text"
                  value={formData.url}
                  onChange={e => setFormData({ ...formData, url: e.target.value })}
                  placeholder={formData.type === 'redis' 
                    ? 'redis://localhost:6379/0'
                    : 'postgresql://localhost:5432/plaita'}
                  className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                             focus:border-plaita-500 focus:outline-none font-mono text-sm"
                />
              </div>
            )}

            {formData.type === 'kafka' && (
              <div>
                <label className="block text-sm font-medium mb-1 text-dark-300">
                  Bootstrap Servers
                </label>
                <input
                  type="text"
                  value={formData.bootstrap_servers}
                  onChange={e => setFormData({ ...formData, bootstrap_servers: e.target.value })}
                  placeholder="localhost:9092"
                  className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                             focus:border-plaita-500 focus:outline-none font-mono text-sm"
                />
              </div>
            )}

            {/* 高级设置 - Docker 配置 */}
            <div className="border-t border-dark-700 pt-4">
              <button
                type="button"
                onClick={() => setShowAdvanced(!showAdvanced)}
                className="flex items-center gap-2 text-sm text-dark-400 hover:text-ink-primary"
              >
                {showAdvanced ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                {showAdvanced ? '隐藏' : '显示'} Docker 配置
                <span className="text-xs text-dark-500">(可选)</span>
              </button>
              
              {showAdvanced && (
                <div className="mt-2">
                  <textarea
                    value={JSON.stringify(formData.docker, null, 2)}
                    onChange={e => {
                      try {
                        setFormData({ ...formData, docker: JSON.parse(e.target.value) })
                      } catch {
                        // 忽略解析错误
                      }
                    }}
                    rows={6}
                    className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                               focus:border-plaita-500 focus:outline-none font-mono text-sm"
                    placeholder='{"image": "redis:7-alpine", "ports": ["6379:6379"]}'
                  />
                </div>
              )}
            </div>
          </div>
        )}

        {/* 操作按钮 */}
        <div className="flex justify-end gap-3 mt-6 pt-4 border-t border-dark-700">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded bg-dark-700 hover:bg-dark-600 text-sm"
          >
            取消
          </button>
          <button
            onClick={handleSubmit}
            disabled={isPending || (!selectedTemplate && !isCustom && !isEdit) || !formData.name}
            className="flex items-center gap-2 px-4 py-2 rounded bg-plaita-500 hover:bg-plaita-600 
                       text-sm disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isPending ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                保存中…
              </>
            ) : (
              <>
                <Save className="w-4 h-4" />
                {isEdit ? '保存修改' : '添加服务'}
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  )
}

// 快速测试弹窗
function QuickTestDialog({
  onClose
}: {
  onClose: () => void
}) {
  const [selectedTest, setSelectedTest] = useState<string>('')
  const [testParams, setTestParams] = useState<string>('{"value": 21}')
  const [testResult, setTestResult] = useState<QuickTestResponse | null>(null)

  // 获取测试模板
  const { data: templatesData } = useQuery({
    queryKey: ['testTemplates'],
    queryFn: api.getTestTemplates,
  })

  // 运行测试
  const testMutation = useMutation({
    mutationFn: (data: { type: string; params?: Record<string, unknown> }) => 
      api.runQuickTest(data.type, data.params),
    onSuccess: (result) => {
      setTestResult(result)
    },
  })

  const handleRunTest = () => {
    if (!selectedTest) return
    
    let params: Record<string, unknown> | undefined
    try {
      params = testParams ? JSON.parse(testParams) : undefined
    } catch {
      params = undefined
    }
    
    testMutation.mutate({ type: selectedTest, params })
  }

  const getTestIcon = (testId: string) => {
    switch (testId) {
      case 'simple': return <Zap className="w-5 h-5 text-plaita-400" />
      case 'delay': return <Clock className="w-5 h-5 text-blue-400" />
      case 'approval': return <UserCheck className="w-5 h-5 text-purple-400" />
      default: return <Play className="w-5 h-5 text-gray-400" />
    }
  }

  return (
    <div className="fixed inset-0 animate-fade bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className="bg-dark-800 rounded-lg border border-dark-700 p-6 max-w-5xl w-full mx-4 max-h-[90vh] overflow-hidden flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        {/* 标题栏 */}
        <div className="flex items-center justify-between mb-6 flex-shrink-0">
          <h3 className="text-xl font-semibold flex items-center gap-2">
            <Zap className="w-6 h-6 text-plaita-400" />
            快速测试
          </h3>
          <button onClick={onClose} className="text-dark-400 hover:text-ink-primary">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* 左右分栏布局 */}
        <div className="flex gap-6 flex-1 overflow-hidden min-h-0">
          {/* 左侧：测试类型选择 */}
          <div className="w-80 flex-shrink-0 overflow-y-auto">
            <label className="block text-sm font-medium mb-3 text-dark-300">
              选择测试类型
            </label>
            <div className="space-y-2">
              {templatesData?.templates.map(template => (
                <button
                  key={template.id}
                  onClick={() => {
                    setSelectedTest(template.id)
                    setTestParams(JSON.stringify(template.default_params, null, 2))
                    setTestResult(null)
                  }}
                  className={`w-full p-3 rounded border text-left transition-colors ${
                    selectedTest === template.id
                      ? 'border-plaita-500 bg-plaita-500/10'
                      : 'border-dark-600 hover:border-dark-500 bg-dark-900'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      {getTestIcon(template.id)}
                      <span className="font-medium">{template.name}</span>
                    </div>
                    {selectedTest === template.id && (
                      <CheckCircle className="w-4 h-4 text-plaita-400" />
                    )}
                  </div>
                  <p className="text-xs text-dark-400 mt-1 ml-7">{template.description}</p>
                  {template.required_services.length > 0 && (
                    <p className="text-xs text-status-warning/80 mt-1 ml-7">
                      需要服务: {template.required_services.join(', ')}
                    </p>
                  )}
                </button>
              ))}
            </div>
          </div>

          {/* 右侧：测试参数和结果 */}
          <div className="flex-1 flex flex-col overflow-hidden min-w-0">
            {/* 测试参数 */}
            <div className="mb-4 flex-shrink-0">
              <label className="block text-sm font-medium mb-2 text-dark-300">
                测试参数 (JSON)
              </label>
              <textarea
                value={testParams}
                onChange={e => setTestParams(e.target.value)}
                rows={3}
                disabled={!selectedTest}
                className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                           focus:border-plaita-500 focus:outline-none font-mono text-sm
                           disabled:opacity-50 disabled:cursor-not-allowed"
                placeholder='{"key": "value"}'
              />
            </div>

            {/* 运行按钮 */}
            <div className="flex justify-start mb-4 flex-shrink-0">
              <button
                onClick={handleRunTest}
                disabled={!selectedTest || testMutation.isPending}
                className="flex items-center gap-2 px-6 py-2.5 rounded bg-plaita-500 hover:bg-plaita-600 
                           disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {testMutation.isPending ? (
                  <>
                    <RefreshCw className="w-5 h-5 animate-spin" />
                    运行中…
                  </>
                ) : (
                  <>
                    <Play className="w-5 h-5" />
                    运行测试
                  </>
                )}
              </button>
            </div>

            {/* 测试结果 */}
            <div className="flex-1 overflow-y-auto min-h-0">
              {!selectedTest && (
                <div className="flex items-center justify-center h-full text-dark-500">
                  <div className="text-center">
                    <Play className="w-12 h-12 mx-auto mb-3 opacity-30" />
                    <p>请先选择一个测试类型</p>
                  </div>
                </div>
              )}
              
              {selectedTest && !testResult && !testMutation.isPending && (
                <div className="flex items-center justify-center h-full text-dark-500">
                  <div className="text-center">
                    <Zap className="w-12 h-12 mx-auto mb-3 opacity-30" />
                    <p>点击"运行测试"开始执行</p>
                  </div>
                </div>
              )}

              {testResult && (
                <div className={`p-4 rounded-lg border ${
                  testResult.success 
                    ? 'bg-plaita-500/10 border-plaita-500/30' 
                    : 'bg-status-error-dim border-status-error/30'
                }`}>
                  <div className="flex items-start gap-2 mb-3">
                    {testResult.success ? (
                      <CheckCircle className="w-5 h-5 text-plaita-400 flex-shrink-0 mt-0.5" />
                    ) : (
                      <XCircle className="w-5 h-5 text-status-error flex-shrink-0 mt-0.5" />
                    )}
                    <span className={`font-medium whitespace-pre-wrap ${testResult.success ? 'text-plaita-400' : 'text-status-error'}`}>
                      {testResult.message}
                    </span>
                  </div>
                  
                  {testResult.execution_id && (
                    <p className="text-sm text-dark-400 mb-3">
                      执行 ID: <code className="bg-dark-900 px-2 py-0.5 rounded">{testResult.execution_id}</code>
                    </p>
                  )}
                  
                  {testResult.result && (
                    <div className="mb-3">
                      <p className="text-sm font-medium text-dark-300 mb-2">执行结果:</p>
                      <pre className="bg-dark-900 p-3 rounded text-xs font-mono overflow-auto max-h-48">
                        {JSON.stringify(testResult.result, null, 2)}
                      </pre>
                    </div>
                  )}
                  
                  {testResult.flow_definition && (
                    <details className="mb-3">
                      <summary className="text-sm font-medium text-dark-300 mb-2 cursor-pointer hover:text-dark-200">
                        流程定义 (点击展开)
                      </summary>
                      <pre className="bg-dark-900 p-3 rounded text-xs font-mono overflow-auto max-h-48 mt-2">
                        {JSON.stringify(testResult.flow_definition, null, 2)}
                      </pre>
                    </details>
                  )}
                  
                  {testResult.error && (
                    <div className="p-3 rounded bg-status-error-dim border border-red-500/20">
                      <p className="text-sm font-medium text-status-error mb-1">错误详情:</p>
                      <pre className="text-xs text-red-300 whitespace-pre-wrap">{testResult.error}</pre>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* 底部按钮 */}
        <div className="flex justify-end mt-6 pt-4 border-t border-dark-700 flex-shrink-0">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded bg-dark-700 hover:bg-dark-600 text-sm"
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  )
}

// 内存模式提示组件
function MemoryModeNotice({ clusterId }: { clusterId: string }) {
  const { data: configData } = useQuery({
    queryKey: ['clusterConfig', clusterId],
    queryFn: () => api.getClusterConfigDetail(clusterId),
  })

  const config = configData?.config as {
    storage?: { type?: string }
    eventbus?: { type?: string }
    queue?: { type?: string }
  } | undefined

  const isMemoryMode = 
    config?.storage?.type === 'memory' || 
    config?.eventbus?.type === 'memory' || 
    config?.queue?.type === 'memory'

  if (!isMemoryMode) return null

  return (
    <div className="mb-4 p-4 rounded-lg bg-yellow-500/10 border border-status-warning/30">
      <div className="flex items-start gap-3">
        <div className="p-2 rounded bg-status-warning-dim">
          <AlertCircle className="w-5 h-5 text-status-warning" />
        </div>
        <div>
          <h3 className="font-medium text-status-warning mb-1">配置中存在 memory 后端声明</h3>
          <p className="text-sm text-status-warning/80">
            集群配置里 storage / eventbus / queue 声明为 memory。注意：这些键只影响
            内嵌运行路径（如 plaita_flows.run 本地脚本）；console 拉起的执行器与
            服务始终使用下方「基础设施」里的 Redis 作为队列与状态存储。
          </p>
          <p className="text-xs text-status-warning/60 mt-2">
            如需持久化，请在"集群配置"中将存储后端切换为 Redis 或数据库，并确保
            Redis 自身开启持久化（AOF）。
          </p>
        </div>
      </div>
    </div>
  )
}

// 基本设置表单组件
function BasicSettingsForm({
  config,
  onSave,
  isSaving
}: {
  config?: {
    mode?: string
    redis?: Record<string, string>
    storage?: { type?: string }
    eventbus?: { type?: string }
    queue?: { type?: string }
  }
  onSave: (config: Record<string, unknown>) => void
  isSaving: boolean
}) {
  const [isEditing, setIsEditing] = useState(false)
  const [formData, setFormData] = useState({
    mode: config?.mode || 'process',
    redis_url: config?.redis?.url || 'redis://localhost:6379/0',
    storage_type: config?.storage?.type || 'redis',
    eventbus_type: config?.eventbus?.type || 'redis',
    queue_type: config?.queue?.type || 'redis'
  })

  useEffect(() => {
    setFormData({
      mode: config?.mode || 'process',
      redis_url: config?.redis?.url || 'redis://localhost:6379/0',
      storage_type: config?.storage?.type || 'redis',
      eventbus_type: config?.eventbus?.type || 'redis',
      queue_type: config?.queue?.type || 'redis'
    })
  }, [config])

  const handleSave = () => {
    const newConfig = {
      mode: formData.mode,
      redis: { url: formData.redis_url },
      storage: { type: formData.storage_type },
      eventbus: { type: formData.eventbus_type },
      queue: { type: formData.queue_type }
    }
    onSave(newConfig)
    setIsEditing(false)
  }

  // 快速切换到纯内存模式
  const handleMemoryMode = () => {
    setFormData({
      ...formData,
      storage_type: 'memory',
      eventbus_type: 'memory',
      queue_type: 'memory'
    })
    setIsEditing(true)
  }

  return (
    <div className="bg-dark-800 rounded-lg border border-dark-700 p-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium flex items-center gap-2">
          <Settings className="w-4 h-4 text-plaita-400" />
          基本设置
        </h3>
        <div className="flex items-center gap-2">
          {!isEditing && formData.storage_type !== 'memory' && (
            <button
              onClick={handleMemoryMode}
              className="text-xs px-2 py-1 rounded bg-dark-700 hover:bg-dark-600 text-dark-300"
            >
              切换到纯内存模式
            </button>
          )}
          {isEditing ? (
            <>
              <button
                onClick={() => setIsEditing(false)}
                className="text-xs px-2 py-1 rounded bg-dark-700 hover:bg-dark-600"
              >
                取消
              </button>
              <button
                onClick={handleSave}
                disabled={isSaving}
                className="text-xs px-3 py-1 rounded bg-plaita-500 hover:bg-plaita-600 flex items-center gap-1"
              >
                {isSaving ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Save className="w-3 h-3" />}
                保存
              </button>
            </>
          ) : (
            <button
              onClick={() => setIsEditing(true)}
              className="text-xs px-2 py-1 rounded bg-dark-700 hover:bg-dark-600 flex items-center gap-1"
            >
              <Edit className="w-3 h-3" />
              编辑
            </button>
          )}
        </div>
      </div>
      
      {isEditing ? (
        <div className="space-y-3">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-dark-400 mb-1">运行模式</label>
              <select
                value={formData.mode}
                onChange={e => setFormData({ ...formData, mode: e.target.value })}
                className="w-full px-2 py-1.5 rounded bg-dark-900 border border-dark-600 text-sm"
              >
                <option value="process">本地进程 (process)</option>
                <option value="docker">Docker 容器 (docker)</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-dark-400 mb-1">Redis URL</label>
              <input
                type="text"
                value={formData.redis_url}
                onChange={e => setFormData({ ...formData, redis_url: e.target.value })}
                className="w-full px-2 py-1.5 rounded bg-dark-900 border border-dark-600 text-sm font-mono"
                placeholder="redis://localhost:6379/0"
              />
            </div>
          </div>
          
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-xs text-dark-400 mb-1">状态存储</label>
              <select
                value={formData.storage_type}
                onChange={e => setFormData({ ...formData, storage_type: e.target.value })}
                className="w-full px-2 py-1.5 rounded bg-dark-900 border border-dark-600 text-sm"
              >
                <option value="memory">内存 (memory)</option>
                <option value="redis">Redis</option>
                <option value="sqlalchemy">数据库 (sqlalchemy)</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-dark-400 mb-1">事件总线</label>
              <select
                value={formData.eventbus_type}
                onChange={e => setFormData({ ...formData, eventbus_type: e.target.value })}
                className="w-full px-2 py-1.5 rounded bg-dark-900 border border-dark-600 text-sm"
              >
                <option value="memory">内存 (memory)</option>
                <option value="redis">Redis</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-dark-400 mb-1">任务队列</label>
              <select
                value={formData.queue_type}
                onChange={e => setFormData({ ...formData, queue_type: e.target.value })}
                className="w-full px-2 py-1.5 rounded bg-dark-900 border border-dark-600 text-sm"
              >
                <option value="memory">内存 (memory)</option>
                <option value="redis">Redis</option>
              </select>
            </div>
          </div>
          
          {formData.storage_type === 'memory' && (
            <div className="p-2 rounded bg-yellow-500/10 border border-yellow-500/20 text-xs text-status-warning">
              ⚠️ 纯内存模式下，服务重启后所有状态会丢失，仅适用于开发测试
            </div>
          )}
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-xs text-dark-400 mb-1">运行模式</label>
            <span className="text-sm font-mono">{formData.mode}</span>
          </div>
          <div>
            <label className="block text-xs text-dark-400 mb-1">Redis URL</label>
            <span className="text-sm font-mono truncate block" title={formData.redis_url}>
              {formData.redis_url || '未配置'}
            </span>
          </div>
          <div>
            <label className="block text-xs text-dark-400 mb-1">存储后端</label>
            <span className={`text-sm font-mono ${formData.storage_type === 'memory' ? 'text-status-warning' : ''}`}>
              {formData.storage_type}
            </span>
          </div>
          <div>
            <label className="block text-xs text-dark-400 mb-1">事件总线</label>
            <span className={`text-sm font-mono ${formData.eventbus_type === 'memory' ? 'text-status-warning' : ''}`}>
              {formData.eventbus_type}
            </span>
          </div>
        </div>
      )}
    </div>
  )
}

// 内置服务类型模板
const BUILTIN_SERVICE_TEMPLATES = [
  {
    id: 'flow_worker',
    display_name: '流程执行器',
    description: '执行和恢复流程的核心工作器',
    module: 'plaita.server.flow_worker',
    docker_image: 'plaita/flow-worker:latest',
    default_instances: 1,
    max_instances: 10,
    default_env: {
      'PLAITA_REDIS_URL': '${redis.url}',
      'PLAITA_QUEUE_NAME': 'plaita:flow:queue'
    }
  },
  {
    id: 'delay_service',
    display_name: '延迟定时服务',
    description: '处理延迟和定时任务',
    command: 'python -m plaita.server.services delay_service',
    docker_image: 'plaita/delay-service:latest',
    default_instances: 1,
    max_instances: 3,
    default_env: {
      'PLAITA_REDIS_URL': '${redis.url}',
      'PLAITA_CHECK_INTERVAL': '5'
    }
  },
  {
    id: 'redis_queue_service',
    display_name: 'Redis 队列服务',
    description: '处理 Redis 队列消息',
    command: 'python -m plaita.server.services redis_queue_service',
    docker_image: 'plaita/redis-queue-service:latest',
    default_instances: 1,
    max_instances: 5,
    default_env: {
      'PLAITA_REDIS_URL': '${redis.url}',
      'PLAITA_QUEUE_PREFIX': 'plaita:queue:'
    }
  },
  {
    id: 'http_callback_service',
    display_name: 'HTTP 回调服务',
    description: '处理 HTTP 回调任务',
    command: 'python -m plaita.server.services http_callback_service',
    docker_image: 'plaita/http-callback-service:latest',
    default_instances: 1,
    max_instances: 3,
    default_env: {
      'PLAITA_REDIS_URL': '${redis.url}'
    }
  },
  {
    id: 'approval_service',
    display_name: '人工审批服务',
    description: '处理人工审批流程',
    command: 'python -m plaita.server.services approval_service',
    docker_image: 'plaita/approval-service:latest',
    default_instances: 1,
    max_instances: 2,
    default_env: {
      'PLAITA_REDIS_URL': '${redis.url}'
    }
  }
]

// 服务配置表单弹窗
function ServiceConfigFormDialog({
  service,
  existingServices,
  onClose,
  onSave
}: {
  service?: {
    service_type: string
    display_name: string
    process: Record<string, unknown>
    docker: Record<string, unknown>
    env: Record<string, string>
    default_instances: number
    max_instances: number
  }
  existingServices?: string[]
  onClose: () => void
  onSave: (data: Record<string, unknown>) => void
}) {
  const isEdit = !!service
  const [isCustom, setIsCustom] = useState(
    isEdit && !BUILTIN_SERVICE_TEMPLATES.find(t => t.id === service?.service_type)
  )
  const [showAdvanced, setShowAdvanced] = useState(false)
  
  // 找到已存在的内置服务
  const usedBuiltinTypes = existingServices?.filter(
    s => BUILTIN_SERVICE_TEMPLATES.find(t => t.id === s)
  ) || []
  
  // 可用的内置服务模板
  const availableTemplates = BUILTIN_SERVICE_TEMPLATES.filter(
    t => !usedBuiltinTypes.includes(t.id) || t.id === service?.service_type
  )
  
  const [formData, setFormData] = useState({
    service_type: service?.service_type || '',
    display_name: service?.display_name || '',
    module: (service?.process as Record<string, string>)?.module || '',
    command: (service?.process as Record<string, string>)?.command || '',
    docker_image: (service?.docker as Record<string, string>)?.image || '',
    default_instances: service?.default_instances || 1,
    max_instances: service?.max_instances || 10,
    env: Object.entries(service?.env || {}).map(([k, v]) => ({ key: k, value: v }))
  })

  // 选择内置服务模板
  const handleSelectTemplate = (templateId: string) => {
    const template = BUILTIN_SERVICE_TEMPLATES.find(t => t.id === templateId)
    if (template) {
      setFormData({
        service_type: template.id,
        display_name: template.display_name,
        module: template.module || '',
        command: template.command || '',
        docker_image: template.docker_image,
        default_instances: template.default_instances,
        max_instances: template.max_instances,
        env: Object.entries(template.default_env).map(([k, v]) => ({ key: k, value: v }))
      })
      setIsCustom(false)
    }
  }

  const handleAddEnv = () => {
    setFormData({ ...formData, env: [...formData.env, { key: '', value: '' }] })
  }

  const handleRemoveEnv = (index: number) => {
    setFormData({ ...formData, env: formData.env.filter((_, i) => i !== index) })
  }

  const handleSubmit = () => {
    if (!formData.service_type || !formData.display_name) {
      alert('请选择服务类型')
      return
    }
    
    const envObj: Record<string, string> = {}
    formData.env.forEach(e => {
      if (e.key) envObj[e.key] = e.value
    })

    const data: Record<string, unknown> = {
      display_name: formData.display_name,
      process: {
        ...(formData.module && { module: formData.module }),
        ...(formData.command && { command: formData.command })
      },
      docker: {
        image: formData.docker_image || `plaita/${formData.service_type}:latest`
      },
      env: envObj,
      default_instances: formData.default_instances,
      max_instances: formData.max_instances
    }

    onSave({ [formData.service_type]: data })
    onClose()
  }

  return (
    <div className="fixed inset-0 animate-fade bg-black/50 flex items-center justify-center z-50" onClick={onClose}>
      <div 
        className="bg-dark-800 rounded-lg border border-dark-700 p-6 max-w-xl w-full mx-4 max-h-[85vh] overflow-auto"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-section text-ink-primary flex items-center gap-2">
            <Server className="w-5 h-5 text-plaita-400" />
            {isEdit ? '编辑服务配置' : '添加服务'}
          </h3>
          <button onClick={onClose} className="text-dark-400 hover:text-ink-primary">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* 服务类型选择（仅新建时） */}
        {!isEdit && (
          <div className="mb-6">
            <label className="block text-sm font-medium mb-3 text-dark-300">
              选择服务类型
            </label>
            <div className="grid grid-cols-1 gap-2">
              {availableTemplates.map(template => (
                <button
                  key={template.id}
                  onClick={() => handleSelectTemplate(template.id)}
                  className={`p-3 rounded border text-left transition-colors ${
                    formData.service_type === template.id
                      ? 'border-plaita-500 bg-plaita-500/10'
                      : 'border-dark-600 hover:border-dark-500 bg-dark-900'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Server className="w-4 h-4 text-plaita-400" />
                      <span className="font-medium text-sm">{template.display_name}</span>
                      <span className="text-xs text-dark-500 font-mono">({template.id})</span>
                    </div>
                    {formData.service_type === template.id && (
                      <CheckCircle className="w-4 h-4 text-plaita-400" />
                    )}
                  </div>
                  <p className="text-xs text-dark-400 mt-1 ml-6">{template.description}</p>
                </button>
              ))}
              
              {/* 自定义服务选项 */}
              <button
                onClick={() => {
                  setIsCustom(true)
                  setFormData({
                    ...formData,
                    service_type: '',
                    display_name: '',
                    module: '',
                    command: '',
                    docker_image: '',
                    env: []
                  })
                }}
                className={`p-3 rounded border text-left transition-colors ${
                  isCustom
                    ? 'border-plaita-500 bg-plaita-500/10'
                    : 'border-dark-600 hover:border-dark-500 bg-dark-900'
                }`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Plus className="w-4 h-4 text-dark-400" />
                    <span className="font-medium text-sm">自定义服务</span>
                  </div>
                  {isCustom && (
                    <CheckCircle className="w-4 h-4 text-plaita-400" />
                  )}
                </div>
                <p className="text-xs text-dark-400 mt-1 ml-6">手动配置服务参数</p>
              </button>
            </div>

            {availableTemplates.length === 0 && !isCustom && (
              <p className="text-sm text-dark-400 mt-2">
                所有内置服务类型已添加，请选择"自定义服务"
              </p>
            )}
          </div>
        )}

        {/* 已选择服务类型后显示配置 */}
        {(formData.service_type || isCustom) && (
          <div className="space-y-4">
            {/* 自定义服务需要手动填写类型和名称 */}
            {isCustom && (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1 text-dark-300">
                    服务类型 ID <span className="text-status-error">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.service_type}
                    onChange={e => setFormData({ ...formData, service_type: e.target.value })}
                    disabled={isEdit}
                    placeholder="如: my_service"
                    className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                               focus:border-plaita-500 focus:outline-none disabled:opacity-50"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1 text-dark-300">
                    显示名称 <span className="text-status-error">*</span>
                  </label>
                  <input
                    type="text"
                    value={formData.display_name}
                    onChange={e => setFormData({ ...formData, display_name: e.target.value })}
                    placeholder="如: 我的服务"
                    className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                               focus:border-plaita-500 focus:outline-none"
                  />
                </div>
              </div>
            )}

            {/* 内置服务显示选中的信息 */}
            {!isCustom && formData.service_type && (
              <div className="p-3 rounded bg-dark-900 border border-dark-700">
                <div className="flex items-center gap-2 mb-1">
                  <Server className="w-4 h-4 text-plaita-400" />
                  <span className="font-medium">{formData.display_name}</span>
                </div>
                <p className="text-xs text-dark-400 ml-6">
                  类型: {formData.service_type}
                </p>
              </div>
            )}

            {/* 实例数配置 */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium mb-1 text-dark-300">
                  默认实例数
                </label>
                <input
                  type="number"
                  min={0}
                  value={formData.default_instances}
                  onChange={e => setFormData({ ...formData, default_instances: parseInt(e.target.value) || 0 })}
                  className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                             focus:border-plaita-500 focus:outline-none"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1 text-dark-300">
                  最大实例数
                </label>
                <input
                  type="number"
                  min={1}
                  value={formData.max_instances}
                  onChange={e => setFormData({ ...formData, max_instances: parseInt(e.target.value) || 1 })}
                  className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                             focus:border-plaita-500 focus:outline-none"
                />
              </div>
            </div>

            {/* 高级配置（可折叠） */}
            <div className="border-t border-dark-700 pt-4">
              <button
                type="button"
                onClick={() => setShowAdvanced(!showAdvanced)}
                className="flex items-center gap-2 text-sm text-dark-400 hover:text-ink-primary"
              >
                {showAdvanced ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                {showAdvanced ? '隐藏高级配置' : '显示高级配置'}
                <span className="text-xs text-dark-500">
                  {isCustom ? '(自定义服务必须配置)' : '(可选，使用默认值)'}
                </span>
              </button>
              
              {(showAdvanced || isCustom) && (
                <div className="mt-4 space-y-4">
                  <div>
                    <label className="block text-sm font-medium mb-1 text-dark-300">
                      Python 模块路径
                    </label>
                    <input
                      type="text"
                      value={formData.module}
                      onChange={e => setFormData({ ...formData, module: e.target.value })}
                      placeholder="如: plaita.server.flow_worker"
                      className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                                 focus:border-plaita-500 focus:outline-none font-mono text-sm"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-1 text-dark-300">
                      启动命令（与模块二选一）
                    </label>
                    <input
                      type="text"
                      value={formData.command}
                      onChange={e => setFormData({ ...formData, command: e.target.value })}
                      placeholder="如: python -m plaita.server.services delay_service"
                      className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                                 focus:border-plaita-500 focus:outline-none font-mono text-sm"
                    />
                  </div>

                  <div>
                    <label className="block text-sm font-medium mb-1 text-dark-300">
                      Docker 镜像
                    </label>
                    <input
                      type="text"
                      value={formData.docker_image}
                      onChange={e => setFormData({ ...formData, docker_image: e.target.value })}
                      placeholder="如: plaita/flow-worker:latest"
                      className="w-full px-3 py-2 rounded bg-dark-900 border border-dark-600 
                                 focus:border-plaita-500 focus:outline-none font-mono text-sm"
                    />
                  </div>

                  {/* 环境变量 */}
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <label className="text-sm font-medium text-dark-300">环境变量</label>
                      <button
                        type="button"
                        onClick={handleAddEnv}
                        className="text-xs text-plaita-400 hover:text-plaita-300"
                      >
                        + 添加变量
                      </button>
                    </div>
                    <div className="space-y-2">
                      {formData.env.map((env, idx) => (
                        <div key={idx} className="flex gap-2">
                          <input
                            type="text"
                            value={env.key}
                            onChange={e => {
                              const newEnv = [...formData.env]
                              newEnv[idx] = { ...env, key: e.target.value }
                              setFormData({ ...formData, env: newEnv })
                            }}
                            placeholder="变量名"
                            className="flex-1 px-3 py-1.5 rounded bg-dark-900 border border-dark-600 
                                       focus:border-plaita-500 focus:outline-none text-sm font-mono"
                          />
                          <input
                            type="text"
                            value={env.value}
                            onChange={e => {
                              const newEnv = [...formData.env]
                              newEnv[idx] = { ...env, value: e.target.value }
                              setFormData({ ...formData, env: newEnv })
                            }}
                            placeholder="值"
                            className="flex-1 px-3 py-1.5 rounded bg-dark-900 border border-dark-600 
                                       focus:border-plaita-500 focus:outline-none text-sm font-mono"
                          />
                          <button
                            type="button"
                            onClick={() => handleRemoveEnv(idx)}
                            className="px-2 text-dark-400 hover:text-status-error"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      ))}
                      {formData.env.length === 0 && (
                        <p className="text-sm text-dark-500">暂无环境变量</p>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        <div className="flex justify-end gap-3 mt-6 pt-4 border-t border-dark-700">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded bg-dark-700 hover:bg-dark-600 text-sm"
          >
            取消
          </button>
          <button
            onClick={handleSubmit}
            disabled={!formData.service_type}
            className="flex items-center gap-2 px-4 py-2 rounded bg-plaita-500 hover:bg-plaita-600 
                       text-sm disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Save className="w-4 h-4" />
            {isEdit ? '保存修改' : '添加服务'}
          </button>
        </div>
      </div>
    </div>
  )
}

// 配置编辑器组件
function ConfigEditor({ clusterId }: { clusterId: string }) {
  const queryClient = useQueryClient()
  const [configText, setConfigText] = useState('')
  const [hasChanges, setHasChanges] = useState(false)
  const [parseError, setParseError] = useState<string | null>(null)
  const [saveSuccess, setSaveSuccess] = useState(false)
  const [viewMode, setViewMode] = useState<'form' | 'json'>('form')
  const [editingService, setEditingService] = useState<{
    service_type: string
    display_name: string
    process: Record<string, unknown>
    docker: Record<string, unknown>
    env: Record<string, string>
    default_instances: number
    max_instances: number
  } | null>(null)
  const [showAddService, setShowAddService] = useState(false)

  // 获取集群配置
  const { data: configData, isLoading } = useQuery({
    queryKey: ['clusterConfig', clusterId],
    queryFn: () => api.getClusterConfigDetail(clusterId),
  })

  // 解析配置
  const parsedConfig = configData?.config as {
    mode?: string
    redis?: Record<string, string>
    services?: Record<string, {
      display_name: string
      process?: Record<string, unknown>
      docker?: Record<string, unknown>
      env?: Record<string, string>
      default_instances?: number
      max_instances?: number
    }>
    infrastructure?: Record<string, unknown>
  } | undefined

  // 初始化配置文本
  useEffect(() => {
    if (configData) {
      const yaml = JSON.stringify(configData.config, null, 2)
      setConfigText(yaml)
      setHasChanges(false)
    }
  }, [configData])

  // 保存配置
  const saveMutation = useMutation({
    mutationFn: (config: object) => api.saveClusterConfigDetail(clusterId, config),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['clusterConfig', clusterId] })
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['infrastructure'] })
      setHasChanges(false)
      setSaveSuccess(true)
      setTimeout(() => setSaveSuccess(false), 3000)
    },
  })

  const handleSave = () => {
    try {
      const config = JSON.parse(configText)
      setParseError(null)
      saveMutation.mutate(config)
    } catch (e) {
      setParseError('JSON 格式错误：' + (e as Error).message)
    }
  }

  const handleServiceSave = (serviceData: Record<string, unknown>) => {
    if (!parsedConfig) return
    
    const newConfig = {
      ...parsedConfig,
      services: {
        ...parsedConfig.services,
        ...serviceData
      }
    }
    
    saveMutation.mutate(newConfig)
  }

  const handleDeleteService = (serviceType: string) => {
    if (!parsedConfig) return
    if (!confirm(`确定要删除服务 ${serviceType} 吗？`)) return
    
    const { [serviceType]: _, ...remainingServices } = parsedConfig.services || {}
    
    const newConfig = {
      ...parsedConfig,
      services: remainingServices
    }
    
    saveMutation.mutate(newConfig)
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-8 h-8 text-plaita-400 animate-spin" />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <FileCode className="w-5 h-5 text-plaita-400" />
          <span className="font-medium">集群配置</span>
          <span className="text-sm text-dark-500">({clusterId})</span>
          {hasChanges && (
            <span className="px-2 py-0.5 rounded bg-status-warning-dim text-status-warning text-xs">
              未保存
            </span>
          )}
          {saveSuccess && (
            <span className="px-2 py-0.5 rounded bg-plaita-500/20 text-plaita-400 text-xs">
              ✓ 已保存
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {/* 视图切换 */}
          <div className="flex rounded bg-dark-700 p-0.5">
            <button
              onClick={() => setViewMode('form')}
              className={`px-3 py-1 rounded text-sm ${
                viewMode === 'form' 
                  ? 'bg-plaita-500 text-white' 
                  : 'text-dark-400 hover:text-ink-primary'
              }`}
            >
              表单
            </button>
            <button
              onClick={() => setViewMode('json')}
              className={`px-3 py-1 rounded text-sm ${
                viewMode === 'json' 
                  ? 'bg-plaita-500 text-white' 
                  : 'text-dark-400 hover:text-ink-primary'
              }`}
            >
              JSON
            </button>
          </div>
          {viewMode === 'json' && (
            <button
              onClick={handleSave}
              disabled={!hasChanges || saveMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 rounded bg-plaita-500 hover:bg-plaita-600
                         disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {saveMutation.isPending ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              保存
            </button>
          )}
        </div>
      </div>

      {viewMode === 'form' ? (
        <div className="space-y-6">
          {/* 基本设置 */}
          <BasicSettingsForm 
            config={parsedConfig}
            onSave={(newConfig) => {
              const updatedConfig = { ...parsedConfig, ...newConfig }
              saveMutation.mutate(updatedConfig)
            }}
            isSaving={saveMutation.isPending}
          />

          {/* 程序服务列表 */}
          <div className="bg-dark-800 rounded-lg border border-dark-700 p-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium flex items-center gap-2">
                <Server className="w-4 h-4 text-plaita-400" />
                程序服务
                <span className="text-dark-400 font-normal">
                  ({Object.keys(parsedConfig?.services || {}).length} 个)
                </span>
              </h3>
              <button
                onClick={() => setShowAddService(true)}
                className="flex items-center gap-1 px-3 py-1 rounded bg-plaita-500 hover:bg-plaita-600 text-sm"
              >
                <Plus className="w-4 h-4" />
                添加服务
              </button>
            </div>
            
            <div className="space-y-2">
              {Object.entries(parsedConfig?.services || {}).map(([type, config]) => (
                <div 
                  key={type}
                  className="flex items-center justify-between p-3 rounded bg-dark-900 border border-dark-700"
                >
                  <div className="flex items-center gap-3">
                    <Server className="w-5 h-5 text-dark-400" />
                    <div>
                      <div className="font-medium">{config.display_name}</div>
                      <div className="text-xs text-dark-400 font-mono">{type}</div>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <div className="text-xs text-dark-400">
                      {config.default_instances || 1} / {config.max_instances || 10} 实例
                    </div>
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => setEditingService({
                          service_type: type,
                          display_name: config.display_name,
                          process: config.process as Record<string, unknown> || {},
                          docker: config.docker as Record<string, unknown> || {},
                          env: config.env || {},
                          default_instances: config.default_instances || 1,
                          max_instances: config.max_instances || 10
                        })}
                        className="p-1.5 rounded hover:bg-dark-700 text-dark-400 hover:text-ink-primary"
                        title="编辑"
                      >
                        <Edit className="w-4 h-4" />
                      </button>
                      <button
                        onClick={() => handleDeleteService(type)}
                        className="p-1.5 rounded hover:bg-status-error-dim text-dark-400 hover:text-status-error"
                        title="删除"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                </div>
              ))}
              {Object.keys(parsedConfig?.services || {}).length === 0 && (
                <div className="text-center py-8 text-dark-400">
                  <Server className="w-12 h-12 mx-auto mb-2 opacity-50" />
                  <p>暂无服务配置</p>
                  <button
                    onClick={() => setShowAddService(true)}
                    className="mt-2 text-plaita-400 hover:text-plaita-300 text-sm"
                  >
                    添加第一个服务
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      ) : (
        <>
          {parseError && (
            <div className="p-3 rounded bg-status-error-dim text-status-error text-sm">
              {parseError}
            </div>
          )}

          <div className="bg-dark-800 rounded-lg border border-dark-700 overflow-hidden">
            <textarea
              value={configText}
              onChange={(e) => {
                setConfigText(e.target.value)
                setHasChanges(true)
                setParseError(null)
              }}
              className="w-full h-[500px] p-4 bg-dark-900 text-sm font-mono
                         focus:outline-none resize-none"
              spellCheck={false}
            />
          </div>

          <p className="text-sm text-dark-500">
            提示：配置使用 JSON 格式。修改后点击"保存"生效。
          </p>
        </>
      )}

      {/* 添加服务弹窗 */}
      {showAddService && (
        <ServiceConfigFormDialog
          existingServices={Object.keys(parsedConfig?.services || {})}
          onClose={() => setShowAddService(false)}
          onSave={handleServiceSave}
        />
      )}

      {/* 编辑服务弹窗 */}
      {editingService && (
        <ServiceConfigFormDialog
          service={editingService}
          existingServices={Object.keys(parsedConfig?.services || {})}
          onClose={() => setEditingService(null)}
          onSave={handleServiceSave}
        />
      )}
    </div>
  )
}

export default function Cluster() {
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const [startingType, setStartingType] = useState<string | null>(null)
  const [stoppingId, setStoppingId] = useState<string | null>(null)
  const [removingId, setRemovingId] = useState<string | null>(null)
  const [errorToView, setErrorToView] = useState<string | null>(null)
  const [logInstance, setLogInstance] = useState<{id: string, type: string} | null>(null)
  const [editingInfra, setEditingInfra] = useState<InfrastructureInfo | null>(null)
  const [showAddInfra, setShowAddInfra] = useState(false)
  const [showQuickTest, setShowQuickTest] = useState(false)
  const [showStopAll, setShowStopAll] = useState(false)
  const [bootstrapMsg, setBootstrapMsg] = useState<string | null>(null)
  
  // 当前标签页
  const currentTab = searchParams.get('tab') || 'services'
  const setTab = (tab: string) => setSearchParams({ tab })

  // 获取当前活动集群
  const { data: activeCluster } = useQuery({
    queryKey: ['activeCluster'],
    queryFn: api.getActiveCluster,
  })

  // 获取服务类型
  const { data: serviceTypesData, isLoading: isLoadingTypes } = useQuery({
    queryKey: ['serviceTypes'],
    queryFn: api.getServiceTypes,
    refetchInterval: 5000,
  })

  // 获取托管实例
  const { data: instancesData, isLoading: isLoadingInstances } = useQuery({
    queryKey: ['managedInstances'],
    queryFn: () => api.getManagedInstances(),
    refetchInterval: 3000,
  })

  // 获取基础设施服务
  const { data: infraData, isLoading: isLoadingInfra } = useQuery({
    queryKey: ['infrastructure'],
    queryFn: api.getInfrastructure,
    refetchInterval: 30000, // 30秒刷新一次
  })

  // 获取基础设施模板
  const { data: templatesData } = useQuery({
    queryKey: ['infrastructureTemplates'],
    queryFn: api.getInfrastructureTemplates,
  })

  // 启动服务
  const startMutation = useMutation({
    mutationFn: api.startManagedService,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['managedInstances'] })
      setStartingType(null)
    },
    onError: () => {
      setStartingType(null)
    },
  })

  // 停止服务
  const stopMutation = useMutation({
    mutationFn: (instanceId: string) => api.stopManagedService(instanceId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['managedInstances'] })
      setStoppingId(null)
    },
    onError: () => {
      setStoppingId(null)
    },
  })

  // 移除实例
  const removeMutation = useMutation({
    mutationFn: (instanceId: string) => api.removeInstance(instanceId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['managedInstances'] })
      setRemovingId(null)
    },
    onError: () => {
      setRemovingId(null)
    },
  })

  // 清除所有失败实例
  const clearFailedMutation = useMutation({
    mutationFn: () => api.clearFailedInstances(),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['managedInstances'] })
    },
  })

  // 停止所有
  // 一键启动基础服务：新用户不必知道「哪几个服务要先起」
  const CORE_SERVICES = ['flow_worker', 'delay_service', 'event_filter', 'schedule_service']
  const bootstrapMutation = useMutation({
    mutationFn: async () => {
      const results: string[] = []
      for (const svc of CORE_SERVICES) {
        const running = (serviceTypesData?.service_types || []).some(
          (t) => t.service_type === svc && t.running_count > 0
        )
        if (running) {
          results.push(`${svc}: 已在运行`)
          continue
        }
        const res = await api.startManagedService(svc)
        results.push(`${svc}: ${res.success ? '已启动' : `失败(${res.error || '未知'})`}`)
      }
      return results
    },
    onSuccess: (results) => {
      setBootstrapMsg(results.join(' · '))
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['managedInstances'] })
    },
    onError: (e: Error) => setBootstrapMsg(`启动失败: ${e.message}`),
  })

  const stopAllMutation = useMutation({
    mutationFn: () => api.stopAllManagedServices(),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['serviceTypes'] })
      queryClient.invalidateQueries({ queryKey: ['managedInstances'] })
    },
  })

  const handleStart = (serviceType: string) => {
    setStartingType(serviceType)
    startMutation.mutate(serviceType)
  }

  const handleStop = (instanceId: string) => {
    setStoppingId(instanceId)
    stopMutation.mutate(instanceId)
  }

  const handleRemove = (instanceId: string) => {
    setRemovingId(instanceId)
    removeMutation.mutate(instanceId)
  }

  if (isLoadingTypes || isLoadingInstances) {
    return (
      <div className="flex items-center justify-center h-full">
        <RefreshCw className="w-8 h-8 text-plaita-400 animate-spin" />
      </div>
    )
  }

  const runningCount = instancesData?.instances.filter(i => i.status === 'running').length || 0

  return (
    <div className="p-6 space-y-5">
      {/* 标题栏 */}
      <div className="flex items-center justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-page-title text-ink-primary">集群管理</h1>
          <p className="text-caption text-ink-muted mt-1">
            {activeCluster && (
              <>
                当前集群: <span className="text-plaita-400 font-medium">{activeCluster.name}</span>
                {' | '}
              </>
            )}
            当前模式: <span className="text-plaita-400 font-medium">{serviceTypesData?.mode || 'process'}</span>
            {' | '}
            运行中实例: <span className="text-plaita-400 font-medium tabular-nums">{runningCount}</span>
          </p>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {/* 快速测试按钮 - 总是显示 */}
          <Button variant="secondary" onClick={() => setShowQuickTest(true)}>
            <Zap size={14} className="text-plaita-400" />
            快速测试
          </Button>

          {currentTab === 'services' && (
            <>
              <Button
                variant="primary"
                onClick={() => bootstrapMutation.mutate()}
                disabled={bootstrapMutation.isPending}
                title="启动执行器/延迟/事件恢复/调度四个核心服务"
              >
                <Play size={14} />
                {bootstrapMutation.isPending ? '启动中…' : '一键启动基础服务'}
              </Button>

              <Button
                variant="secondary"
                onClick={() => clearFailedMutation.mutate()}
                disabled={clearFailedMutation.isPending || !instancesData?.instances.some(i => i.status !== 'running')}
              >
                <Trash2 size={14} />
                清除失败记录
              </Button>
              <Button
                variant="danger"
                onClick={() => setShowStopAll(true)}
                disabled={stopAllMutation.isPending || runningCount === 0}
              >
                <Square size={14} />
                停止全部
              </Button>
            </>
          )}
        </div>
      </div>

      {bootstrapMsg && (
        <div className="px-3 py-2 bg-plaita-500/10 text-plaita-400 text-caption rounded-md">
          {bootstrapMsg}
        </div>
      )}

      {/* 标签页 */}
      <div className="flex gap-1 border-b border-dark-700">
        <button
          onClick={() => setTab('services')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors
            ${currentTab === 'services' 
              ? 'border-plaita-500 text-plaita-400' 
              : 'border-transparent text-dark-400 hover:text-ink-primary'}`}
        >
          <div className="flex items-center gap-2">
            <Server className="w-4 h-4" />
            服务管理
          </div>
        </button>
        <button
          onClick={() => setTab('infrastructure')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors
            ${currentTab === 'infrastructure' 
              ? 'border-plaita-500 text-plaita-400' 
              : 'border-transparent text-dark-400 hover:text-ink-primary'}`}
        >
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4" />
            基础设施
            {infraData && (
              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                infraData.infrastructure.every(i => i.status === 'healthy' || i.status === 'disabled')
                  ? 'bg-plaita-500/20 text-plaita-400'
                  : 'bg-status-warning-dim text-status-warning'
              }`}>
                {infraData.infrastructure.filter(i => i.status === 'healthy').length}/{infraData.infrastructure.filter(i => i.enabled).length}
              </span>
            )}
          </div>
        </button>
        <button
          onClick={() => setTab('config')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors
            ${currentTab === 'config' 
              ? 'border-plaita-500 text-plaita-400' 
              : 'border-transparent text-dark-400 hover:text-ink-primary'}`}
        >
          <div className="flex items-center gap-2">
            <Settings className="w-4 h-4" />
            集群配置
          </div>
        </button>
      </div>

      {/* 服务管理标签页内容 */}
      {currentTab === 'services' && (
        <>
          <p className="text-caption text-ink-muted -mt-1 mb-3">
            服务是 console 拉起并管理生命周期的运行进程（执行流程、延迟唤醒、事件恢复等）。
            它们连接「基础设施」中的 Redis / Kafka 干活；下方「托管实例」即这些进程的运行实例。
          </p>
          {/* 可用服务类型 */}
          <section>
            <h2 className="text-section text-ink-primary mb-4 flex items-center gap-2">
              <Box className="w-5 h-5 text-plaita-400" />
              可用服务
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {serviceTypesData?.service_types.map((st) => (
                <ServiceTypeCard
                  key={st.service_type}
                  serviceType={st}
                  onStart={() => handleStart(st.service_type)}
                  isStarting={startingType === st.service_type}
                />
              ))}
            </div>
          </section>

          {/* 托管实例列表 */}
          <section>
            <h2 className="text-section text-ink-primary mb-4 flex items-center gap-2">
              <Server className="w-5 h-5 text-plaita-400" />
              托管实例
              <span className="text-sm text-dark-400 font-normal">
                ({instancesData?.total || 0} 个)
              </span>
            </h2>
            
            {instancesData && instancesData.instances.length > 0 ? (
              <div className="bg-dark-800 rounded-lg border border-dark-700 overflow-hidden">
                <table className="w-full">
                  <thead className="bg-dark-900">
                    <tr>
                      <th className="text-left py-3 px-4 text-sm font-medium text-dark-400">实例 ID</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-dark-400">服务类型</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-dark-400">进程/容器</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-dark-400">状态</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-dark-400">启动时间</th>
                      <th className="text-left py-3 px-4 text-sm font-medium text-dark-400">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {instancesData.instances.map((instance) => (
                      <ManagedInstanceRow
                        key={instance.instance_id}
                        instance={instance}
                        onStop={() => handleStop(instance.instance_id)}
                        isStopping={stoppingId === instance.instance_id}
                        onViewError={setErrorToView}
                        onRemove={() => handleRemove(instance.instance_id)}
                        isRemoving={removingId === instance.instance_id}
                        onViewLogs={() => setLogInstance({id: instance.instance_id, type: instance.service_type})}
                      />
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="bg-dark-800 rounded-lg border border-dark-700 p-8 text-center text-dark-400">
                暂无托管实例，点击上方"启动实例"按钮开始
              </div>
            )}
          </section>
        </>
      )}

      {/* 基础设施标签页内容 */}
      {currentTab === 'infrastructure' && (
        <section>
          {/* 内存模式提示 */}
          {activeCluster && (
            <MemoryModeNotice clusterId={activeCluster.id} />
          )}
          
          <p className="text-caption text-ink-muted mb-3">
            基础设施是服务依赖的有状态后端资源（Redis / Kafka / 数据库）——相当于水电气，
            服务是用电的工人。配置了 docker 镜像的资源可在此直接容器化启停与健康检查；
            未配置的（或外部部署的）由外部管理，console 仅做健康检查。
          </p>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-section text-ink-primary flex items-center gap-2">
              <Database className="w-5 h-5 text-plaita-400" />
              基础设施服务
              <span className="text-sm text-dark-400 font-normal">
                (依赖的存储、队列等底层服务)
              </span>
            </h2>
            <div className="flex items-center gap-2">
              <button
                onClick={() => queryClient.invalidateQueries({ queryKey: ['infrastructure'] })}
                disabled={isLoadingInfra}
                className="flex items-center gap-2 px-3 py-1.5 rounded bg-dark-700 hover:bg-dark-600 text-sm"
              >
                <RefreshCw className={`w-4 h-4 ${isLoadingInfra ? 'animate-spin' : ''}`} />
                刷新
              </button>
              <button
                onClick={() => setShowAddInfra(true)}
                className="flex items-center gap-2 px-4 py-1.5 rounded bg-plaita-500 hover:bg-plaita-600 text-sm"
              >
                <Plus className="w-4 h-4" />
                添加服务
              </button>
            </div>
          </div>

          {isLoadingInfra ? (
            <div className="flex items-center justify-center h-48">
              <RefreshCw className="w-8 h-8 text-plaita-400 animate-spin" />
            </div>
          ) : infraData && infraData.infrastructure.length > 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {infraData.infrastructure.map((infra) => (
                <InfrastructureCard 
                  key={infra.name}
                  infra={infra}
                  onRefresh={() => queryClient.invalidateQueries({ queryKey: ['infrastructure'] })}
                  onEdit={() => setEditingInfra(infra)}
                  onDelete={() => queryClient.invalidateQueries({ queryKey: ['infrastructure'] })}
                />
              ))}
            </div>
          ) : (
            <div className="bg-dark-800 rounded-lg border border-dark-700 p-8 text-center text-dark-400">
              <Database className="w-12 h-12 mx-auto mb-3 opacity-50" />
              <p>未配置基础设施服务</p>
              <p className="text-sm mt-2">
                点击"添加服务"按钮开始配置 Redis、Kafka、数据库等依赖服务
              </p>
              <button
                onClick={() => setShowAddInfra(true)}
                className="mt-4 flex items-center gap-2 px-4 py-2 rounded bg-plaita-500 hover:bg-plaita-600 text-sm mx-auto"
              >
                <Plus className="w-4 h-4" />
                添加服务
              </button>
            </div>
          )}

          {/* 快速添加模板区域 */}
          {templatesData && infraData && (
            <div className="mt-6 p-4 bg-dark-800 rounded-lg border border-dark-700">
              <h3 className="text-sm font-medium mb-3 text-dark-300">快速添加</h3>
              <div className="flex flex-wrap gap-2">
                {templatesData.templates
                  .filter(t => !infraData.infrastructure.find(i => i.name === t.name))
                  .map(template => (
                    <button
                      key={template.name}
                      onClick={() => {
                        setShowAddInfra(true)
                      }}
                      className="flex items-center gap-2 px-3 py-1.5 rounded bg-dark-700 hover:bg-dark-600 text-sm"
                    >
                      <InfraTypeIcon type={template.type} />
                      <span>{template.display_name}</span>
                      <Plus className="w-3 h-3 text-dark-400" />
                    </button>
                  ))}
                {templatesData.templates.every(t => infraData.infrastructure.find(i => i.name === t.name)) && (
                  <span className="text-sm text-dark-500">所有预置服务已添加</span>
                )}
              </div>
            </div>
          )}
        </section>
      )}

      {/* 配置标签页内容 */}
      {currentTab === 'config' && activeCluster && (
        <ConfigEditor clusterId={activeCluster.id} />
      )}

      {/* 错误详情弹窗 */}
      {errorToView && (
        <ErrorDialog 
          error={errorToView} 
          onClose={() => setErrorToView(null)} 
        />
      )}

      {/* 日志查看弹窗 */}
      {logInstance && (
        <LogDialog
          instanceId={logInstance.id}
          serviceType={logInstance.type}
          onClose={() => setLogInstance(null)}
        />
      )}

      {/* 添加基础设施服务弹窗 */}
      {showAddInfra && (
        <InfrastructureFormDialog
          templates={templatesData?.templates || []}
          onClose={() => setShowAddInfra(false)}
          onSave={() => queryClient.invalidateQueries({ queryKey: ['infrastructure'] })}
        />
      )}

      {/* 编辑基础设施服务弹窗 */}
      {editingInfra && (
        <InfrastructureFormDialog
          infra={editingInfra}
          templates={templatesData?.templates || []}
          onClose={() => setEditingInfra(null)}
          onSave={() => queryClient.invalidateQueries({ queryKey: ['infrastructure'] })}
        />
      )}

      {/* 快速测试弹窗 */}
      {showQuickTest && (
        <QuickTestDialog onClose={() => setShowQuickTest(false)} />
      )}

      {/* 停止全部确认：一次中断所有运行中实例，必须有明确确认 */}
      <ConfirmDialog
        open={showStopAll}
        title={`停止全部托管实例（${runningCount} 个运行中）？`}
        variant="danger"
        confirmLabel="全部停止"
        busy={stopAllMutation.isPending}
        onCancel={() => setShowStopAll(false)}
        onConfirm={() => stopAllMutation.mutate(undefined, { onSuccess: () => setShowStopAll(false) })}
      >
        正在执行的流程会被中断；实例可在下方重新启动。
      </ConfirmDialog>
    </div>
  )
}

