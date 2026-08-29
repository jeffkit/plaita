import { useMemo, useState } from 'react'
import * as YAML from 'yaml'

type Format = 'yaml' | 'json'

interface SourceViewPanelProps {
  /** 当前画布对应的 flow 定义（已序列化为 JS 对象，含 flow_id/version/desc/inputType/nodes 等）。 */
  flow: Record<string, unknown>
  onClose: () => void
}

/**
 * 源码查看面板：把当前画布的 flow 定义以 YAML 或 JSON 展示，二者可一键切换。
 * 与后端 Flow.from_string / Flow.from_file 的自动识别一致——YAML 与 JSON 是同一份定义的不同序列化形态。
 */
export default function SourceViewPanel({ flow, onClose }: SourceViewPanelProps) {
  const [format, setFormat] = useState<Format>('yaml')
  const [copied, setCopied] = useState(false)

  const text = useMemo(() => {
    if (format === 'yaml') {
      return YAML.stringify(flow, { sortMapEntries: false })
    }
    return JSON.stringify(flow, null, 2)
  }, [flow, format])

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // 剪贴板不可用时静默忽略
    }
  }

  return (
    <div className="w-[28rem] bg-dark-900/95 border-l border-dark-700 p-4 overflow-y-auto text-sm flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-dark-100">源码</h3>
        <button onClick={onClose} className="text-dark-400 hover:text-dark-100">✕</button>
      </div>

      <div className="flex items-center gap-2 mb-3">
        <div className="inline-flex rounded border border-dark-700 overflow-hidden text-xs">
          {(['yaml', 'json'] as Format[]).map((f) => (
            <button
              key={f}
              onClick={() => setFormat(f)}
              className={`px-3 py-1 uppercase ${
                format === f ? 'bg-plaita-600 text-white' : 'bg-dark-800 text-dark-300 hover:bg-dark-700'
              }`}
            >
              {f}
            </button>
          ))}
        </div>
        <div className="flex-1" />
        <button
          onClick={copy}
          className="bg-dark-700 hover:bg-dark-600 px-2.5 py-1 rounded text-xs text-dark-100"
        >
          {copied ? '已复制' : '复制'}
        </button>
      </div>

      <p className="text-xs text-dark-400 mb-2">
        {format === 'yaml'
          ? 'YAML：配置文件首选，支持注释。保存到文件用 .yaml / .yml 后缀，Flow.from_file 可直接加载。'
          : 'JSON：与可视化编排工具互通的格式，Flow.from_string 可直接加载。'}
      </p>

      <pre className="flex-1 overflow-auto rounded bg-dark-800 border border-dark-700 p-3 text-xs font-mono text-dark-100 whitespace-pre">
        {text}
      </pre>
    </div>
  )
}
