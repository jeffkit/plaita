import { useEffect, useMemo, useRef, useState } from 'react'
import * as YAML from 'yaml'

type Format = 'yaml' | 'json' | 'flow'

interface SourceViewPanelProps {
  /** 当前画布对应的 flow 定义（已序列化为 JS 对象，含 flow_id/version/desc/inputType/nodes 等）。 */
  flow: Record<string, unknown>
  /** 权威 @flow 源码（flow 定义的 metadata.source）；缺省时不显示 @flow 页签。 */
  source?: string
  /** 需要高亮并滚动到的源码行（1-based）；滚动定位完成后回调 onHighlightDone。 */
  highlightLine?: number | null
  onHighlightDone?: () => void
  onClose: () => void
}

/**
 * 源码查看面板：展示当前画布 flow 定义的 YAML / JSON，二者可一键切换；
 * 若 flow 定义带权威 @flow 源码（metadata.source，由 plaita-flow-coder 编译
 * 发布链路写入），追加「@flow」页签——按行号展示，支持外部按 source_line
 * 高亮定位（画布节点 → 源码行回溯）。
 */
export default function SourceViewPanel({ flow, source, highlightLine, onHighlightDone, onClose }: SourceViewPanelProps) {
  const [format, setFormat] = useState<Format>(source ? 'flow' : 'yaml')
  const [copied, setCopied] = useState(false)
  const highlightRef = useRef<HTMLSpanElement | null>(null)

  const text = useMemo(() => {
    if (format === 'yaml') {
      return YAML.stringify(flow, { sortMapEntries: false })
    }
    if (format === 'json') {
      return JSON.stringify(flow, null, 2)
    }
    return source ?? ''
  }, [flow, format, source])

  const sourceLines = useMemo(
    () => (format === 'flow' && source ? source.split('\n') : null),
    [format, source],
  )

  // 高亮行滚动定位（@flow 模式）
  useEffect(() => {
    if (format === 'flow' && highlightLine && highlightRef.current) {
      highlightRef.current.scrollIntoView({ block: 'center' })
      onHighlightDone?.()
    }
  }, [format, highlightLine, onHighlightDone])

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // 剪贴板不可用时静默忽略
    }
  }

  const formats: Format[] = source ? ['flow', 'yaml', 'json'] : ['yaml', 'json']
  const formatLabel: Record<Format, string> = { flow: '@flow', yaml: 'YAML', json: 'JSON' }

  return (
    <div className="w-[28rem] bg-dark-900/95 border-l border-dark-700 p-4 overflow-y-auto text-sm flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-dark-100">源码</h3>
        <button onClick={onClose} className="text-dark-400 hover:text-dark-100">✕</button>
      </div>

      <div className="flex items-center gap-2 mb-3">
        <div className="inline-flex rounded border border-dark-700 overflow-hidden text-xs">
          {formats.map((f) => (
            <button
              key={f}
              onClick={() => setFormat(f)}
              className={`px-3 py-1 uppercase ${
                format === f ? 'bg-plaita-600 text-white' : 'bg-dark-800 text-dark-300 hover:bg-dark-700'
              }`}
            >
              {formatLabel[f]}
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
          : format === 'json'
            ? 'JSON：与可视化编排工具互通的格式，Flow.from_string 可直接加载。'
            : '权威 @flow 源码（节点 id 即变量名 / 条件语义）；行号与画布节点 source_line 对应。'}
      </p>

      {sourceLines ? (
        <pre className="flex-1 overflow-auto rounded bg-dark-800 border border-dark-700 p-3 text-xs font-mono text-dark-100">
          {sourceLines.map((line, i) => {
            const no = i + 1
            const hit = highlightLine === no
            return (
              <div key={no} className={hit ? 'bg-plaita-600/30 -mx-3 px-3' : ''}>
                <span ref={hit ? highlightRef : undefined} className="inline-block w-8 select-none text-right pr-2 text-dark-500">{no}</span>
                <span className="whitespace-pre">{line || ' '}</span>
              </div>
            )
          })}
        </pre>
      ) : (
        <pre className="flex-1 overflow-auto rounded bg-dark-800 border border-dark-700 p-3 text-xs font-mono text-dark-100 whitespace-pre">
          {text}
        </pre>
      )}
    </div>
  )
}
