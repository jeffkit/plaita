import CodeMirror from '@uiw/react-codemirror'
import { javascript } from '@codemirror/lang-javascript'
import { json } from '@codemirror/lang-json'
import { python } from '@codemirror/lang-python'

/**
 * 代码编辑器 widget（CodeMirror 6）：用于 code/script/template 类多行字段。
 * 语言按同级 language/lang 字段推断，缺省 javascript。
 */
export default function CodeEditor({
  value,
  onChange,
  language,
  height = '180px',
}: {
  value: string
  onChange: (v: string) => void
  language?: string
  height?: string
}) {
  const extensions =
    language === 'python'
      ? [python()]
      : language === 'json'
        ? [json()]
        : [javascript()]
  return (
    <div className="border border-line rounded-md overflow-hidden text-[12px]">
      <CodeMirror
        value={value ?? ''}
        height={height}
        extensions={extensions}
        onChange={onChange}
        theme="dark"
        basicSetup={{
          lineNumbers: true,
          foldGutter: false,
          highlightActiveLine: true,
          autocompletion: false,
        }}
      />
    </div>
  )
}

/** 代码类字段名启发式：命中则用 CodeEditor 渲染 */
export function isCodeField(key: string): boolean {
  return /^(code|script|source|sql)$/.test(key) || /template|snippet/.test(key)
}

/** 从同级字段推断代码语言 */
export function inferLanguage(fields: Record<string, unknown>): string | undefined {
  const lang = (fields.language ?? fields.lang) as string | undefined
  if (typeof lang === 'string' && lang) return lang.toLowerCase()
  return undefined
}
