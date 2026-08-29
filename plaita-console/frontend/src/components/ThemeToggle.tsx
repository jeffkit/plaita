/**
 * 明暗主题切换（DESIGN.md §2.6）
 * 主题由 html[data-theme] 驱动；初始值由 index.html 内联脚本在渲染前设置
 * （localStorage > 系统偏好 > 暗色），本组件只负责切换与持久化。
 */

import { useEffect, useState } from 'react'
import { Moon, Sun } from 'lucide-react'

type Theme = 'dark' | 'light'

function getInitialTheme(): Theme {
  const saved = localStorage.getItem('plaita-theme')
  if (saved === 'light' || saved === 'dark') return saved
  return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
}

export default function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(getInitialTheme)

  useEffect(() => {
    document.documentElement.dataset.theme = theme
    localStorage.setItem('plaita-theme', theme)
  }, [theme])

  return (
    <button
      type="button"
      onClick={() => setTheme(t => (t === 'dark' ? 'light' : 'dark'))}
      title={theme === 'dark' ? '切换到亮色模式' : '切换到暗色模式'}
      aria-label={theme === 'dark' ? '切换到亮色模式' : '切换到暗色模式'}
      className="p-1.5 rounded-lg text-ink-muted hover:text-ink-primary hover:bg-elevated transition-colors duration-150 shrink-0"
    >
      {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
    </button>
  )
}
