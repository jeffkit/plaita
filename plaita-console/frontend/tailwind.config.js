/**
 * Plaita Console design tokens — 权威定义见 ../DESIGN.md
 * 规则：新颜色必须先进这里，禁止组件内写一次性 hex。
 *
 * 主题机制（DESIGN.md §2.6）：所有颜色指向 index.css 的 CSS 变量（R G B 三元组），
 * 随 html[data-theme="dark" | "light"] 整体翻转。`<alpha-value>` 使 /xx 透明度修饰符可用；
 * line 与 status 的 dim 底使用固定透明度（变量只换色相）。
 *
 * 兼容性说明：`dark.*` 与 `plaita.*` 色阶键名保留（存量页面大量引用），值随主题翻转：
 * 亮色下 dark-100 系变深色文字、dark-800/900 变白色/浅灰背景，plaita 下移两档保对比度。
 */

/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // 品牌绿：仅用于主按钮 / 激活态 / running / 焦点环 / Logo 点（DESIGN.md §2.4）
        plaita: {
          50: 'rgb(var(--c-plaita-50) / <alpha-value>)',
          100: 'rgb(var(--c-plaita-100) / <alpha-value>)',
          200: 'rgb(var(--c-plaita-200) / <alpha-value>)',
          300: 'rgb(var(--c-plaita-300) / <alpha-value>)',
          400: 'rgb(var(--c-plaita-400) / <alpha-value>)',
          500: 'rgb(var(--c-plaita-500) / <alpha-value>)',
          600: 'rgb(var(--c-plaita-600) / <alpha-value>)',
          700: 'rgb(var(--c-plaita-700) / <alpha-value>)',
          800: 'rgb(var(--c-plaita-800) / <alpha-value>)',
          900: 'rgb(var(--c-plaita-900) / <alpha-value>)',
        },
        // 存量灰阶（键名保留，随主题翻转）
        dark: {
          50: 'rgb(var(--c-dark-50) / <alpha-value>)',
          100: 'rgb(var(--c-dark-100) / <alpha-value>)',
          200: 'rgb(var(--c-dark-200) / <alpha-value>)',
          300: 'rgb(var(--c-dark-300) / <alpha-value>)',
          400: 'rgb(var(--c-dark-400) / <alpha-value>)',
          500: 'rgb(var(--c-dark-500) / <alpha-value>)',
          600: 'rgb(var(--c-dark-600) / <alpha-value>)',
          700: 'rgb(var(--c-dark-700) / <alpha-value>)',
          800: 'rgb(var(--c-dark-800) / <alpha-value>)',
          900: 'rgb(var(--c-dark-900) / <alpha-value>)',
          950: 'rgb(var(--c-dark-950) / <alpha-value>)',
        },
        // ── 语义背景四层（DESIGN.md §2.1，纯色，禁止渐变）──
        canvas: 'rgb(var(--c-canvas) / <alpha-value>)',    // 页面底
        surface: 'rgb(var(--c-surface) / <alpha-value>)',  // 卡片/面板/侧边栏
        elevated: 'rgb(var(--c-elevated) / <alpha-value>)', // 浮层/hover 底
        inset: 'rgb(var(--c-inset) / <alpha-value>)',      // 下沉区
        // ── 描边（暗色=白透明、亮色=黑透明；固定透明度，DESIGN.md §2.3）──
        line: {
          DEFAULT: 'rgb(var(--c-line) / 0.07)',
          strong: 'rgb(var(--c-line) / 0.12)',
        },
        // ── 品牌绿上的文字/图标色（随主题翻转，DESIGN.md §2.4）──
        onAccent: 'rgb(var(--c-on-accent) / <alpha-value>)',
        // ── 文字四档（DESIGN.md §2.2）──
        ink: {
          primary: 'rgb(var(--c-ink-primary) / <alpha-value>)',
          secondary: 'rgb(var(--c-ink-secondary) / <alpha-value>)',
          muted: 'rgb(var(--c-ink-muted) / <alpha-value>)',
          faint: 'rgb(var(--c-ink-faint) / <alpha-value>)',
        },
        // ── 语义状态色（DESIGN.md §2.5）；dim = badge 底（固定 12% 透明）──
        status: {
          running:   { DEFAULT: 'rgb(var(--c-status-running) / <alpha-value>)',   dim: 'rgb(var(--c-status-running) / 0.12)' },
          success:   { DEFAULT: 'rgb(var(--c-status-success) / <alpha-value>)',   dim: 'rgb(var(--c-status-success) / 0.12)' },
          error:     { DEFAULT: 'rgb(var(--c-status-error) / <alpha-value>)',     dim: 'rgb(var(--c-status-error) / 0.12)' },
          warning:   { DEFAULT: 'rgb(var(--c-status-warning) / <alpha-value>)',   dim: 'rgb(var(--c-status-warning) / 0.12)' },
          pending:   { DEFAULT: 'rgb(var(--c-status-pending) / <alpha-value>)',   dim: 'rgb(var(--c-status-pending) / 0.12)' },
          cancelled: { DEFAULT: 'rgb(var(--c-status-cancelled) / <alpha-value>)', dim: 'rgb(var(--c-status-cancelled) / 0.12)' },
        },
      },
      // ── 混合式排版：UI 用 Inter，数据用 JetBrains Mono（DESIGN.md §1）──
      fontFamily: {
        sans: ['Inter Variable', 'Inter', '-apple-system', 'BlinkMacSystemFont', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', 'sans-serif'],
        mono: ['JetBrains Mono Variable', 'JetBrains Mono', 'SF Mono', 'Menlo', 'Consolas', 'monospace'],
      },
      // ── 字号阶梯（DESIGN.md §1，text-* 可直接用）──
      fontSize: {
        'page-title': ['20px', { lineHeight: '28px', letterSpacing: '-0.02em', fontWeight: '600' }],
        'section': ['14px', { lineHeight: '20px', fontWeight: '600' }],
        'body': ['13px', { lineHeight: '20px' }],
        'caption': ['12px', { lineHeight: '16px' }],
        'micro': ['11px', { lineHeight: '14px', letterSpacing: '0.06em', fontWeight: '500' }],
        'data': ['13px', { lineHeight: '20px' }],
        'data-sm': ['12px', { lineHeight: '16px' }],
      },
      boxShadow: {
        card: 'var(--shadow-card)',
        pop: 'var(--shadow-pop)',
      },
      keyframes: {
        breathe: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.45' },
        },
        'fade-up': {
          from: { opacity: '0', transform: 'translateY(6px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        fade: {
          from: { opacity: '0' },
          to: { opacity: '1' },
        },
        /* 对话框面板：fade + 轻微上浮缩放，180ms 出曲线（DESIGN.md §4） */
        pop: {
          from: { opacity: '0', transform: 'scale(0.97) translateY(4px)' },
          to: { opacity: '1', transform: 'scale(1) translateY(0)' },
        },
      },
      animation: {
        breathe: 'breathe 2s ease-in-out infinite',
        'fade-up': 'fade-up 0.35s cubic-bezier(0.16, 1, 0.3, 1) both',
        fade: 'fade 0.15s ease-out both',
        pop: 'pop 0.18s cubic-bezier(0.16, 1, 0.3, 1) both',
      },
    },
  },
  plugins: [],
}
