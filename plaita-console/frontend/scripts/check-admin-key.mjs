#!/usr/bin/env node
/**
 * #266 回归护栏：前端不得再出现浏览器管理密钥通道。
 *
 *   node scripts/check-admin-key.mjs                 # 源码/文档扫描
 *   node scripts/check-admin-key.mjs --dist dist     # 构建产物扫描（CI）
 *   node scripts/check-admin-key.mjs --dist dist --key "$VITE_PLAITA_ADMIN_API_KEY"
 *                                                    # 额外断言构建期哨兵值未内联
 *
 * 背景：Vite 会把 `VITE_*` 变量在构建期字面内联进公开 bundle。平台管理
 * Key 一旦从浏览器侧读取，未认证 `GET /assets/index-*.js` 即可拿到平台身份。
 * 浏览器端只允许会话 Bearer；`PLAITA_CONSOLE_ADMIN_API_KEY` 仅供服务端/CLI。
 *
 * 任一命中 → 退出码 1。脚本不依赖第三方包（node ≥18）。
 */
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const frontendRoot = resolve(here, '..')
const repoRoot = resolve(frontendRoot, '../..')

const SOURCE_FORBIDDEN = [
  'VITE_PLAITA_ADMIN_API_KEY',
  'plaita_admin_api_key',
  'getAdminApiKey',
  'X-Admin-API-Key',
]
// 文档只禁「前端注入路径」；服务端 Key 的 X-Admin-API-Key 用法是正当说明
const DOC_FORBIDDEN = ['VITE_PLAITA_ADMIN_API_KEY', 'plaita_admin_api_key', 'getAdminApiKey']
const DIST_FORBIDDEN = [...SOURCE_FORBIDDEN]
// 构建期环境变量任何「敏感形态」都不允许（#266 判据 5）
const SENSITIVE_VITE = /\bVITE_[A-Z0-9_]*(?:KEY|SECRET|TOKEN|PASSWORD)\b/g

const args = process.argv.slice(2)
const opt = (name) => {
  const idx = args.indexOf(name)
  return idx >= 0 ? args[idx + 1] : null
}
const distArg = opt('--dist')
const keyArg = opt('--key')

const failures = []
let scanned = 0

function scanText(file, text, forbidden, label) {
  scanned += 1
  for (const token of forbidden) {
    let from = 0
    for (;;) {
      const idx = text.indexOf(token, from)
      if (idx < 0) break
      const line = text.slice(0, idx).split('\n').length
      failures.push(`${label}: ${relative(repoRoot, file)}:${line} 命中 ${token}`)
      from = idx + token.length
    }
  }
  for (const match of text.matchAll(SENSITIVE_VITE)) {
    const line = text.slice(0, match.index).split('\n').length
    failures.push(
      `${label}: ${relative(repoRoot, file)}:${line} 使用敏感构建期变量 ${match[0]}`,
    )
  }
}

function walk(dir, predicate, visit) {
  for (const name of readdirSync(dir)) {
    if (name === 'node_modules' || name === 'dist' || name === '.git') continue
    const full = join(dir, name)
    const st = statSync(full)
    if (st.isDirectory()) walk(full, predicate, visit)
    else if (predicate(full)) visit(full)
  }
}

console.log('[#266] 前端管理密钥回归扫描')

// 1) 源码：frontend/src + 构建配置/入口
const sourceFiles = []
walk(join(frontendRoot, 'src'), (f) => /\.(?:[cm]?[jt]sx?)$/.test(f), (f) => sourceFiles.push(f))
for (const extra of ['vite.config.ts', 'vite.config.js', 'index.html', 'package.json']) {
  const full = join(frontendRoot, extra)
  try {
    if (statSync(full).isFile()) sourceFiles.push(full)
  } catch {
    /* 可选文件 */
  }
}
for (const file of sourceFiles) {
  scanText(file, readFileSync(file, 'utf8'), SOURCE_FORBIDDEN, 'source')
}

// 2) 文档：console README（禁止把前端注入路径写回文档）
const readme = join(frontendRoot, '..', 'README.md')
scanText(readme, readFileSync(readme, 'utf8'), DOC_FORBIDDEN, 'docs')

// 3) 构建产物：dist/assets/*.js|css|html
if (distArg) {
  const dist = resolve(process.cwd(), distArg)
  let entries = []
  try {
    entries = readdirSync(dist, { recursive: true })
  } catch (e) {
    failures.push(`bundle: 构建产物目录不存在/不可读: ${dist} (${e.message})`)
    entries = []
  }
  const bundleFiles = entries
    .map((name) => join(dist, name))
    .filter((f) => /\.(?:js|css|html)$/.test(f) && statSync(f).isFile())
  if (bundleFiles.length === 0) {
    failures.push(`bundle: ${dist} 下没有 js/css/html 产物——构建未完成或被跳过`)
  }
  for (const file of bundleFiles) {
    const text = readFileSync(file, 'utf8')
    scanText(file, text, DIST_FORBIDDEN, 'bundle')
    if (keyArg && text.includes(keyArg)) {
      failures.push(`bundle: ${relative(repoRoot, file)} 内联了构建期管理密钥值（长度 ${keyArg.length}）`)
    }
  }
}

if (failures.length > 0) {
  console.error(`\n✗ #266 扫描失败（${failures.length} 处）：`)
  for (const failure of failures) console.error(`  - ${failure}`)
  console.error('\n浏览器端只允许会话 Bearer；管理 API Key 仅限服务端/CLI。')
  process.exit(1)
}

console.log(`✓ #266 扫描通过（${scanned} 个文件${distArg ? `，含产物 ${distArg}` : ''}）`)
