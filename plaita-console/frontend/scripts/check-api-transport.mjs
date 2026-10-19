#!/usr/bin/env node
/**
 * S17-03 / S17-01 / S17-02 / S17-04 行为与接线断言（无需前端测试框架）。
 *
 *   node scripts/check-api-transport.mjs
 *   # 等价：pnpm check:api-transport
 *
 * [A] 直接 import `src/services/transport.ts` 的纯函数，断言传输层行为；
 * [B] 静态接线断言：页面/后端确实消费了这些分支（被断言形状就是出货形状）。
 * 任一断言失败 → 退出码 1。
 */
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  API_TIMEOUTS,
  ApiError,
  buildHttpError,
  decodeSuccessBody,
  isTerminalStatus,
  resolveApiUrl,
  shouldRetryQuery,
} from '../src/services/transport.ts'

const here = dirname(fileURLToPath(import.meta.url))
const read = (p) => readFileSync(join(here, p), 'utf8')

let checks = 0
const failures = []
function check(name, fn) {
  checks += 1
  try {
    fn()
    console.log(`  ✓ ${name}`)
  } catch (e) {
    const message = String(e.message).slice(0, 300)
    failures.push(`${name}: ${message}`)
    console.log(`  ✗ ${name}: ${message}`)
  }
}

console.log('[A] 传输层纯函数（transport.ts）')
check('200 + text/html → spa_fallback，消息含「接口不存在/版本不一致」', () => {
  try {
    decodeSuccessBody('text/html', '<!doctype html><html><body>app</body></html>', '/api/x')
    assert.fail('应抛错')
  } catch (e) {
    assert.ok(e instanceof ApiError)
    assert.equal(e.kind, 'spa_fallback')
    assert.match(e.userMessage, /接口不存在|版本不一致/)
    assert.doesNotMatch(e.userMessage, /Unexpected token/)
  }
})
check('200 + text/plain → non_json，带 url 与 bodySnippet', () => {
  try {
    decodeSuccessBody('text/plain', 'oops', '/api/x')
    assert.fail('应抛错')
  } catch (e) {
    assert.equal(e.kind, 'non_json')
    assert.match(e.userMessage, /非 JSON/)
  }
})
check('200 + application/json → 原样解析', () => {
  assert.deepEqual(decodeSuccessBody('application/json; charset=utf-8', '{"a":1}', '/api/x'), { a: 1 })
})
check('JSON content-type 但 HTML body → spa_fallback（不让 SyntaxError 冒到 UI）', () => {
  try {
    decodeSuccessBody('application/json', '<html><body>gateway</body></html>', '/api/x')
    assert.fail('应抛错')
  } catch (e) {
    assert.equal(e.kind, 'spa_fallback')
  }
})
check('坏 JSON → non_json', () => {
  try {
    decodeSuccessBody('application/json', '{bad', '/api/x')
    assert.fail('应抛错')
  } catch (e) {
    assert.equal(e.kind, 'non_json')
  }
})
check('502 + HTML 错误页 → kind=http、status=502，不丢状态码', () => {
  const err = buildHttpError(502, '<html>bad gateway</html>', 'text/html', '/api/x')
  assert.equal(err.kind, 'http')
  assert.equal(err.status, 502)
  assert.match(err.userMessage, /502/)
})
check('409 结构化 detail 原样保留（message/conflict/latest_updated_at）', () => {
  const detail = { message: '版本冲突', conflict: true, latest_updated_at: '2026-10-19T00:00:00Z' }
  const err = buildHttpError(409, JSON.stringify({ detail }), 'application/json', '/api/x')
  assert.equal(err.status, 409)
  assert.equal(err.userMessage, '版本冲突')
  assert.deepEqual(err.detail, detail)
})
check('超时/网络重试、401/403/404/409/422 不重试、最多 1 次', () => {
  assert.equal(shouldRetryQuery(0, new ApiError('t', { kind: 'timeout', url: '/' })), true)
  assert.equal(shouldRetryQuery(0, new ApiError('n', { kind: 'network', url: '/' })), true)
  assert.equal(shouldRetryQuery(0, { status: 500 }), true)
  assert.equal(shouldRetryQuery(0, { status: 408 }), true)
  assert.equal(shouldRetryQuery(0, { status: 429 }), true)
  assert.equal(shouldRetryQuery(0, { status: 401 }), false)
  assert.equal(shouldRetryQuery(0, { status: 403 }), false)
  assert.equal(shouldRetryQuery(0, { status: 404 }), false)
  assert.equal(shouldRetryQuery(0, { status: 409 }), false)
  assert.equal(shouldRetryQuery(0, { status: 422 }), false)
  assert.equal(shouldRetryQuery(1, { status: 500 }), false)
})
check('终态判定覆盖 completed/failed/cancelled/error', () => {
  for (const s of ['completed', 'failed', 'cancelled', 'error']) assert.equal(isTerminalStatus(s), true)
  for (const s of ['running', 'suspended', '', null, undefined]) assert.equal(isTerminalStatus(s), false)
})
check('子路径解析：根路径/已带前缀/绝对 URL', () => {
  assert.equal(resolveApiUrl('/api/x', ''), '/api/x')
  assert.equal(resolveApiUrl('/api/x', '/plaita'), '/plaita/api/x')
  assert.equal(resolveApiUrl('/plaita/api/x', '/plaita'), '/plaita/api/x')
  assert.equal(resolveApiUrl('/health', '/plaita'), '/plaita/health')
  assert.equal(resolveApiUrl('https://x/y', '/plaita'), 'https://x/y')
})
check('超时分级常量：15s/60s/120s/600s/10s', () => {
  assert.equal(API_TIMEOUTS.json, 15_000)
  assert.equal(API_TIMEOUTS.lifecycle, 60_000)
  assert.equal(API_TIMEOUTS.dryRun, 120_000)
  assert.equal(API_TIMEOUTS.supervisorIterate, 600_000)
  assert.equal(API_TIMEOUTS.streamConnect, 10_000)
})

console.log('[B] 出货接线静态断言')
const apiSrc = read('../src/services/api.ts')
const loginSrc = read('../src/pages/Login.tsx')
const appSrc = read('../src/App.tsx')
const copilotSrc = read('../src/components/flow/CopilotPanel.tsx')
const detailSrc = read('../src/pages/ExecutionDetail.tsx')
const logsSrc = read('../src/pages/Logs.tsx')
const supervisorSrc = read('../src/pages/Supervisor.tsx')
const aiGenSrc = read('../src/components/flow/AiGenerateDialog.tsx')
const mainSrc = read('../src/main.tsx')
const serviceHintSrc = read('../src/components/ServiceHint.tsx')
const hookSrc = read('../src/hooks/useServiceOnline.ts')
const backendMain = read('../../backend/main.py')
const executionsPy = read('../../backend/api/executions.py')
const readme = read('../../README.md')

check('#266：前端源码/README 无管理密钥通道', () => {
  for (const token of ['VITE_PLAITA_ADMIN_API_KEY', 'plaita_admin_api_key', 'getAdminApiKey']) {
    assert.equal(apiSrc.includes(token), false, `api.ts 仍含 ${token}`)
    assert.equal(readme.includes(token), false, `README 仍含 ${token}`)
  }
  assert.equal(apiSrc.includes('X-Admin-API-Key'), false, 'api.ts 仍含 X-Admin-API-Key')
})
check('#266：authHeaders 只发 Bearer + 平台管理员 X-Tenant-ID', () => {
  assert.match(apiSrc, /headers\['Authorization'\] = `Bearer \$\{token\}`/)
  assert.match(apiSrc, /headers\['X-Tenant-ID'\] = getTenant\(\)/)
})
check('#210/S17-02：apiFetch 调用时合并 authHeaders（Supervisor 探针认证性质）', () => {
  assert.match(apiSrc, /for \(const \[key, value\] of Object\.entries\(authHeaders\(\)\)\)/)
  assert.match(apiSrc, /if \(!headers\.has\(key\)\) headers\.set\(key, value\)/)
})
check('S17-03：request 有 AbortController、非 JSON 守卫、204/空 body 防御', () => {
  assert.match(apiSrc, /new AbortController\(\)/)
  assert.match(apiSrc, /decodeSuccessBody\(/)
  assert.match(apiSrc, /response\.status === 204/)
  assert.match(apiSrc, /apiFetch\(/)
})
check('S17-01：Login 显式传内存 token + authRedirect:false，成功后才 setSession', () => {
  assert.match(loginSrc, /switchTenant\(tenantId,\s*\{[\s\S]*?token:\s*pendingSession\.token/)
  assert.match(loginSrc, /authRedirect:\s*false/)
  const pick = loginSrc.slice(loginSrc.indexOf('const pickTenant'), loginSrc.indexOf('const backToLogin'))
  assert.ok(pick.indexOf('await api.switchTenant') < pick.indexOf('setSession('), 'setSession 必须晚于 await switchTenant')
  assert.doesNotMatch(pick, /localStorage\.(?:setItem|removeItem)/)
})
check('S17-01：TenantSwitcher 失败分支不再 setTenant', () => {
  assert.doesNotMatch(appSrc, /catch\s*\{\s*setTenant\(tenantId\)/)
  assert.match(appSrc, /\[tenant-switch\] 切换失败/)
  assert.match(appSrc, /setSwitchError\(message\)/)
})
check('A2R17-01：TenantSwitcher 全量视角先服务端清租户，成功后落本地', () => {
  const switcher = appSrc.slice(appSrc.indexOf('const switchTo'), appSrc.indexOf('const options = memberships'))
  assert.match(switcher, /await api\.switchTenant\(tenantId \|\| null\)/)
  assert.ok(switcher.indexOf('await api.switchTenant') < switcher.indexOf('setTenant(tenantId, res.role)'),
    'setTenant 必须晚于 await switchTenant（全量视角同样服务端权威）')
  assert.match(appSrc, /全量视角为只读，写操作请先选择具体租户/)
})
check('A2R17-01：api.switchTenant 以 null 表示全量视角', () => {
  assert.match(apiSrc, /async switchTenant\(\s*tenantId: string \| null/)
  assert.match(apiSrc, /JSON\.stringify\(\{ tenant_id: tenantId \}\)/)
})
check('S17-02：Copilot 使用 API_BASE + apiFetch，无字面量 /api/copilot', () => {
  assert.match(copilotSrc, /url:\s*`\$\{API_BASE\}\/copilot`/)
  assert.match(copilotSrc, /fetch:\s*\(\(input: RequestInfo \| URL, init\?: RequestInit\)/)
  assert.match(copilotSrc, /apiFetch\(String\(input\), init, \{ stream: true \}\)/)
  assert.doesNotMatch(copilotSrc, /url:\s*'\/api\/copilot'/)
  assert.match(copilotSrc, /abortRun/)
})
check('S17-03：直连调用点全部迁移 apiFetch', () => {
  assert.doesNotMatch(logsSrc, /await fetch\(/)
  assert.doesNotMatch(supervisorSrc, /await fetch\(/)
  assert.doesNotMatch(aiGenSrc, /await fetch\(/)
  assert.match(logsSrc, /apiFetch\(/)
  assert.match(supervisorSrc, /API_TIMEOUTS\.supervisorIterate/)
  assert.match(detailSrc, /API_TIMEOUTS\.streamConnect/)
})
check('S17-03：查询重试策略 + mutation retry:0 + MutationCache 兜底', () => {
  assert.match(mainSrc, /retry:\s*shouldRetryQuery/)
  assert.match(mainSrc, /mutations:\s*\{[\s\S]*?retry:\s*0/)
  assert.match(mainSrc, /new MutationCache\(\{/)
  assert.match(serviceHintSrc, /本地单机档：流程在 console 进程内执行/)
  assert.match(hookSrc, /local_mode/)
})
check('S17-03 3C：SPA 对 /api/* 返回 JSON 404', () => {
  assert.match(backendMain, /def is_api_path/)
  assert.match(backendMain, /JSONResponse\(/)
  assert.match(backendMain, /接口不存在/)
})
check('S17-04：后端发 finished，前端监听并终态守卫', () => {
  assert.match(executionsPy, /yield \{"event": "finished", "data": payload\}/)
  assert.match(detailSrc, /addEventListener\('finished'/)
  assert.match(detailSrc, /isTerminalStatus\(cached\?\.status\)/)
  assert.match(detailSrc, /refetchInterval:\s*\(query\)\s*=>/)
})

if (failures.length > 0) {
  console.error(`\n✗ ${failures.length}/${checks} 断言失败：`)
  for (const failure of failures) console.error(`  - ${failure}`)
  process.exit(1)
}
console.log(`\n✓ ${checks}/${checks} 断言通过`)
