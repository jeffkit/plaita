#!/usr/bin/env node
/**
 * 数据面 Redis 读面契约（A2R40-01 / #276 前端采数，第六十五轮 B2）。
 *
 *   node scripts/check-cluster-dataplane.mjs
 *   # 等价：pnpm check:cluster-dataplane
 *
 * 后端 GET /clusters、/clusters/active、/clusters/{id} 与 POST /clusters/switch
 * 响应统一携带 data_plane_redis_url / data_plane_redis_source / warnings；
 * PUT /clusters/{id} 被 409 cluster_redis_url_pinned 拒绝时 detail.message 带
 * 生效来源与改面指引。本脚本钉住前端确实消费了这些字段：
 *
 * [A] 行为断言：直接 import `src/services/transport.ts`，用后端真实 409
 *     detail 形状断言错误文案携带生效来源 + 脱敏 URL + 改面指引，且结构化
 *     detail（effective_source / effective_redis_url）不被丢弃；
 * [B] 静态接线断言：ClusterInfo 类型、警示条组件、集群页挂载点、切换器
 *     行级错位标记与 activeCluster 失配刷新确实存在（被断言形状即出货形状）。
 * 任一断言失败 → 退出码 1。
 */
import assert from 'node:assert/strict'
import { readFileSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import { ApiError, buildHttpError } from '../src/services/transport.ts'

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

console.log('[A] 409 cluster_redis_url_pinned 文案契约（transport.ts 行为）')
// 与后端 api/cluster.py 的 error_detail(...) 输出同形（A2R40-01 测试同源）
const PINNED_DETAIL = {
  code: 'cluster_redis_url_pinned',
  message:
    '数据面 Redis 由集群配置文件 redis.url 钉住（当前生效 redis://pin-redis:6379/0）；' +
    '注册表元数据在 plaita#65 优先级序中仅为末位，本次修改不会改变数据面连接。' +
    '请改用集群配置编辑面（PUT /api/clusters/{id}/config）后重试，' +
    '或先把元数据对齐当前生效 URL。',
  effective_source: 'config_file',
  effective_redis_url: 'redis://pin-redis:6379/0',
  cluster_id: 'pinned',
}

check('409 detail.message 成为用户文案（不经 defaultHttpMessage 折叠）', () => {
  const err = buildHttpError(
    409,
    JSON.stringify({ detail: PINNED_DETAIL }),
    'application/json',
    '/api/clusters/pinned',
  )
  assert.ok(err instanceof ApiError)
  assert.equal(err.status, 409)
  assert.match(err.userMessage, /数据面 Redis 由集群配置文件 redis\.url 钉住/)
  assert.doesNotMatch(err.userMessage, /请求失败（HTTP 409）/)
})

check('错误提示带生效来源 + 脱敏生效 URL + 改面指引', () => {
  const err = buildHttpError(
    409,
    JSON.stringify({ detail: PINNED_DETAIL }),
    'application/json',
    '/api/clusters/pinned',
  )
  assert.match(err.userMessage, /当前生效 redis:\/\/pin-redis:6379\/0/)
  assert.match(err.userMessage, /请改用集群配置编辑面/)
  assert.match(err.userMessage, /或先把元数据对齐当前生效 URL/)
})

check('结构化 detail 保留 effective_source / effective_redis_url', () => {
  const err = buildHttpError(
    409,
    JSON.stringify({ detail: PINNED_DETAIL }),
    'application/json',
    '/api/clusters/pinned',
  )
  assert.equal(err.detail.effective_source, 'config_file')
  assert.equal(err.detail.effective_redis_url, 'redis://pin-redis:6379/0')
  assert.equal(err.detail.code, 'cluster_redis_url_pinned')
})

console.log('[B] 前端接线（静态形状 = 出货形状）')
check('api.ts ClusterInfo 携带 data_plane_redis_url / data_plane_redis_source / warnings', () => {
  const source = read('../src/services/api.ts')
  const block = source.match(/export interface ClusterInfo \{[\s\S]*?\n\}/)
  assert.ok(block, 'ClusterInfo 接口定义存在')
  for (const field of [
    'data_plane_redis_url',
    'data_plane_redis_source',
    'warnings',
  ]) {
    assert.ok(
      block[0].includes(field),
      `ClusterInfo 缺字段 ${field}（后端六面响应已带，前端类型必须采）`,
    )
  }
})

check('DataPlaneNotice 组件消费三字段；来源标签与判据在 services/clusterDataplane.ts', () => {
  const componentPath = join(here, '../src/components/DataPlaneNotice.tsx')
  assert.ok(existsSync(componentPath), 'components/DataPlaneNotice.tsx 不存在')
  const component = read('../src/components/DataPlaneNotice.tsx')
  for (const token of [
    'data_plane_redis_url',
    'data_plane_redis_source',
    'warnings',
  ]) {
    assert.ok(component.includes(token), `DataPlaneNotice 未消费 ${token}`)
  }
  const helpersPath = join(here, '../src/services/clusterDataplane.ts')
  assert.ok(existsSync(helpersPath), 'services/clusterDataplane.ts 不存在')
  const helpers = read('../src/services/clusterDataplane.ts')
  for (const token of [
    'config_file',
    'console_env',
    'registry_metadata',
    'data_plane_redis_url',
    'data_plane_redis_source',
  ]) {
    assert.ok(helpers.includes(token), `clusterDataplane 判据未覆盖 ${token}`)
  }
})

check('集群管理页挂载 DataPlaneNotice（activeCluster 读 /clusters/active）', () => {
  const source = read('../src/pages/Cluster.tsx')
  assert.match(source, /DataPlaneNotice/, '集群页未引入 DataPlaneNotice')
  assert.match(
    source,
    /<DataPlaneNotice\s+cluster=\{activeCluster\}/,
    '集群页未用 activeCluster 挂载警示条',
  )
})

check('切换器行级错位标记 + switch 后刷新 activeCluster', () => {
  const source = read('../src/components/ClusterSwitcher.tsx')
  assert.match(
    source,
    /dataPlaneMismatched\(/,
    '切换器未做行级错位标记（元数据≠生效在列表不可见）',
  )
  assert.match(
    source,
    /queryKey: \['activeCluster'\]/,
    'switch 成功后未失效 activeCluster 查询（警示条会滞留旧集群）',
  )
})

console.log('')
if (failures.length) {
  console.error(`${failures.length}/${checks} 项断言失败：`)
  for (const failure of failures) console.error(`  - ${failure}`)
  process.exit(1)
}
console.log(`${checks} 项断言全部通过`)
