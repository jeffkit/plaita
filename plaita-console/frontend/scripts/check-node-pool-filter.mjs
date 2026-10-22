#!/usr/bin/env node
/**
 * 节点面板按 runtime 池过滤（plaita#300 需求4，第七十五轮 B2）。
 *
 *   node scripts/check-node-pool-filter.mjs
 *   # 等价：pnpm check:node-pool-filter
 *
 * #300 需求4：graph 编辑器节点面板此前展示全量节点——go worker 跑不了的
 * Python 家族节点也能被拖进画布，发布/启动才被能力门 422。收口后：
 * go runtime 下面板按池内能力清单（services 读面 capability_manifest 投影）
 * **交集**灰置不支持的节点；池内无任何清单（旧 build / 无 go worker）兜底
 * 全量 + 提示（与后端能力门 fail-open + 具名 WARNING 同语义）；python 池
 * 全量（console 注册表即 python 权威面，不过滤）。
 *
 * [A] 行为断言：直接 import `src/services/nodePool.ts`，用真实清单形状断言
 *     交集/引擎过滤/坏形状跳过/缺清单 null 兜底与不支持集计算；
 * [B] 静态接线断言：api.ts 类型（ServiceInfo.capability_manifest /
 *     VersionView.runtime / saveVersion runtime）、NodePalette 消费与灰置/
 *     提示、FlowEditor 的 runtime 状态-选择器-保存继承-面板挂载确实存在
 *     （被断言形状即出货形状）。
 * 任一断言失败 → 退出码 1。
 */
import assert from 'node:assert/strict'
import { readFileSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  GO_ENGINE,
  poolSupportedNodeTypes,
  unsupportedNodeTypes,
} from '../src/services/nodePool.ts'

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

const goManifest = (supported) => ({
  engine: 'plaita-go',
  ir_contract_version: '2026-10-22',
  supported_node_types: supported,
  unsupported_node_types: [],
  features: [],
})
const ALL_NODES = [
  { node_type: 'start' },
  { node_type: 'end' },
  { node_type: 'http' },
  { node_type: 'map' },
  { node_type: 'while' },
]

console.log('[A] nodePool.ts 行为（池清单 → supported 交集 / 不支持集）')
check('单清单投影：go 清单即池口径', () => {
  assert.deepEqual(
    poolSupportedNodeTypes([goManifest(['end', 'http', 'start'])], GO_ENGINE),
    ['end', 'http', 'start']
  )
})
check('混部取交集（多 go worker 最严口径，与后端 _pooled_supported 同语义）', () => {
  assert.deepEqual(
    poolSupportedNodeTypes(
      [goManifest(['start', 'end', 'http', 'loop']), goManifest(['start', 'end', 'switch'])],
      GO_ENGINE
    ),
    ['end', 'start']
  )
})
check('引擎过滤：python 清单不混入 go 池判定', () => {
  assert.deepEqual(
    poolSupportedNodeTypes(
      [goManifest(['start', 'end']), { engine: 'python', supported_node_types: ['start', 'map'] }],
      GO_ENGINE
    ),
    ['end', 'start']
  )
})
check('清单缺失/全坏形状 → null（兜底全量的信号）', () => {
  assert.equal(poolSupportedNodeTypes([], GO_ENGINE), null)
  assert.equal(poolSupportedNodeTypes([null, undefined], GO_ENGINE), null)
  assert.equal(poolSupportedNodeTypes([{ engine: 'plaita-go' }], GO_ENGINE), null)
  assert.equal(poolSupportedNodeTypes([goManifest([])], GO_ENGINE), null)
})
check('unsupportedNodeTypes：全量减池支持集；null → 空集（不过滤）', () => {
  assert.deepEqual(
    [...unsupportedNodeTypes(ALL_NODES, ['end', 'http', 'start'])].sort(),
    ['map', 'while']
  )
  assert.equal(unsupportedNodeTypes(ALL_NODES, null).size, 0)
})
check('GO_ENGINE 与后端 plaita.server.capability.ENGINE_GO 同值', () => {
  assert.equal(GO_ENGINE, 'plaita-go')
})

console.log('[B] 前端接线（静态形状 = 出货形状）')
check('api.ts：ServiceInfo.capability_manifest + VersionView/FlowVersionSummary.runtime', () => {
  const source = read('../src/services/api.ts')
  const serviceBlock = source.match(/export interface ServiceInfo \{[\s\S]*?\n\}/)
  assert.ok(serviceBlock, 'ServiceInfo 接口定义存在')
  assert.ok(
    serviceBlock[0].includes('capability_manifest'),
    'ServiceInfo 缺 capability_manifest（services 读面已投影，前端必须采）'
  )
  const versionBlock = source.match(/export interface VersionView \{[\s\S]*?\n\}/)
  assert.ok(versionBlock, 'VersionView 接口定义存在')
  assert.ok(versionBlock[0].includes('runtime'), 'VersionView 缺 runtime')
  const summaryBlock = source.match(/export interface FlowVersionSummary \{[\s\S]*?\n\}/)
  assert.ok(summaryBlock && summaryBlock[0].includes('runtime'), 'FlowVersionSummary 缺 runtime')
  assert.match(source, /export type \{ CapabilityManifest \}/, 'CapabilityManifest 未再导出')
})
check('api.ts：saveVersion 显式带 runtime（go 版本另存不再静默降级 python）', () => {
  const source = read('../src/services/api.ts')
  const saveBlock = source.match(/async saveVersion\([\s\S]*?\n  \},/)
  assert.ok(saveBlock, 'saveVersion 定义存在')
  assert.ok(saveBlock[0].includes('runtime?'), 'saveVersion payload 缺 runtime 参数')
})
check('NodePalette：runtime 入参 + 池清单交集 + 灰置（不隐藏）', () => {
  const source = read('../src/components/flow/NodePalette.tsx')
  assert.match(source, /runtime = 'python'/, 'NodePalette 无 runtime 入参（默认 python）')
  assert.match(source, /poolSupportedNodeTypes/, '未用池交集判定')
  assert.match(source, /GO_ENGINE/, '未按 go 引擎过滤清单')
  assert.match(source, /capability_manifest/, '未消费 services 读面的清单投影')
  assert.match(source, /unsupportedNodeTypes/, '未计算不支持集')
  assert.match(source, /opacity-40 cursor-not-allowed/, '不支持节点未灰置')
  assert.match(source, /draggable=\{!blocked\}/, '不支持节点仍可拖拽（灰置必须禁拖）')
  assert.match(source, /仅 Python 池可执行/, '灰置节点缺 title 说明')
  assert.match(source, /仅Py/, '灰置节点缺角标')
})
check('NodePalette：缺清单兜底全量 + 提示（降级可观测，与门 fail-open 同语义）', () => {
  const source = read('../src/components/flow/NodePalette.tsx')
  assert.match(
    source,
    /go 池无能力清单/,
    '缺清单提示缺失——兜底全量必须可见，不得静默'
  )
  assert.match(source, /go 池：.*不支持/, '缺 go 池灰置计数提示')
})
check('FlowEditor：runtime 状态随版本行 + 池选择器 + 保存继承 + 面板挂载', () => {
  const source = read('../src/pages/FlowEditor.tsx')
  assert.match(
    source,
    /setRuntime\(versionQuery\.data\.runtime === 'go' \? 'go' : 'python'\)/,
    'runtime 未随版本行同步'
  )
  assert.match(source, /value=\{runtime\}/, '编辑器头部无 runtime 选择器')
  assert.match(source, /setRuntime\(e\.target\.value as 'go' \| 'python'\)/, '选择器未回写状态')
  assert.match(source, /<NodePalette runtime=\{runtime\} \/>/, '节点面板未接 runtime')
  assert.match(
    source,
    /setRuntime\(res\.runtime === 'go' \? 'go' : 'python'\)/,
    '保存成功后未按服务端响应校正 runtime'
  )
  const saveCalls = source.match(/api\.saveVersion\([\s\S]*?\}\)/g) || []
  assert.ok(
    saveCalls.length >= 2 && saveCalls.every((call) => call.includes('runtime')),
    'saveVersion 调用未全部带 runtime（保存/发布继承断裂）'
  )
})
check('负控脚本已入 package.json scripts（门禁可达）', () => {
  const pkg = JSON.parse(read('../package.json'))
  assert.equal(
    pkg.scripts['check:node-pool-filter'],
    'node scripts/check-node-pool-filter.mjs'
  )
})

console.log('')
if (failures.length) {
  console.error(`${failures.length}/${checks} 项断言失败：`)
  for (const failure of failures) console.error(`  - ${failure}`)
  process.exit(1)
}
console.log(`${checks} 项断言全部通过`)
