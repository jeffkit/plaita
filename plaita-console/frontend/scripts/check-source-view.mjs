#!/usr/bin/env node
/**
 * #117 行为断言（无需前端测试框架：node ≥22.18 直接跑 .ts 类型擦除）。
 *
 * 覆盖 `GET /flows/{id}/versions/{ver}/source` 的全部结局 × 前端渲染状态，
 * 以及「反编译横幅 vs 说明段落」的文案一致性：
 *
 *   pnpm check:source-view
 *   # 等价：node plaita-console/frontend/scripts/check-source-view.mjs
 *
 * 断言对象是组件真正使用的纯函数（src/components/flow/sourceViewState.ts）
 * 加少量静态接线断言（面板 / 页面确实消费了这些分支）。任一断言失败 → 退出码 1。
 */
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import {
  EMPTY_SOURCE_VIEW,
  embeddedSource,
  resolveSourceFailure,
  resolveSourceResponse,
  sourceNoteText,
} from '../src/components/flow/sourceViewState.ts'

const here = dirname(fileURLToPath(import.meta.url))
const read = (p) => readFileSync(join(here, p), 'utf8')

const panelSrc = read('../src/components/flow/SourceViewPanel.tsx')
const editorSrc = read('../src/pages/FlowEditor.tsx')
const apiSrc = read('../src/services/api.ts')
const transportSrc = read('../src/services/transport.ts')

let checks = 0
const failures = []
function check(name, fn) {
  checks += 1
  try {
    fn()
    console.log(`  ✓ ${name}`)
  } catch (e) {
    failures.push(`${name}: ${e.message}`)
    console.log(`  ✗ ${name}: ${e.message}`)
  }
}

const AUTHORITATIVE_NOTE = '权威 @flow 源码（节点 id 即变量名 / 条件语义）；行号与画布节点 source_line 对应。'
const YAML_NOTE = 'YAML：配置文件首选，支持注释。保存到文件用 .yaml / .yml 后缀，Flow.from_file 可直接加载。'
const JSON_NOTE = 'JSON：与可视化编排工具互通的格式，Flow.from_string 可直接加载。'
const CLAIM = '行号与画布节点 source_line 对应'

console.log('[A] 端点响应 → 前端状态（resolveSourceResponse）')
check('内嵌源码判定与后端 .strip() 一致：纯空白 metadata.source 按「没有」处理', () => {
  assert.equal(embeddedSource({ source: '   ' }), '')
  assert.equal(embeddedSource({ source: '\n\t ' }), '')
  assert.equal(embeddedSource({ source: '' }), '')
  assert.equal(embeddedSource({ source: 42 }), '')
  assert.equal(embeddedSource({}), '')
  assert.equal(embeddedSource(undefined), '')
  assert.equal(embeddedSource(null), '')
})
check('内嵌源码判定：真源码逐字节返回（不 trim 正文，保留权威字节）', () => {
  const src = '\n\ndef x(INPUT):\n    return INPUT.name\n'
  assert.equal(embeddedSource({ source: src }), src)
})
check('authoritative + 正文 → 原样展示、无降级原因', () => {
  const s = resolveSourceResponse({ source_kind: 'authoritative', source: 'def x(): pass\n' })
  assert.deepEqual(s, { source: 'def x(): pass\n', sourceKind: 'authoritative', unavailableReason: null, warning: null })
})
check('decompiled + 正文 → 展示且标注来源', () => {
  const s = resolveSourceResponse({ source_kind: 'decompiled', source: 'def x(): pass\n' })
  assert.equal(s.sourceKind, 'decompiled')
  assert.equal(s.source, 'def x(): pass\n')
  assert.equal(s.unavailableReason, null)
})
check("unavailable + emit_source 失败 reason → reason 原样落到可渲染状态", () => {
  const reason = "emit_source 失败: 节点类型 'switch' 不可表达为 @flow 源码"
  const s = resolveSourceResponse({ source_kind: 'unavailable', source: null, reason })
  assert.equal(s.source, null)
  assert.equal(s.sourceKind, null)
  assert.equal(s.unavailableReason, reason) // ← 修复前该字段在前端零消费
})
check('unavailable + 定义缺失或不可解析 → reason 原样呈现', () => {
  const s = resolveSourceResponse({ source_kind: 'unavailable', source: null, reason: '定义缺失或不可解析' })
  assert.equal(s.unavailableReason, '定义缺失或不可解析')
})
check('unavailable 但后端未给 reason → 兜底文案 + 告警（不静默）', () => {
  const s = resolveSourceResponse({ source_kind: 'unavailable', source: null })
  assert.ok(s.unavailableReason && s.unavailableReason.length > 0)
  assert.ok(s.warning, '缺 reason 必须留可观测告警')
})
check('decompiled 却给空正文 / null 正文 → 不出现空白 @flow 页签，如实报不可用', () => {
  for (const source of ['', '   ', null]) {
    const s = resolveSourceResponse({ source_kind: 'decompiled', source })
    assert.equal(s.source, null)
    assert.equal(s.sourceKind, null)
    assert.ok(s.unavailableReason, `source=${JSON.stringify(source)} 必须有原因`)
    assert.ok(s.warning, `source=${JSON.stringify(source)} 必须有告警`)
  }
})
check('未识别的 source_kind + 正文 → 展示但 sourceKind=null（不认领权威性）+ 告警', () => {
  const s = resolveSourceResponse({ source_kind: 'future_kind', source: 'def y(): pass\n' })
  assert.equal(s.source, 'def y(): pass\n')
  assert.equal(s.sourceKind, null)
  assert.equal(s.unavailableReason, null)
  assert.ok(s.warning)
})
check('未识别的 source_kind + 无正文 → 如实报「无法识别」', () => {
  const s = resolveSourceResponse({ source_kind: 'future_kind', source: null, reason: '新原因' })
  assert.equal(s.source, null)
  assert.ok(s.unavailableReason.includes('future_kind'))
  assert.ok(s.unavailableReason.includes('新原因'))
  assert.ok(s.warning)
})
check('缺 source_kind（{} / null / undefined，如畸形 body）→ 如实报「缺少 source_kind」', () => {
  for (const res of [{}, null, undefined, { source: 42 }, { source_kind: 7 }]) {
    const s = resolveSourceResponse(res)
    assert.equal(s.source, null)
    assert.ok(s.unavailableReason.includes('缺少 source_kind'), JSON.stringify(res))
    assert.ok(s.warning)
  }
})
check('source_kind 不是字符串 / 正文不是字符串：不炸、按缺失处理', () => {
  assert.equal(resolveSourceResponse({ source_kind: [], source: {} }).source, null)
})
check('未请求 / 已清空状态：无源码、无原因、无告警', () => {
  assert.deepEqual(EMPTY_SOURCE_VIEW, { source: null, sourceKind: null, unavailableReason: null, warning: null })
})
check('不变量：无正文 ⇒ 必有 unavailableReason；有正文 ⇒ sourceKind ∈ {authoritative,decompiled,null} 且无原因', () => {
  const bodies = [
    { source_kind: 'authoritative', source: 'a' },
    { source_kind: 'decompiled', source: 'a' },
    { source_kind: 'unavailable', source: null, reason: 'r' },
    { source_kind: 'unavailable', source: null },
    { source_kind: 'weird', source: 'a' },
    { source_kind: 'weird', source: null },
    {},
    undefined,
  ]
  for (const b of bodies) {
    const s = resolveSourceResponse(b)
    if (s.source === null) assert.ok(s.unavailableReason, `无正文必须有原因: ${JSON.stringify(b)}`)
    else {
      assert.ok(['authoritative', 'decompiled', null].includes(s.sourceKind))
      assert.equal(s.unavailableReason, null)
    }
  }
})

console.log('[B] 请求失败分类（resolveSourceFailure）：404 ≠ 5xx ≠ 网络 ≠ 响应不可解析')
const withStatus = (status, message) => Object.assign(new Error(message), { status })
check('HTTP 404（版本不存在 / 后端版本旧 / 路由缺失）→ 专门的 404 说明', () => {
  const s = resolveSourceFailure(withStatus(404, '版本不存在: echo@9.9.9'))
  assert.equal(s.source, null)
  assert.ok(s.unavailableReason.includes('404'))
  assert.ok(!s.unavailableReason.includes('5xx'))
  assert.ok(s.warning)
})
check('HTTP 5xx → 与 404 可区分的服务端错误说明', () => {
  for (const status of [500, 502, 503]) {
    const s = resolveSourceFailure(withStatus(status, 'Internal Server Error'))
    assert.ok(s.unavailableReason.includes(String(status)), `缺状态码 ${status}`)
    assert.ok(s.unavailableReason.includes('服务端错误'))
    assert.ok(!s.unavailableReason.includes('404'))
  }
})
check('其它 4xx（如 403）→ 带状态码与后端 message', () => {
  const s = resolveSourceFailure(withStatus(403, '无权限'))
  assert.ok(s.unavailableReason.includes('403'))
  assert.ok(s.unavailableReason.includes('无权限'))
})
check('HTTP 200 但正文非 JSON（SyntaxError）→ 「无法解析」，不冒充网络错误', () => {
  const s = resolveSourceFailure(new SyntaxError('Unexpected token < in JSON'))
  assert.ok(s.unavailableReason.includes('无法解析'))
  assert.ok(s.warning)
})
check('网络不可达 / 超时（无 status）→ 网络说明，且与 404/5xx 不同形', () => {
  const s = resolveSourceFailure(new TypeError('Failed to fetch'))
  assert.ok(s.unavailableReason.includes('网络'))
  assert.ok(s.unavailableReason.includes('Failed to fetch'))
  assert.ok(s.warning)
})
check('非 Error 抛出物（如字符串）→ 不炸且有原因', () => {
  const s = resolveSourceFailure('boom')
  assert.ok(s.unavailableReason.includes('boom'))
})
check('失败状态永不给出源码（不可能把错误渲染成 @flow 页签）', () => {
  for (const e of [withStatus(404, 'x'), withStatus(500, 'x'), new TypeError('x'), new SyntaxError('x'), 'x', null, undefined]) {
    assert.equal(resolveSourceFailure(e).source, null)
  }
})
check('失败状态之间两两原因不同（用户可据此分辨）', () => {
  const reasons = [
    resolveSourceFailure(withStatus(404, 'x')).unavailableReason,
    resolveSourceFailure(withStatus(500, 'x')).unavailableReason,
    resolveSourceFailure(new TypeError('Failed to fetch')).unavailableReason,
    resolveSourceFailure(new SyntaxError('bad json')).unavailableReason,
  ]
  assert.equal(new Set(reasons).size, reasons.length, reasons.join(' | '))
})

console.log('[C] 文案一致性（sourceNoteText）：横幅与说明段落不得互相否定')
check('YAML / JSON 说明与 sourceKind 无关且逐字不变（无回归）', () => {
  for (const k of ['authoritative', 'decompiled', null]) {
    assert.equal(sourceNoteText('yaml', k), YAML_NOTE)
    assert.equal(sourceNoteText('json', k), JSON_NOTE)
  }
})
check('authoritative + @flow → 逐字保留原「行号与 source_line 对应」文案', () => {
  assert.equal(sourceNoteText('flow', 'authoritative'), AUTHORITATIVE_NOTE)
  assert.ok(sourceNoteText('flow', 'authoritative').includes(CLAIM))
})
check('decompiled + @flow → 不再声称「行号与画布节点 source_line 对应」', () => {
  const t = sourceNoteText('flow', 'decompiled')
  assert.ok(!t.includes(CLAIM), `decompiled 说明仍含矛盾句: ${t}`)
  assert.ok(t.includes('不对应任何仓内文件'))
  assert.ok(t.includes('反编译'))
})
check('来源未标注（sourceKind=null）+ @flow → 不下任何行号对应结论', () => {
  const t = sourceNoteText('flow', null)
  assert.ok(!t.includes(CLAIM))
  assert.ok(t.includes('未知'))
})
check('不变量：@flow 说明声称「行号与 source_line 对应」⇔ sourceKind === authoritative', () => {
  for (const k of ['authoritative', 'decompiled', null, undefined]) {
    const claims = sourceNoteText('flow', k).includes(CLAIM)
    assert.equal(claims, k === 'authoritative', `sourceKind=${k} 的对应性声明不对`)
  }
})
check('decompiled 说明与同屏横幅不矛盾（横幅说 id/行号不对应原文件）', () => {
  assert.ok(panelSrc.includes('id/行号不对应原文件'), '反编译横幅丢失')
  assert.ok(!sourceNoteText('flow', 'decompiled').includes(CLAIM))
})

console.log('[D] 静态接线（组件 / 页面确实消费这些分支，而非只存在于纯函数里）')
check('SourceViewPanel 用 sourceNoteText(format, sourceKind) 选说明文案', () => {
  assert.match(panelSrc, /sourceNoteText\(format,\s*sourceKind\)/)
})
check('SourceViewPanel 不再内联那份只按 format 分支的说明文案', () => {
  assert.ok(!panelSrc.includes('权威 @flow 源码（节点 id 即变量名'), '说明文案仍是内联旧三元（只看 format）')
})
check('SourceViewPanel 渲染 unavailableReason（有消费者）', () => {
  assert.match(panelSrc, /unavailableReason/)
  assert.match(panelSrc, /unavailableReason\s*&&/)
})
check('FlowEditor 接线 unavailable 与失败两个分支（不再有吞掉的 .catch(() => {})）', () => {
  assert.match(editorSrc, /resolveSourceResponse\(/)
  assert.match(editorSrc, /resolveSourceFailure\(/)
  assert.match(editorSrc, /sourceUnavailableReason/)
  assert.ok(!/\.catch\(\(\)\s*=>\s*\{\s*\}\)/.test(editorSrc), 'FlowEditor 仍存在空 catch 吞错')
  assert.match(
    editorSrc,
    /getFlowVersionSource[\s\S]{0,600}?\.catch\(\(err\)/,
    'getFlowVersionSource 的失败未接线到分类处理',
  )
})
check('FlowEditor 把 reason 传给面板 + 在「源码」按钮上可发现', () => {
  assert.match(editorSrc, /setSourceUnavailableReason\(state\.unavailableReason\)/)
  assert.match(editorSrc, /setFlowSource\(state\.source\)/)
  assert.match(editorSrc, /unavailableReason=\{sourceUnavailableReason\}/)
  assert.match(editorSrc, /sourceUnavailableReason\s*\?/)
})
check('api.request 把 HTTP status 挂到错误对象上（失败分类的前提）', () => {
  // S17-03 后 request 统一抛 ApiError：状态码由 buildHttpError 写入
  assert.match(apiSrc, /buildHttpError\(response\.status/)
  assert.match(transportSrc, /new ApiError\(message, \{[\s\S]*?status,/)
})
check('authoritative 主链路未被改写：metadata.source 仍直接作为权威源码', () => {
  assert.match(editorSrc, /embeddedSource\(defMeta\)/)
  assert.match(editorSrc, /setSourceKind\(src \? 'authoritative' : null\)/)
  assert.match(
    editorSrc,
    /useFlowEditor\.setState\(\{ hasFlowSource: !!src, sourceKind: src \? 'authoritative' : null, sourceLineRequest: null \}\)/,
  )
})
check('节点行号跳转门禁仍只认 authoritative（unavailable 不得放开跳转）', () => {
  const drawer = read('../src/components/flow/NodeConfigDrawer.tsx')
  assert.match(drawer, /hasFlowSource && sourceKind === 'authoritative' && nodeSourceLine != null/)
})

console.log('')
if (failures.length) {
  console.error(`✗ check-source-view: ${failures.length}/${checks} 条断言失败`)
  for (const f of failures) console.error(`  - ${f}`)
  process.exit(1)
}
console.log(`✓ check-source-view: ${checks} 条断言全部通过`)
