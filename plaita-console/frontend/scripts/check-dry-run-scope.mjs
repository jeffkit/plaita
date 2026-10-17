#!/usr/bin/env node
/**
 * plaita#67 行为断言（无需前端测试框架：node ≥22.18 直接跑 .ts 类型擦除）。
 *
 * 缺陷：试跑面板两个入口都是「先 setOnlyNode(...)，再**同步** mut.mutate()」，
 * 而 mutationFn 闭包取自**上一次渲染**——请求携带的执行范围与用户点击的按钮错开一拍：
 *   首次点节点行 ▶ → onlyNode=undefined（= **整条流程真跑**，下游不 mock）
 *   已是单节点态时点主按钮「完整试跑」→ 仍是旧节点 id（= 只跑单节点）
 *
 * 本脚本钉住三件事：
 *   [A] 真实纯函数 src/components/flow/dryRunScope.ts 的行为（直接 import 执行）；
 *   [B] DryRunPanel.tsx 的**接线**：范围只由 mutate 变量传入，mutationFn 不再读组件 state，
 *       两个入口分别显式传 {onlyNode: id} / {onlyNode: null}，且全文件无裸 mut.mutate()；
 *   [C] 时序语义模型：按 React「事件处理器内 setState 排队、处理器返回后才提交渲染」
 *       的语义跑**旧形状 vs 新形状**，复现 issue 报告的两个错位并证明新形状对齐。
 *
 *   node plaita-console/frontend/scripts/check-dry-run-scope.mjs
 *   # 等价：pnpm --dir plaita-console/frontend check:dry-run-scope
 *
 * [C] 是**语义模型**（按 React 已文档化的批处理/闭包语义重演），不是浏览器/React 运行时
 * 实跑——本仓前端无测试基建（package.json 无 test script、devDependencies 无 vitest/jest），
 * 故组件级实跑不在本脚本能力内；[B] 保证被断言形状就是出货形状。任一断言失败 → 退出码 1。
 */
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import { onlyNodeParam } from '../src/components/flow/dryRunScope.ts'

const here = dirname(fileURLToPath(import.meta.url))
const panelSrc = readFileSync(join(here, '../src/components/flow/DryRunPanel.tsx'), 'utf8')

let checks = 0
const failures = []
/** assert 失败会把整个被断言源码回显出来——截断，保持输出可读。 */
function clip(s, n = 200) {
  const t = String(s)
  return t.length > n ? `${t.slice(0, n)}…(+${t.length - n} 字符)` : t
}
function check(name, fn) {
  checks += 1
  try {
    fn()
    console.log(`  ✓ ${name}`)
  } catch (e) {
    const msg = clip(e.message)
    failures.push(`${name}: ${msg}`)
    console.log(`  ✗ ${name}: ${msg}`)
  }
}

/** 从 marker 之后第一个 `{` 起做花括号配平，取回整段块（对格式不敏感）。 */
function extractBlock(src, marker) {
  const at = src.indexOf(marker)
  assert.ok(at >= 0, `源码里找不到标记：${marker}`)
  const open = src.indexOf('{', at)
  assert.ok(open >= 0, `标记后找不到 {：${marker}`)
  let depth = 0
  for (let i = open; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1
    else if (src[i] === '}') {
      depth -= 1
      if (depth === 0) return src.slice(open + 1, i)
    }
  }
  throw new Error(`花括号未配平：${marker}`)
}

const mutationFnBody = extractBlock(panelSrc, 'mutationFn:')
const runOnlyBody = extractBlock(panelSrc, 'const runOnly = ')
const runFullBody = extractBlock(panelSrc, 'const runFull = ')

console.log('[A] 真实纯函数（dryRunScope.ts）：范围 → 请求字段')
check('节点 id → onlyNode 原样透传（「仅运行此节点」真的带 id）', () => {
  assert.equal(onlyNodeParam({ onlyNode: 'n2' }), 'n2')
})
check('null → undefined（「完整试跑」真的不带该键）', () => {
  assert.equal(onlyNodeParam({ onlyNode: null }), undefined)
})
check('范围缺失（又写出裸 mut.mutate()）→ fail-closed 抛错，不静默退化成整跑', () => {
  for (const bad of [undefined, null, {}, { onlyNode: undefined }]) {
    assert.throws(
      () => onlyNodeParam(bad),
      /dry-run 执行范围缺失/,
      `scope=${JSON.stringify(bad)} 未被拒绝——会静默退化成真实整跑`,
    )
  }
})
check("空串 id 被拒（后端以 `if not only_node` 判定整跑，'' 会被当成整跑）", () => {
  assert.throws(() => onlyNodeParam({ onlyNode: '' }), /dry-run 执行范围缺失/)
})
check('不变量：返回 undefined ⇔ scope.onlyNode === null（其余必有 id）', () => {
  for (const scope of [{ onlyNode: 'a' }, { onlyNode: null }, { onlyNode: '0' }, { onlyNode: 'x y' }]) {
    const p = onlyNodeParam(scope)
    assert.equal(p === undefined, scope.onlyNode === null, JSON.stringify(scope))
  }
})

console.log('[B] 静态接线（DryRunPanel.tsx 就是出货形状）')
check('mutationFn 接收 DryRunScope 变量（范围不再从渲染期闭包取）', () => {
  assert.match(panelSrc, /mutationFn:\s*async\s*\(\s*scope\s*:\s*DryRunScope\s*\)/)
  assert.match(panelSrc, /import\s*\{[^}]*onlyNodeParam[^}]*\}\s*from\s*'\.\/dryRunScope'/)
})
check('mutationFn 内只有 onlyNodeParam(scope) 一处 onlyNode 引用（不再读组件 state）', () => {
  assert.match(mutationFnBody, /onlyNode:\s*onlyNodeParam\(scope\)/)
  // 剥掉 API 字段名 `onlyNode:` 与 onlyNodeParam 之后，不得再有 onlyNode 标识符
  const stripped = mutationFnBody
    .replace(/onlyNodeParam/g, '')
    .replace(/onlyNode\s*:/g, '')
  assert.ok(
    !/\bonlyNode\b/.test(stripped),
    'mutationFn 仍直接读组件 state 的 onlyNode（闭包时序缺陷会复现）',
  )
  assert.ok(
    !/onlyNode\s*\?\?\s*undefined/.test(panelSrc),
    "旧的 `onlyNode: onlyNode ?? undefined` 闭包读法仍在",
  )
})
check('「仅运行此节点」入口显式传本次节点 id', () => {
  assert.match(runOnlyBody, /mut\.mutate\(\s*\{\s*onlyNode:\s*id\s*\}\s*\)/)
})
check('「完整试跑 / 开始试跑」入口显式传 null（不带 onlyNode）', () => {
  assert.match(runFullBody, /mut\.mutate\(\s*\{\s*onlyNode:\s*null\s*\}\s*\)/)
})
check('全文件不再有裸 mut.mutate()（唯一的静默整跑入口）', () => {
  assert.ok(!/mut\.mutate\(\s*\)/.test(panelSrc), '仍有不带范围的 mut.mutate()')
  const calls = panelSrc.match(/mut\.mutate\(/g) ?? []
  assert.equal(calls.length, 2, `mut.mutate 调用点应为 2 处（runOnly/runFull），实际 ${calls.length}`)
})
check('setOnlyNode 仍在且只承担展示职责（按钮文案 / 取消单节点）', () => {
  assert.match(panelSrc, /setOnlyNode\(id\)/)
  assert.match(panelSrc, /setOnlyNode\(null\)/)
  assert.match(panelSrc, /onlyNode\s*\?\s*'完整试跑'\s*:\s*'开始试跑'/)
})
check('文案承诺与请求范围一致：▶ 的 tooltip 仍承诺「下游 mock 无副作用」', () => {
  assert.match(
    panelSrc,
    /title="仅运行此节点（上游取固定值，下游 mock 无副作用）"/,
    'tooltip 承诺被改动——必须与 onlyNode 请求字段同源核对',
  )
})

console.log('[C] 时序语义模型：旧形状错位 / 新形状对齐（非浏览器实跑）')
// 最小 React 语义：事件处理器内 setState 只排队；渲染在处理器返回后才提交；
// mutationFn 在渲染期创建，闭包只能看到**本次渲染快照**。
function makeHarness(shape) {
  let state = { onlyNode: null }
  let mutationFn = null
  const sent = []
  const render = () => {
    mutationFn = shape.render(state, sent)
  }
  render()
  return {
    sent,
    setState: (v) => {
      state = { onlyNode: v } // 排队，本次渲染的闭包不受影响
    },
    commit: render, // 处理器返回后提交渲染
    mutate: (arg) => shape.invoke(mutationFn, arg, sent),
  }
}
const OLD_SHAPE = {
  // 旧：mutationFn 从渲染快照读 state.onlyNode；入口调用裸 mut.mutate()
  render: (state, sent) => () => sent.push(state.onlyNode ?? undefined),
  invoke: (fn) => fn(),
}
const NEW_SHAPE = {
  // 新：范围作为 mutate 变量传入
  render: (state, sent) => (scope) => sent.push(onlyNodeParam(scope)),
  invoke: (fn, arg) => fn(arg),
}
function journey(shape) {
  const h = makeHarness(shape)
  h.setState('n2') // 点节点行 ▶
  h.mutate(shape === NEW_SHAPE ? { onlyNode: 'n2' } : undefined)
  h.commit() // 事件处理器返回 → React 提交渲染
  h.setState(null) // 点主按钮「完整试跑」
  h.mutate(shape === NEW_SHAPE ? { onlyNode: null } : undefined)
  h.commit()
  return h.sent
}
check('旧形状精确复现 issue 报告的两个错位：undefined（整跑）→ 旧 id（只跑单节点）', () => {
  assert.deepEqual(journey(OLD_SHAPE), [undefined, 'n2'])
})
check('新形状两个入口都与点击一致：n2（单节点）→ undefined（整跑）', () => {
  assert.deepEqual(journey(NEW_SHAPE), ['n2', undefined])
})
check('对照组：首次点 ▶ 的请求范围，新旧形状必须不同（否则断言无鉴别力）', () => {
  assert.notDeepEqual(journey(OLD_SHAPE)[0], journey(NEW_SHAPE)[0])
  assert.equal(journey(NEW_SHAPE)[0], 'n2')
})

console.log('')
if (failures.length) {
  console.error(`✗ check-dry-run-scope: ${failures.length}/${checks} 条断言失败`)
  for (const f of failures) console.error(`  - ${f}`)
  process.exit(1)
}
console.log(`✓ check-dry-run-scope: ${checks} 条断言全部通过`)
