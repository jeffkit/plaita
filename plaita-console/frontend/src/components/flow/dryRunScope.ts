/**
 * 试跑请求的**执行范围**（plaita#67）。
 *
 * 缺陷根因：两个入口都写成「先 `setOnlyNode(...)`，再**同步** `mut.mutate()`」。
 * `setState` 只是排队一次更新，React 在事件处理器返回后才提交渲染；而 `mutationFn`
 * 是在**渲染期**创建的闭包，读到的 `onlyNode` 永远是**上一次渲染**的值。于是：
 *
 *   - 初始 `onlyNode === null` 时点节点行的 ▶ → 请求 `onlyNode=undefined` = **整条流程真跑**
 *     （后端 `if not pinned and not only_node: return data` 不做任何 mock 改写）；
 *   - 已处于单节点态时点主按钮「完整试跑」→ 请求仍带旧节点 id = **只跑单节点**。
 *
 * 两种错位都不报错、界面上也不体现，用户无法从「我点了哪个按钮」自证。
 *
 * 修法（本模块承载的约定）：**范围由入口显式产出、作为 `mutate` 变量传入**，
 * `mutationFn` 只读入参，不再经过组件 state 与渲染提交的时序。`onlyNode` state
 * 仅保留展示职责（按钮文案 / 「取消单节点」）。
 */

/** 本次试跑的执行范围：节点 id = 仅真实执行该节点（其余 mock）；`null` = 整条流程。 */
export interface DryRunScope {
  onlyNode: string | null
}

/**
 * 请求体字段：`null`（整跑）→ `undefined`（不带该键），与后端 `onlyNode` 缺省语义一致。
 *
 * 缺省即整跑是后端既有约定（CLI/API 直接调用方合理），但在 console 这条链路上，
 * 「范围缺失」只可能来自**代码写错**（又写出裸 `mut.mutate()`），绝不能静默退化成
 * 真实整跑——故此处 fail-closed 抛错，让误用在界面上立刻可见。
 * 空串同理拒绝：后端以 `if not only_node` 判定整跑，`onlyNode: ''` 会被当成整跑。
 */
export function onlyNodeParam(scope: DryRunScope | undefined): string | undefined {
  if (scope == null || (scope.onlyNode !== null && !scope.onlyNode)) {
    throw new Error(
      'dry-run 执行范围缺失：必须显式传入 mut.mutate({ onlyNode })' +
        '（节点 id = 仅运行该节点，null = 完整试跑；plaita#67）'
    )
  }
  return scope.onlyNode ?? undefined
}
