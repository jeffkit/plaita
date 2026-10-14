"""plaita#141：子执行继承 ``expose_env``——两条子执行创建路径必须一致。

``#106`` 让 ``FlowExecution`` 的子执行（``get_child_execution``，被 ``child``
内联子逻辑 / ``parallel`` 分支 / ``map``·``filter``·``find``·``reduce`` 迭代体
共用）继承六个 ``express_*`` 命名空间参数；引擎自己的姊妹路径
``ExecutionContext.child()`` 一直传**七**项——多一个 ``expose_env``。两条路径
因此可以分叉：父级自定义 ``express_environment_variable`` 且声明 ``expose_env``
时，经 ``child()`` 建的子上下文带 allowlist、经 ``get_child_execution()`` 建的
没有。

本文件钉住的口径（与 ``MIGRATION.md`` / ``docs-site/docs/guide/expressions.md``
中的 contract 一致）：**子执行继承父级 ``$ENV`` allowlist**，除非被执行 flow
自己声明了非空 ``expose_env``（此时以声明为准）。父级从未声明 expose_env 的
流程（allowlist 为空）零行为变更。

为什么判别器要这么绕（``$PARENT`` 回落掩蔽）：单纯"构造函数多传一项"在端到端
不可观测——``setup_flow`` 会按被执行 flow 的声明覆盖 ``expose_env``，而读单个
变量时引擎还有 ``$PARENT`` 回落兜底，正好把「子 ``$ENV`` 是空的」这件事掩掉
（子读 ``$VARS.X`` 拿不到值时会去父上下文再求值一次）。所以这里的判别器让父级
在**自己的 ``$ENV`` 快照拍下之后**才导出环境变量（父流程节点 export，然后子
流程才 setup）：父级快照里没有该键 ⇒ ``$PARENT`` 回落给不出值 ⇒ 子执行只能读
自己 ``$ENV`` 里的那份。把 ``expose_env`` 从继承集里去掉，子 ``$ENV`` 回落到
空 dict，子读值变 ``None``，用例当场变红（负向对照见交付证据）。
"""

import json
import os
import unittest
from typing import ClassVar

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.node import End, InlineFlow, Node, Start
from plaita.node.concurrent import Parallel, ParallelBranch

PARENT_VAR = "PLAITA141_PARENT_VAR"
CHILD_VAR = "PLAITA141_CHILD_VAR"

# ``ExecutionContext.child()`` 与 ``FlowExecution.get_child_execution()`` 共同
# 需要复制的整份配置（六个 express_* + expose_env）。#141 之前第二条路径少最后
# 一项，本元组就是"整张表"的机器可读版本。
INHERITED_FIELDS = (
    "express_prefix", "express_input_name", "express_parent_name",
    "express_node_name", "express_global_name", "express_environment_variable",
    "expose_env",
)

CUSTOM_EXPRESS = {
    "express_prefix": "#",
    "express_input_name": "PARAMS",
    "express_parent_name": "UP",
    "express_node_name": "STEPS",
    "express_global_name": "GLOB",
    "express_environment_variable": "VARS",
}


class _ExportEnvNode(Node):
    """父流程里的"导出运行时环境变量"节点（判别器的时序开关）。

    放在父流程 ``Start`` 之后、子执行之前：父上下文的 ``$ENV`` 快照在
    ``setup_flow`` 时已经拍完（此时该键还不存在），子执行 setup 时它才存在——
    这正是"继承 vs 回落"可以被区分开的窗口。
    """

    node_type: ClassVar[str] = "export_env_issue141"
    node_name: ClassVar[str] = "导出环境变量（#141 判别器）"

    var: str = ""
    value: str = ""

    def run(self, execution):
        os.environ[self.var] = self.value
        return self.value


def _child_flow_json(var: str, output: str = None, expose_env=None, env_name: str = "VARS") -> dict:
    """子流程：``End.output`` 读自定义名的 ``$<env_name>.<var>``。

    ``env_name`` 默认 ``VARS``（#141 的判别器场景：父级自定义 ENV 名）；边界用例
    传默认名 ``ENV``。
    """
    flow = {
        "flow_id": "sub",
        "nodes": [
            {"id": "s", "type": "start", "next": "e"},
            {
                "id": "e", "type": "end", "resultType": "success",
                "output": output if output is not None else f"${env_name}.{var}",
            },
        ],
    }
    if expose_env is not None:
        flow["exposeEnv"] = expose_env
    return json.loads(json.dumps(flow))


class TestChildExecutionInheritsExposeEnv(unittest.TestCase):
    def setUp(self):
        for var in (PARENT_VAR, CHILD_VAR):
            os.environ.pop(var, None)
            self.addCleanup(os.environ.pop, var, None)

    # -- 1. 整张表：两条子执行创建路径逐项一致 -------------------------------

    def test_both_child_creation_paths_agree_on_whole_field_table(self):
        """``get_child_execution()`` 与 ``ExecutionContext.child()`` 的复制项一致。

        #141 前 ``expose_env`` 只在后者被复制——本用例是"整张表"的守卫：任何一项
        被漏掉都会在这里红（不只 expose_env）。
        """
        parent = FlowExecution()
        parent._ctx.expose_env = [PARENT_VAR]  # 生产上由 flow.expose_env 经 setup_flow 落入
        for name, value in CUSTOM_EXPRESS.items():
            setattr(parent, name, value)

        via_execution = parent.get_child_execution()
        via_context = parent._ctx.child()

        # FlowExecution 类体顶到 SC-003 硬上限，故 allowlist 没有公开只读属性，
        # 权威副本在 ``_ctx`` 上（``_inherited_express`` 也从这里取）。
        self.assertEqual(parent._ctx.expose_env, [PARENT_VAR])
        for name in INHERITED_FIELDS:
            self.assertEqual(
                getattr(via_execution._ctx, name), getattr(parent._ctx, name),
                f"get_child_execution() 必须继承 {name}",
            )
            self.assertEqual(
                getattr(via_context, name), getattr(parent._ctx, name),
                f"ExecutionContext.child() 必须继承 {name}",
            )

    # -- 2. 端到端判别器：child 内联子逻辑 -----------------------------------

    def test_child_reads_env_under_custom_namespace_e2e(self):
        """父级自定义 ENV 名 + allowlist：子执行读 ``$VARS.<var>`` 须命中父级白名单。

        判别器构造：``Start`` → 导出节点（此刻才 ``os.environ[VAR] = ...``）→
        内联子流程（读 ``$VARS.<var>``）。父级 ``$VARS`` 快照为空，``$PARENT``
        回落给不出值；子执行的值只能来自它自己继承来的 allowlist。

        RED 条件：``_inherited_express`` 丢掉 ``expose_env``（子 ``$VARS`` 回落
        空 dict）或 ``setup_flow`` 丢掉继承回退（同上）。
        """
        main = Flow(
            flow_id="main-child-env",
            expose_env=[PARENT_VAR],
            nodes=[
                Start(id="s", next="export"),
                _ExportEnvNode(id="export", var=PARENT_VAR, value="inherited-child", next="c"),
                InlineFlow(id="c", child_flow=_child_flow_json(PARENT_VAR), next="e"),
                End(id="e", output="$NODE.c"),
            ],
        )
        result = FlowExecution.run(
            main, params={}, express_environment_variable="VARS",
        )
        self.assertEqual(os.environ[PARENT_VAR], "inherited-child", "导出节点应先于子执行跑过")
        self.assertEqual(
            result, "inherited-child",
            "子执行的 $VARS.<var> 必须来自继承的父级 allowlist，而不是默认空 $ENV",
        )

    # -- 3. 端到端判别器：parallel 分支（另一条共用漏斗的调用方） -------------

    def test_parallel_branch_reads_env_under_custom_namespace_e2e(self):
        """同型判别器走 ``parallel`` 分支（与 child 共用 ``get_child_execution``）。"""
        main = Flow(
            flow_id="main-parallel-env",
            expose_env=[PARENT_VAR],
            nodes=[
                Start(id="s", next="export"),
                _ExportEnvNode(id="export", var=PARENT_VAR, value="inherited-branch", next="p"),
                Parallel(
                    id="p",
                    branches=[ParallelBranch(name="bg", flow=_child_flow_json(PARENT_VAR))],
                    join_branches=["bg"],
                    next="e",
                ),
                End(id="e", output="$NODE.p.bg"),
            ],
        )
        result = FlowExecution.run(
            main, params={}, express_environment_variable="VARS",
        )
        self.assertEqual(result, "inherited-branch")

    # -- 4. 边界：父级从未声明 expose_env ⇒ 零行为变更 -----------------------

    def test_default_env_flows_untouched_when_parent_never_sets_expose_env(self):
        """父级 allowlist 为空时子执行 ``$ENV`` 仍为空——即便进程里确实有该变量。

        这是 #141 的边界条款：默认 ``$ENV`` 用户（父级从不声明 expose_env）不受
        本修复影响。导出节点先跑（变量在子 setup 前已存在于 ``os.environ``），
        子执行依然读不到任何东西：整份 ``$ENV`` 读出 ``{}``、``$ENV.<var>`` 读出
        ``None``（两个子流程分别钉这两条）。
        """
        main = Flow(
            flow_id="main-no-allowlist",
            nodes=[
                Start(id="s", next="export"),
                _ExportEnvNode(id="export", var=PARENT_VAR, value="must-not-leak", next="c1"),
                InlineFlow(
                    id="c1",
                    child_flow=_child_flow_json(PARENT_VAR, output="$ENV", env_name="ENV"),
                    next="c2",
                ),
                InlineFlow(
                    id="c2",
                    child_flow=_child_flow_json(PARENT_VAR, env_name="ENV"),
                    next="e",
                ),
                End(id="e", output={"whole": "$NODE.c1", "single": "$NODE.c2"}),
            ],
        )
        result = FlowExecution.run(main, params={})
        self.assertEqual(os.environ[PARENT_VAR], "must-not-leak")
        self.assertEqual(result, {"whole": {}, "single": None},
                         "父级未声明 allowlist 时子 $ENV 必须保持为空")

    # -- 5. 声明优先：子流程自己的 allowlist 不被父级覆盖 ---------------------

    def test_child_flow_own_allowlist_wins_over_parent(self):
        """子流程显式声明 exposeEnv 时以声明为准（继承只填"未声明"的空档）。

        判据用整份 ``$VARS`` 读：整根存在（dict）时不再触发 ``$PARENT`` 回落，
        因此能同时证明"子白名单里的键在"且"父白名单里的键不在"。
        """
        main = Flow(
            flow_id="main-declared-wins",
            expose_env=[PARENT_VAR],
            nodes=[
                Start(id="s", next="export"),
                _ExportEnvNode(id="export", var=PARENT_VAR, value="parent-value", next="export-child"),
                _ExportEnvNode(id="export-child", var=CHILD_VAR, value="child-value", next="c"),
                InlineFlow(
                    id="c",
                    child_flow=_child_flow_json(CHILD_VAR, output="$VARS", expose_env=[CHILD_VAR]),
                    next="e",
                ),
                End(id="e", output="$NODE.c"),
            ],
        )
        result = FlowExecution.run(
            main, params={}, express_environment_variable="VARS",
        )
        self.assertEqual(result, {CHILD_VAR: "child-value"})


if __name__ == "__main__":
    unittest.main()
