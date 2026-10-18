"""plaita#238：DSL 往返 conformance——全节点×全字段双向等价（CI 常驻门）。

口径（继承 test_issue146 注释）：规范化 IR 整树相等。采样与比较机制见
:mod:`tests.unit._dsl_roundtrip_samples`（harvest+补齐+三表示出口+diff）。

判读规则：
1. 每个类型×表示面的当前丢失必须 **⊆ 白名单**（恶化即红）；
2. 丢失总路径数 **钉死在白名单总量**（修复减少须同 commit 收白名单）；
3. 全 46 类型样本覆盖必须有（harvest 回退为零即红）。

已知系统性模式（#238 扫掠结论，2026-10-18）：
- **model 链全线丢 ``errorHandler``/``timeoutHandler`` 子对象**（46 类型 ×
  errorCode/errorMessage/timeoutHandler.*）——Flow.model_dump 的序列化面
  不含错误/超时处理器，是"逐点修不够"的定量证明（190/441 条丢失来自此模式）；
- sexpr 面 60 类型 ``<type-gap>``：plaita-nodes 外部节点类型不在 sexpr
  写/读面的支持范围（合法容量边界，非缺陷）。
"""

import pytest

import _dsl_roundtrip_samples as S

AXES = ("sexpr", "model", "e2e")

# 基线白名单：{类型: {表示面: [丢失路径...]}}（2026-10-18 全量扫掠快照）
BASELINE = {
        'agentrun': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'agentrun.errorHandler.errorCode',
                'agentrun.errorHandler.errorMessage',
                'agentrun.timeoutHandler.errorCode',
                'agentrun.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'api_request': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'api_request.errorHandler.errorCode',
                'api_request.errorHandler.errorMessage',
                'api_request.timeoutHandler.errorCode',
                'api_request.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'approval': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'approval.errorHandler.errorCode',
                'approval.errorHandler.errorMessage',
                'approval.eventType',
                'approval.timeoutHandler.errorCode',
                'approval.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'assignment': {
            'sexpr': [
                'assignment.desc',
                'assignment.name',
                'assignment.timeoutHandler',
            ],
            'model': [
                'assignment.errorHandler.errorCode',
                'assignment.errorHandler.errorMessage',
                'assignment.timeoutHandler.errorCode',
                'assignment.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'assignment.desc',
                'assignment.errorHandler.errorCode',
                'assignment.errorHandler.errorMessage',
                'assignment.name',
                'assignment.timeoutHandler',
            ],
        },
        'capture': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'capture.errorHandler.errorCode',
                'capture.errorHandler.errorMessage',
                'capture.timeoutHandler.errorCode',
                'capture.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'case': {
            'sexpr': [
                'case.desc',
                'case.name',
                'case.next',
                'case.output',
                'case.outputType',
                'case.timeoutHandler',
            ],
            'model': [
                'case.errorHandler.errorCode',
                'case.errorHandler.errorMessage',
                'case.outputType',
                'case.timeoutHandler.errorCode',
                'case.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'case.desc',
                'case.errorHandler.errorCode',
                'case.errorHandler.errorMessage',
                'case.name',
                'case.next',
                'case.output',
                'case.outputType',
                'case.timeoutHandler',
            ],
        },
        'child': {
            'sexpr': [
                'child.childFlow.version',
                'child.desc',
                'child.name',
                'child.output',
                'child.timeoutHandler',
            ],
            'model': [
                'child.errorHandler.errorCode',
                'child.errorHandler.errorMessage',
                'child.timeoutHandler.errorCode',
                'child.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'child.childFlow.version',
                'child.desc',
                'child.errorHandler.errorCode',
                'child.errorHandler.errorMessage',
                'child.name',
                'child.output',
                'child.timeoutHandler',
            ],
        },
        'decision': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'decision.errorHandler.errorCode',
                'decision.errorHandler.errorMessage',
                'decision.timeoutHandler.errorCode',
                'decision.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'delay': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'delay.errorHandler.errorCode',
                'delay.errorHandler.errorMessage',
                'delay.timeoutHandler.errorCode',
                'delay.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'dingtalk_webhook': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'dingtalk_webhook.errorHandler.errorCode',
                'dingtalk_webhook.errorHandler.errorMessage',
                'dingtalk_webhook.timeoutHandler.errorCode',
                'dingtalk_webhook.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'email_send': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'email_send.errorHandler.errorCode',
                'email_send.errorHandler.errorMessage',
                'email_send.timeoutHandler.errorCode',
                'email_send.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'end': {
            'sexpr': [
                'end.desc',
                'end.name',
                'end.timeoutHandler',
            ],
            'model': [
                'end.errorHandler.errorCode',
                'end.errorHandler.errorMessage',
                'end.timeoutHandler.errorCode',
                'end.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'end.desc',
                'end.errorHandler.errorCode',
                'end.errorHandler.errorMessage',
                'end.name',
                'end.timeoutHandler',
            ],
        },
        'event': {
            'sexpr': [
                'event.desc',
                'event.name',
                'event.output',
                'event.timeoutHandler',
            ],
            'model': [
                'event.errorHandler.errorCode',
                'event.errorHandler.errorMessage',
                'event.timeoutHandler.errorCode',
                'event.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'event.desc',
                'event.errorHandler.errorCode',
                'event.errorHandler.errorMessage',
                'event.name',
                'event.output',
                'event.timeoutHandler',
            ],
        },
        'feishu_webhook': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'feishu_webhook.errorHandler.errorCode',
                'feishu_webhook.errorHandler.errorMessage',
                'feishu_webhook.timeoutHandler.errorCode',
                'feishu_webhook.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'filter': {
            'sexpr': [
                'filter.desc',
                'filter.input',
                'filter.itemType',
                'filter.name',
                'filter.output',
                'filter.timeoutHandler',
            ],
            'model': [
                'filter.errorHandler.errorCode',
                'filter.errorHandler.errorMessage',
                'filter.timeoutHandler.errorCode',
                'filter.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'filter.desc',
                'filter.errorHandler.errorCode',
                'filter.errorHandler.errorMessage',
                'filter.input',
                'filter.itemType',
                'filter.name',
                'filter.output',
                'filter.timeoutHandler',
            ],
        },
        'find': {
            'sexpr': [
                'find.desc',
                'find.input',
                'find.itemType',
                'find.name',
                'find.output',
                'find.timeoutHandler',
            ],
            'model': [
                'find.errorHandler.errorCode',
                'find.errorHandler.errorMessage',
                'find.timeoutHandler.errorCode',
                'find.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'find.desc',
                'find.errorHandler.errorCode',
                'find.errorHandler.errorMessage',
                'find.input',
                'find.itemType',
                'find.name',
                'find.output',
                'find.timeoutHandler',
            ],
        },
        'gate': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'gate.errorHandler.errorCode',
                'gate.errorHandler.errorMessage',
                'gate.timeoutHandler.errorCode',
                'gate.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'generic_webhook': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'generic_webhook.errorHandler.errorCode',
                'generic_webhook.errorHandler.errorMessage',
                'generic_webhook.timeoutHandler.errorCode',
                'generic_webhook.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'git_publish': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'git_publish.errorHandler.errorCode',
                'git_publish.errorHandler.errorMessage',
                'git_publish.timeoutHandler.errorCode',
                'git_publish.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'github_comment': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'github_comment.errorHandler.errorCode',
                'github_comment.errorHandler.errorMessage',
                'github_comment.timeoutHandler.errorCode',
                'github_comment.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'hitl': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'hitl.errorHandler.errorCode',
                'hitl.errorHandler.errorMessage',
                'hitl.timeoutHandler.errorCode',
                'hitl.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'hitl_await': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'hitl_await.errorHandler.errorCode',
                'hitl_await.errorHandler.errorMessage',
                'hitl_await.timeoutHandler.errorCode',
                'hitl_await.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'http': {
            'sexpr': [
                'http.content_type',
                'http.desc',
                'http.name',
                'http.timeoutHandler',
            ],
            'model': [
                'http.errorHandler.errorCode',
                'http.errorHandler.errorMessage',
                'http.input',
                'http.timeoutHandler.errorCode',
                'http.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'http.desc',
                'http.errorHandler.errorCode',
                'http.errorHandler.errorMessage',
                'http.input',
                'http.name',
                'http.timeoutHandler',
            ],
        },
        'http_callback': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'http_callback.errorHandler.errorCode',
                'http_callback.errorHandler.errorMessage',
                'http_callback.eventType',
                'http_callback.timeoutHandler.errorCode',
                'http_callback.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'if': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'if.elseNext',
                'if.errorHandler.errorCode',
                'if.errorHandler.errorMessage',
                'if.timeoutHandler.errorCode',
                'if.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'kafka_queue': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'kafka_queue.errorHandler.errorCode',
                'kafka_queue.errorHandler.errorMessage',
                'kafka_queue.eventType',
                'kafka_queue.timeoutHandler.errorCode',
                'kafka_queue.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'llm': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'llm.errorHandler.errorCode',
                'llm.errorHandler.errorMessage',
                'llm.timeoutHandler.errorCode',
                'llm.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'loop': {
            'sexpr': [
                'loop.desc',
                'loop.input',
                'loop.itemType',
                'loop.name',
                'loop.output',
                'loop.timeoutHandler',
            ],
            'model': [
                'loop.errorHandler.errorCode',
                'loop.errorHandler.errorMessage',
                'loop.timeoutHandler.errorCode',
                'loop.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'loop.desc',
                'loop.errorHandler.errorCode',
                'loop.errorHandler.errorMessage',
                'loop.input',
                'loop.itemType',
                'loop.name',
                'loop.output',
                'loop.timeoutHandler',
            ],
        },
        'map': {
            'sexpr': [
                'map.desc',
                'map.input',
                'map.itemType',
                'map.name',
                'map.output',
                'map.timeoutHandler',
            ],
            'model': [
                'map.errorHandler.errorCode',
                'map.errorHandler.errorMessage',
                'map.maxConcurrent',
                'map.timeoutHandler.errorCode',
                'map.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'map.desc',
                'map.errorHandler.errorCode',
                'map.errorHandler.errorMessage',
                'map.input',
                'map.itemType',
                'map.maxConcurrent',
                'map.name',
                'map.output',
                'map.timeoutHandler',
            ],
        },
        'mock': {
            'sexpr': [
                'mock.desc',
                'mock.name',
                'mock.timeoutHandler',
            ],
            'model': [
                'mock.errorHandler.errorCode',
                'mock.errorHandler.errorMessage',
                'mock.timeoutHandler.errorCode',
                'mock.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'notify': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'notify.errorHandler.errorCode',
                'notify.errorHandler.errorMessage',
                'notify.timeoutHandler.errorCode',
                'notify.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'parallel': {
            'sexpr': [
                'parallel.desc',
                'parallel.name',
                'parallel.output',
                'parallel.timeoutHandler',
            ],
            'model': [
                'parallel.errorHandler.errorCode',
                'parallel.errorHandler.errorMessage',
                'parallel.timeoutHandler.errorCode',
                'parallel.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'parallel.desc',
                'parallel.errorHandler.errorCode',
                'parallel.errorHandler.errorMessage',
                'parallel.name',
                'parallel.output',
                'parallel.timeoutHandler',
            ],
        },
        'parse_json': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'parse_json.errorHandler.errorCode',
                'parse_json.errorHandler.errorMessage',
                'parse_json.timeoutHandler.errorCode',
                'parse_json.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'rate_limit': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'rate_limit.errorHandler.errorCode',
                'rate_limit.errorHandler.errorMessage',
                'rate_limit.timeoutHandler.errorCode',
                'rate_limit.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'redis_queue': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'redis_queue.errorHandler.errorCode',
                'redis_queue.errorHandler.errorMessage',
                'redis_queue.eventType',
                'redis_queue.timeoutHandler.errorCode',
                'redis_queue.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'reduce': {
            'sexpr': [
                'reduce.desc',
                'reduce.input',
                'reduce.itemType',
                'reduce.name',
                'reduce.output',
                'reduce.timeoutHandler',
            ],
            'model': [
                'reduce.errorHandler.errorCode',
                'reduce.errorHandler.errorMessage',
                'reduce.timeoutHandler.errorCode',
                'reduce.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'reduce.desc',
                'reduce.errorHandler.errorCode',
                'reduce.errorHandler.errorMessage',
                'reduce.input',
                'reduce.itemType',
                'reduce.name',
                'reduce.output',
                'reduce.timeoutHandler',
            ],
        },
        'reference': {
            'sexpr': [
                'reference.childFlow.version',
                'reference.desc',
                'reference.flowId',
                'reference.flowVersion',
                'reference.name',
                'reference.output',
                'reference.timeoutHandler',
            ],
            'model': [
                'reference.childFlow',
                'reference.errorHandler.errorCode',
                'reference.errorHandler.errorMessage',
                'reference.timeoutHandler.errorCode',
                'reference.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'reference.childFlow',
                'reference.desc',
                'reference.errorHandler.errorCode',
                'reference.errorHandler.errorMessage',
                'reference.flowId',
                'reference.flowVersion',
                'reference.name',
                'reference.output',
                'reference.timeoutHandler',
            ],
        },
        'report': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'report.errorHandler.errorCode',
                'report.errorHandler.errorMessage',
                'report.timeoutHandler.errorCode',
                'report.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'slack_webhook': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'slack_webhook.errorHandler.errorCode',
                'slack_webhook.errorHandler.errorMessage',
                'slack_webhook.timeoutHandler.errorCode',
                'slack_webhook.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'sql_query': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'sql_query.errorHandler.errorCode',
                'sql_query.errorHandler.errorMessage',
                'sql_query.timeoutHandler.errorCode',
                'sql_query.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'start': {
            'sexpr': [
                'start.desc',
                'start.name',
                'start.output',
                'start.timeoutHandler',
            ],
            'model': [
                'start.errorHandler.errorCode',
                'start.errorHandler.errorMessage',
                'start.timeoutHandler.errorCode',
                'start.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'start.desc',
                'start.errorHandler.errorCode',
                'start.errorHandler.errorMessage',
                'start.name',
                'start.output',
                'start.timeoutHandler',
            ],
        },
        'switch': {
            'sexpr': [
                'switch.branches[].desc',
                'switch.branches[].label',
                'switch.desc',
                'switch.name',
                'switch.output',
                'switch.outputType',
                'switch.timeoutHandler',
            ],
            'model': [
                'switch.errorHandler.errorCode',
                'switch.errorHandler.errorMessage',
                'switch.timeoutHandler.errorCode',
                'switch.timeoutHandler.errorMessage',
            ],
            'e2e': [
                'switch.branches[].desc',
                'switch.branches[].label',
                'switch.desc',
                'switch.errorHandler.errorCode',
                'switch.errorHandler.errorMessage',
                'switch.name',
                'switch.output',
                'switch.outputType',
                'switch.timeoutHandler',
            ],
        },
        'wecom_webhook': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'wecom_webhook.errorHandler.errorCode',
                'wecom_webhook.errorHandler.errorMessage',
                'wecom_webhook.timeoutHandler.errorCode',
                'wecom_webhook.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'while': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'while.errorHandler.errorCode',
                'while.errorHandler.errorMessage',
                'while.timeoutHandler.errorCode',
                'while.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'writefile': {
            'sexpr': [
                '<type-gap>',
            ],
            'model': [
                'writefile.errorHandler.errorCode',
                'writefile.errorHandler.errorMessage',
                'writefile.json_mode',
                'writefile.timeoutHandler.errorCode',
                'writefile.timeoutHandler.errorMessage',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
        'flow:header': {
            'sexpr': [
                'flow.globalContext',
                'flow.inputType.children',
                'flow.metadata',
            ],
            'e2e': [
                '<type-gap>',
            ],
        },
    }

def _current_losses():
    """{类型: {轴: [当前丢失路径]}}——全量扫掠。"""
    out = {}
    for t in S.sample_types():
        entry = {}
        for ax in AXES:
            try:
                faces = S.axis_faces(t, ax)
            except Exception as exc:  # 表示面异常按一条丢失登记
                faces = [f"<axis-error> {type(exc).__name__}: {exc}"]
            if faces:
                entry[ax] = sorted(faces)
        if entry:
            out[t] = entry
    return out


def test_all_registry_types_have_samples():
    """全注册类型都有样本（harvest+补齐零缺口）。"""
    have = set(S.sample_types())
    want = set(S.REG.list_types())
    assert want <= have, f"缺样本类型: {sorted(want - have)}"


def test_no_loss_outside_whitelist():
    """当前丢失必须 ⊆ 白名单——新丢失面即红（防恶化）。"""
    current = _current_losses()
    new_losses = []
    for t, axes in current.items():
        allowed = BASELINE.get(t, {})
        for ax, faces in axes.items():
            known = set(allowed.get(ax, []))
            for f in faces:
                if f not in known:
                    new_losses.append(f"{t}.{ax}: {f}")
    assert not new_losses, "白名单外新增丢失面:\n" + "\n".join(new_losses)


def test_total_loss_paths_pinned():
    """丢失总路径数钉死——修复减少须同 commit 收白名单。"""
    total = sum(
        len(faces)
        for axes in _current_losses().values()
        for faces in axes.values()
    )
    pinned = sum(len(faces) for axes in BASELINE.values() for faces in axes.values())
    assert total == pinned, (
        f"丢失总路径 {total} != 白名单钉值 {pinned}——"
        "若为修复所致，请同 commit 收减 BASELINE；若为新增，先定性再登记"
    )


def test_error_handler_family_loss_is_the_known_pattern():
    """系统性模式钉住：model 链丢 errorHandler/timeoutHandler 是已知主模式。"""
    eh = [
        f
        for axes in _current_losses().values()
        for ax, faces in axes.items()
        if ax == "model"
        for f in faces
        if "errorHandler" in f or "timeoutHandler" in f
    ]
    assert len(eh) >= 90, (
        f"model 链 errorHandler/timeoutHandler 丢失面 {len(eh)} 条——"
        "若数量骤降说明有人开始修此面：请同步收白名单并移除本钉"
    )
