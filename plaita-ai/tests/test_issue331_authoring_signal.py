"""plaita#331 作者期信号：flow_compile(profile=) 非致命报告 + flow_list_nodes 行内 requires/suspends。

引擎侧执行入口/闭包/兜底在 plaita ``tests/unit/test_issue331_execution_profile.py``；
本文件钉 flow_runner 层的两个信号面（A2 r61 评审 §4：形状扩展向后兼容、
``ok`` 判定不变、数据源 = registry 声明不重扫源码）。MCP 工具体消费面
（flow_compile/flow_list_nodes 传参透传与 JSON 形状）在
``test_mcp_integration.py``（TestMcpServerTools，需真 mcp 包）。
"""
from plaita_ai.flow_runner import compile_flow, list_node_types

EVENT_SRC = '''
@flow("wait_flow", input_type="object")
def wait_flow(INPUT):
    EVENT(type="approve_me")
    return "done"
'''

CHILD_EVENT_SRC = '''
@childflow("inner")
def inner():
    EVENT(type="inner_event")
    return "inner-done"


@flow("outer", input_type="object")
def outer(INPUT):
    r = CHILD(flow=inner)
    return "outer-done"
'''


def test_compile_with_profile_reports_requirements_without_changing_ok():
    """验收格：含 event 的源码 compile(profile="normal") 返回
    execution_requirements 但 ``ok`` 不变（非致命，作者期只提示不拦截）。"""
    plain = compile_flow(EVENT_SRC)
    assert plain.ok and plain.execution_requirements is None
    assert "execution_requirements" not in plain.to_dict()  # 稀疏键：不传不出现

    reported = compile_flow(EVENT_SRC, profile="normal")
    assert reported.ok
    req = reported.execution_requirements
    assert req == {
        "profile": "normal",
        "required": ["event_bus", "suspend:root"],
        "missing": ["event_bus", "suspend:root"],
    }


def test_compile_profile_distributed_missing_empty():
    reported = compile_flow(EVENT_SRC, profile="distributed")
    assert reported.ok
    assert reported.execution_requirements["missing"] == []
    assert reported.execution_requirements["required"] == ["event_bus", "suspend:root"]


def test_compile_profile_closure_is_recursive_over_childflow():
    """递归闭包：@childflow 内的 EVENT 计入 required（嵌套 child token 累积）。"""
    reported = compile_flow(CHILD_EVENT_SRC, profile="normal")
    assert reported.ok
    req = reported.execution_requirements
    assert req["missing"] == ["event_bus", "suspend:root"]


def test_compile_unknown_profile_is_compile_error_not_crash():
    """未知 profile 名按一条编译错误呈现（工具体不炸、ok=false）。"""
    reported = compile_flow(EVENT_SRC, profile="staging")
    assert not reported.ok
    assert "staging" in reported.errors[0].message


def test_flow_list_nodes_rows_carry_requires_and_suspends():
    """行内 requires/suspends：event 声明 [event_bus, suspend:root] 且
    suspends=true；纯控制流节点 requires=[] suspends=false。"""
    rows = {r["node_type"]: r for r in list_node_types()}
    assert rows["event"]["requires"] == ["event_bus", "suspend:root"]
    assert rows["event"]["suspends"] is True
    assert rows["start"]["requires"] == []
    assert rows["start"]["suspends"] is False
    assert rows["http"]["requires"] == ["network"]
    assert rows["http"]["suspends"] is False
