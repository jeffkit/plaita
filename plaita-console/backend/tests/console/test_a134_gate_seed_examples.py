"""A134-04 负控：examples 种子发布面补第三点 ``gate_fn`` 注入。

缺陷（A1 台账 r134 提报 / B2 r89 核销）：r86 给发布 re-gate 注入了
publish/import 两点（api/flows.py ``where="publish"``/``"import.publish"``），
examples 文档示例生成面漏注——``seed_example_flows`` 的
``store.publish_version(...)`` 无门（生产路径：main.py 启动期 local_mode
种子 hello-plaita/list-map/http-echo）。「门=发布唯一执法点」口径破例：
未来门收紧时示例定义静默绕门（worker 不支持的节点集照样发布，dispatch 才炸）。

修法：种子发布默认带门（与 API 发布同一组闸——编码闸不依赖 redis 照样执法；
能力门数据源是 worker 注册清单，种子只发生在 local_mode、对 ``redis=None``
自跳过，与本地档 HTTP 发布同判）；调用方可注入 ``gate_fn`` 覆盖（测试/未来
集群启动传真实 redis）。单例门拒=跳过该例 WARNING，不炸启动（既有 per-example
except 语义不变）。

判红口径：结构格=种子发布必须携带非 None ``gate_fn``；行为格=注入拒绝门
→ 该例跳过、版本保持 draft、启动不受影响。
"""

import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import examples, flow_store  # noqa: E402


class _SpyStore:
    """记录 ``publish_version`` 调用形状的透明代理（其余方法直通真 store）。"""

    def __init__(self, inner):
        self._inner = inner
        self.publish_calls = []

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def publish_version(self, flow_id, version, tenant_id=None, gate_fn=None):
        self.publish_calls.append(
            {"flow_id": flow_id, "version": version, "gate_fn": gate_fn}
        )
        return self._inner.publish_version(
            flow_id, version, tenant_id=tenant_id, gate_fn=gate_fn
        )


@pytest.fixture()
def store(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'seed.db'}")
    return flow_store.get_flow_store()


class _SeedGateRejection(RuntimeError):
    """测试注入的拒绝门异常（生产默认门抛 HTTPException，同被 except 兜住）。"""


class TestSeedPublishCarriesGate:
    """结构格：第三点必须与其他发布执法点同构（gate_fn 非 None）。"""

    def test_seed_publish_calls_carry_gate_fn(self, store):
        spy = _SpyStore(store)

        created = examples.seed_example_flows(store=spy)

        assert created == 3
        assert len(spy.publish_calls) == 3
        for call in spy.publish_calls:
            assert call["gate_fn"] is not None, (
                f"种子发布 {call['flow_id']}@{call['version']} 未带门"
                "（A134-04：门=发布唯一执法点，种子面不得破例）"
            )

    def test_default_gate_passes_static_examples(self, store):
        """默认门（编码闸 live + 能力门 local 跳过）对静态示例定义必须放行。"""
        created = examples.seed_example_flows()

        assert created == 3
        flow_ids = {f.flow_id for f in flow_store.get_flow_store().list_flows()}
        assert {"hello-plaita", "list-map", "http-echo"} == flow_ids


class TestSeedGateRejectionIsolation:
    """行为格：门拒=该例版本保持 draft（WARNING），启动面零影响。

    注意 ``ensure_flow`` 先于门执行——门拒不留「流程行消失」形状，判据是
    **版本发布状态**（未过门=永不 published）。
    """

    @staticmethod
    def _version_status(flow_id: str) -> str:
        row = flow_store.get_flow_store().get_version(flow_id, "1.0.0")
        return row.status if row is not None else "<missing>"

    def test_rejecting_gate_skips_all_examples_without_crash(self, store):
        def _reject_all(row):
            raise _SeedGateRejection("门拒（测试注入）")

        created = examples.seed_example_flows(store=store, gate_fn=_reject_all)

        assert created == 0
        for flow_id in ("hello-plaita", "list-map", "http-echo"):
            assert self._version_status(flow_id) == "draft", flow_id

    def test_rejecting_one_flow_does_not_block_others(self, store):
        def _reject_hello(row):
            if row.flow_id == "hello-plaita":
                raise _SeedGateRejection("门拒（测试注入）")

        created = examples.seed_example_flows(store=store, gate_fn=_reject_hello)

        assert created == 2
        assert self._version_status("hello-plaita") == "draft"
        assert self._version_status("list-map") == "published"
        assert self._version_status("http-echo") == "published"
