"""基线行为探针：派发失败窗——旧契约=dispatch_pending armed（#320 改造前）。"""
import json
from unittest.mock import MagicMock, patch
import fakeredis
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1", "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
fake = fakeredis.FakeRedis(decode_responses=True)
fake.rpush = MagicMock(side_effect=ConnectionError("probe: rpush down"))
storage = MemoryExecutionStorage()
fs = MemoryFlowStorage(); fs.save_flow(TEST_FLOW)
worker = RedisFlowWorker(
    redis_url="redis://localhost:6379/15", queue_name="probe",
    execution_storage=storage, flow_storage=fs, redis_client=fake,
    enable_registry=False, enable_redis_logging=False,
)
storage.save_execution_state("e1", ExecutionState(
    execution_id="e1", flow_id="f1", flow_version="1", status="running",
    context={"$LAST_NODE": "a", "$NODE": {"a": {"subscription_id": "s1", "event_type": "evt.go"}}},
))
with patch("plaita.server.flow_worker.FlowExecution") as FE:
    FE.return_value.run_distributed.return_value = {
        "execution_id": "e1", "is_suspend": True, "service_config": {"type": "delay", "delay_ms": 100},
        "context": {"$LAST_NODE": "a", "$NODE": {"a": {}}},
    }
    try:
        worker.resume_flow("f1", "e1", "continue")
    except Exception as e:
        print("raised:", type(e).__name__)
row = storage.load_execution_state("e1")
print("BASELINE dispatch_pending:", json.dumps(row.dispatch_pending))
print("BASELINE pending attr:", getattr(row, "pending", "FIELD-ABSENT"))
