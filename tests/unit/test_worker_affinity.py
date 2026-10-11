"""机器亲和闸（路线二首版）：worker 不领「路径不在本机」的任务。

背景（plaita#41 双机实证）：任务参数 repo/run_dir 是派发方机器的绝对路径。
本机抢到就跑 → OSError 路径不存在 → 烧 delivery 配额 → 死信。本闸让不亲和的
worker 礼貌让出（不 ack、留 pending），交给持有该路径的 worker。

红线：宽松优先——拿不到路径/相对路径/路径存在 → 一律放行，宁可多跑一次失败，
不可误拦本机该跑的任务。
"""
from __future__ import annotations

import os
import tempfile

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")

from plaita.server.flow_worker import RedisFlowWorker, TaskNotForThisWorker  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402


def _worker():
    # 亲和闸挂在 RedisFlowWorker（消费入口所在类）
    return RedisFlowWorker.__new__(RedisFlowWorker)


def test_absolute_missing_repo_is_mismatch():
    w = _worker()
    msg = {"type": "start", "params": {"repo": "/nonexistent/path/to/repo-xyz"}}
    reason = w._detect_affinity_mismatch(msg)
    assert reason is not None and "repo" in reason


def test_existing_repo_is_affine():
    w = _worker()
    with tempfile.TemporaryDirectory() as d:
        msg = {"type": "start", "params": {"repo": d}}
        assert w._detect_affinity_mismatch(msg) is None


def test_no_params_is_affine():
    w = _worker()
    assert w._detect_affinity_mismatch({"type": "start"}) is None
    assert w._detect_affinity_mismatch({"type": "start", "params": {}}) is None


def test_relative_path_is_affine():
    """相对路径/标识符无法判定 → 放行（宽松原则）。"""
    w = _worker()
    assert w._detect_affinity_mismatch({"type": "start", "params": {"repo": "plaita"}}) is None


def test_run_dir_probes_parent_repo():
    """run_dir 首次运行时可能尚未建，查其父（repo/.flowcast/runs）已存在即放行。"""
    w = _worker()
    with tempfile.TemporaryDirectory() as d:
        # repo 存在，run_dir 尚不存在 → 应亲和（首次运行）
        msg = {"type": "start", "params": {
            "repo": d,
            "run_dir": f"{d}/.flowcast/runs/pipeline-1-2026",
        }}
        assert w._detect_affinity_mismatch(msg) is None


def test_resume_message_not_checked():
    """resume 任务不做亲和判（路径已由首次 start 验过）。"""
    w = _worker()
    msg = {"type": "resume", "params": {"repo": "/nonexistent/xyz"}}
    assert w._detect_affinity_mismatch(msg) is None


def test_dispatch_raises_not_for_this_worker():
    """消费入口：不亲和 → 抛 TaskNotForThisWorker（run() 据此不 ack 留 pending）。"""
    w = _worker()
    with pytest.raises(TaskNotForThisWorker):
        w._dispatch_task(
            {"type": "start", "flow_id": "f", "params": {"repo": "/nonexistent/xyz"}}
        )


def test_affinity_disable_switch(monkeypatch):
    """PLAITA_DISABLE_AFFINITY=1 → 不拦（不分路径直接进派发）。"""
    from plaita.server import flow_worker as fw
    monkeypatch.setenv("PLAITA_DISABLE_AFFINITY", "1")
    assert fw._affinity_disabled() is True
    w = _worker()
    # 关了闸，不亲和的也会进 _dispatch_task（后续失败与我们无关，只要不是
    # TaskNotForThisWorker 即可）
    with pytest.raises(Exception) as ei:
        w._dispatch_task({"type": "start", "flow_id": "f",
                          "params": {"repo": "/nonexistent/xyz"}})
    assert not isinstance(ei.value, TaskNotForThisWorker)


class TestAffinityHandover:
    """交接：不亲和任务 ack 原条 + 重入队新副本（delivery 归 1），不死信。

    回归 2026-10-06 双机实测缺陷：只「留 pending」会被对端反复 XCLAIM 虚增
    delivery，5 轮触顶 → 误死信。
    """

    def _worker_with_redis(self):
        import fakeredis
        w = RedisFlowWorker.__new__(RedisFlowWorker)
        w.redis_client = fakeredis.FakeRedis(decode_responses=True)
        w.queue_name = "q:test"
        return w

    class _FakeQueue:
        def __init__(self):
            self.acked = []
            self.conflicts = 0
        def ack(self, mid):
            self.acked.append(mid)
        def note_lease_conflict(self):
            self.conflicts += 1

    def test_handover_acks_and_reenqueues(self):
        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f",
                "params": {"repo": "/nonexistent/xyz"}}
        task = type("T", (), {"message_id": "1-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("no affinity"))
        assert q.acked == ["1-0"], "原条应被 ack（移出 PEL，防对端再抢）"
        assert w.redis_client.xlen("q:test") == 1, "应重入队恰好一份新副本"

    def test_handover_is_idempotent_no_pingpong(self):
        """同一消息二次让出 → 命中标记，改为留 pending（不再重入队）。"""
        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f",
                "params": {"repo": "/nonexistent/xyz"}}
        task = type("T", (), {"message_id": "1-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert w.redis_client.xlen("q:test") == 1
        # 第二次（模拟新副本又被本机抢到）
        task2 = type("T", (), {"message_id": "2-0", "body": body})()
        w._handover_non_affine(task2, q, RuntimeError("x"))
        assert w.redis_client.xlen("q:test") == 1, "第二次不应再重入队（防 ping-pong）"
        assert q.acked == ["1-0"], "第二次不再 ack"

    def test_handover_without_body_leaves_pending(self):
        w = self._worker_with_redis()
        q = self._FakeQueue()
        task = type("T", (), {"message_id": "1-0", "body": None})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert q.acked == [] and w.redis_client.xlen("q:test") == 0


# ── 按仓拒跑名单（2026-10-07）：PLAITA_WORKER_DENY_REPOS ──


def test_deny_repo_blocks_even_when_path_exists(monkeypatch):
    """路径在本机存在（大仓软链双机可解析）时仍可按仓名拒收——重仓留给
    大容量 worker（2 核远端跑 cargo 饱和，实测 load 3.59）。"""
    w = _worker()
    with tempfile.TemporaryDirectory() as d:
        repo = os.path.join(d, "recursive")
        os.makedirs(repo)
        monkeypatch.setenv("PLAITA_WORKER_DENY_REPOS", "recursive, ilink-hub")
        reason = w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": repo}})
        assert reason is not None and "拒跑名单" in reason
        # 不在名单的仓照常亲和
        other = os.path.join(d, "plaita")
        os.makedirs(other)
        assert w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": other}}) is None


def test_deny_repo_empty_is_noop(monkeypatch):
    """空名单 = 不拒（默认零行为变化）。"""
    w = _worker()
    monkeypatch.delenv("PLAITA_WORKER_DENY_REPOS", raising=False)
    with tempfile.TemporaryDirectory() as d:
        repo = os.path.join(d, "recursive")
        os.makedirs(repo)
        assert w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": repo}}) is None


def test_deny_repo_matches_basename_only(monkeypatch):
    """按仓名（basename）匹配，路径前缀不同不影响；resume 消息不查（同亲和闸口径）。"""
    w = _worker()
    monkeypatch.setenv("PLAITA_WORKER_DENY_REPOS", "recursive")
    with tempfile.TemporaryDirectory() as d:
        repo = os.path.join(d, "some", "other", "recursive")
        os.makedirs(repo)
        assert w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": repo}}) is not None
        assert w._detect_affinity_mismatch(
            {"type": "resume", "params": {"repo": repo}}) is None


class TestRunDirHostOwnership:
    """run_dir 的**宿主归属**补判（2026-10-11，plaita#62/#63/#65 根因）。

    双机间有 `/Users/kong/projects → /home/ubuntu/projects` 软链 ⇒ **仓根两边
    都能解析**，原判据（只查仓根）予以放行；但 `<repo>/.flowcast/runs/<run_id>`
    是**建它的那台机器本地**的，对端没有 ⇒ 对端领到后 `sync_in` 必失败：

        AgsError: 宿主临时索引 add 失败：fatal: cannot change to
        '<repo>/.flowcast/runs/<run_id>/worktree': No such file or directory

    实测单条执行在 VM 侧刷 45 次该报错，且与 Mac 侧的正常推进**并发**（同一
    execution 被两台 worker 同时处理）。本组用例钉死三态。
    """

    def _w(self):
        return RedisFlowWorker.__new__(RedisFlowWorker)

    def test_missing_run_dir_rejected_when_runs_exists(self, tmp_path):
        """runs/ 存在而本 run 目录不存在 ⇒ 属别的宿主 ⇒ 拒领。"""
        base = tmp_path / "repo"
        (base / ".flowcast" / "runs" / "other-run").mkdir(parents=True)
        msg = {"type": "start", "params": {
            "repo": str(base),
            "run_dir": str(base / ".flowcast" / "runs" / "NOT-MINE")}}
        reason = self._w()._detect_affinity_mismatch(msg)
        assert reason is not None, "本 run 目录不在本机，应判不亲和（拒领）"
        assert "别的宿主" in reason

    def test_own_run_dir_accepted(self, tmp_path):
        """本 run 目录已存在 ⇒ 是本机的 ⇒ 放行。"""
        base = tmp_path / "repo"
        (base / ".flowcast" / "runs" / "MINE").mkdir(parents=True)
        msg = {"type": "start", "params": {
            "repo": str(base),
            "run_dir": str(base / ".flowcast" / "runs" / "MINE")}}
        assert self._w()._detect_affinity_mismatch(msg) is None

    def test_brand_new_run_accepted(self, tmp_path):
        """全新 run（runs/ 尚不存在）⇒ 不拦，留给派发方本机领取。"""
        base = tmp_path / "repo"
        (base / ".flowcast").mkdir(parents=True)
        msg = {"type": "start", "params": {
            "repo": str(base),
            "run_dir": str(base / ".flowcast" / "runs" / "BRAND-NEW")}}
        assert self._w()._detect_affinity_mismatch(msg) is None
