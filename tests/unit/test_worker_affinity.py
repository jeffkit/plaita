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

from plaita.server.flow_worker import RedisFlowWorker, TaskNotForThisWorker
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


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
