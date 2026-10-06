"""事件过滤器队列名环境变量回归（队列隔离漏口 P1）。

背景：``cluster_config.yaml`` 给 event_filter 配的是 ``PLAITA_QUEUE_NAME``
环境变量，但 event_filter 只认 CLI 参数（``args.queue_name`` 默认硬编码
``plaita:flow:queue``），**不读该 env**——配置形同虚设，事件过滤器会把
resume 事件投到默认队列，无法做队列隔离。

修法：优先级 CLI 参数 > ``PLAITA_QUEUE_NAME`` env > 默认值，与
``flow_worker.py`` 的既有做法一致。
"""
from __future__ import annotations

import argparse
import os
import sys

import pytest

from plaita.server.event_filter import EventFilter, _resolve_queue_name


def _noop_args(**overrides):
    """构造事件过滤器 CLI 参数命名空间（仅 queue_name 相关）。"""
    ns = argparse.Namespace(queue_name=overrides.pop("queue_name", "plaita:flow:queue"))
    ns.__dict__.update(overrides)
    return ns


def test_resolve_queue_name_prefers_cli_over_env(monkeypatch):
    monkeypatch.setenv("PLAITA_QUEUE_NAME", "plaita:flow:queue:env")
    assert _resolve_queue_name("plaita:flow:queue:cli") == "plaita:flow:queue:cli"


def test_resolve_queue_name_uses_env_when_no_cli(monkeypatch):
    monkeypatch.setenv("PLAITA_QUEUE_NAME", "plaita:flow:queue:v2")
    # CLI 未显式给出（None 或等于默认值都视为未给出）
    assert _resolve_queue_name(None) == "plaita:flow:queue:v2"


def test_resolve_queue_name_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("PLAITA_QUEUE_NAME", raising=False)
    monkeypatch.delenv("QUEUE_NAME", raising=False)
    assert _resolve_queue_name(None) == "plaita:flow:queue"
