"""b94-01 (r94): PLAITA_EVENT_TTL_SECONDS must have an explicit upper bound.

Pre-fix the parser only rejected non-integers and <=0; huge positive values
(including 2^63-1) passed through with no WARNING and later blew up Redis
``SET EX`` (seconds→milliseconds overflow).  Because the write pipeline is
MULTI and Redis does not roll back, the failed SET left a type-index member
without its record (ghost), healed later by lazy ZREM.

Fix: a named 10-year business cap; over-bound values fall back to the 7-day
default with a WARNING naming the cap.  Legal values are unchanged; the lazy
ZREM self-heal remains the backstop.

Negative control: remove the cap, the two 2^63-bound tests go red (the huge
values are returned instead of the default).
"""
from __future__ import annotations

import logging

import pytest

from plaita.event.redis import (
    DEFAULT_EVENT_TTL_SECONDS,
    EVENT_TTL_ENV,
    MAX_EVENT_TTL_SECONDS,
    event_retention_ttl_seconds,
)

OVER_BOUND = (
    str(2**63),
    str(2**63 - 1),
    str(MAX_EVENT_TTL_SECONDS + 1),
)


@pytest.mark.parametrize("raw", OVER_BOUND, ids=["2^63", "2^63-1", "cap+1"])
def test_over_bound_falls_back_with_named_warning(raw, monkeypatch, caplog):
    monkeypatch.setenv(EVENT_TTL_ENV, raw)
    with caplog.at_level(logging.WARNING):
        ttl = event_retention_ttl_seconds()

    assert ttl == DEFAULT_EVENT_TTL_SECONDS
    messages = [
        record.getMessage()
        for record in caplog.records
        if EVENT_TTL_ENV in record.getMessage()
    ]
    assert messages, "over-bound TTL must be observable"
    assert str(MAX_EVENT_TTL_SECONDS) in messages[-1]
    assert "上限" in messages[-1]


@pytest.mark.parametrize(
    "raw", ("1", "120", str(MAX_EVENT_TTL_SECONDS)), ids=["1s", "120s", "cap"]
)
def test_legal_values_unchanged(raw, monkeypatch):
    monkeypatch.setenv(EVENT_TTL_ENV, raw)
    assert event_retention_ttl_seconds() == int(raw)


@pytest.mark.parametrize("raw", ("0", "-1"))
def test_non_positive_still_falls_back(raw, monkeypatch):
    """Existing <=0 policy is not weakened by the new upper bound."""
    monkeypatch.setenv(EVENT_TTL_ENV, raw)
    assert event_retention_ttl_seconds() == DEFAULT_EVENT_TTL_SECONDS


def test_non_numeric_still_falls_back(monkeypatch):
    monkeypatch.setenv(EVENT_TTL_ENV, "not-a-number")
    assert event_retention_ttl_seconds() == DEFAULT_EVENT_TTL_SECONDS
