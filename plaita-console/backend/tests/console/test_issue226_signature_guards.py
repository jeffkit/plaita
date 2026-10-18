"""plaita#226 A1/A2：HMAC 验签的两处安全回归钉。

- **A1**：服务端有效期上限 `_MAX_SIGNATURE_VALIDITY`（1h）。签名的
  `sign_expire` 由客户端 `key-time` 自报；删掉上限后被盗 Authorization 在
  未启用 nonce 时可重放到客户端自报的过期时间（7200s 也接受）。
- **A2**：签名比较必须走 `hmac.compare_digest`。改成 `!=` 后验签仍"正确"，
  但引入时序侧信道；现有测试全绿（issue 实测 14 passed）。

负控：
- A1 把 `if sign_expire - ... > _MAX_SIGNATURE_VALIDITY:` 改为 `if False:`
  ⇒ 7200/3601 用例红；
- A2 把 `hmac.compare_digest(fields["signature"], expected_sig)` 改为
  `fields["signature"] != expected_sig` ⇒ spy 用例红（只剩 secret-id 一次
  `compare_digest` 调用）。
证据：`docs/proofs/226-six-regression-gaps.md`、`docs/proofs/data/226-*`。
"""
from __future__ import annotations

import hmac
import sys
from pathlib import Path
from time import time

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import signature  # noqa: E402
from plaita.client import generate_signature  # noqa: E402

SECRET_ID = "issue226-id"
SECRET_KEY = "issue226-secret"


@pytest.fixture(autouse=True)
def _reset_nonce_cache():
    signature.reset_nonce_cache()
    yield
    signature.reset_nonce_cache()


def _auth(validity: int = 3, when: int | None = None) -> str:
    return generate_signature(
        SECRET_KEY, SECRET_ID, validity, int(when or time()),
    )


# -- A1: server-side validity cap -------------------------------------------


@pytest.mark.parametrize("validity", [3601, 7200])
def test_client_declared_validity_above_cap_is_rejected(validity):
    """客户端自报 3601/7200s 有效期必须被服务端 1h 上限拒绝。"""
    auth = _auth(validity=validity)
    assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY) is False


def test_validity_at_cap_is_accepted():
    """正控：恰好 3600s（上限本身）仍放行，避免把上限写成 '<'。"""
    assert signature.verify_authorization(
        _auth(validity=3600), SECRET_ID, SECRET_KEY,
    ) is True


# -- A2: constant-time signature comparison ---------------------------------


def test_signature_comparison_uses_hmac_compare_digest(monkeypatch):
    """spy 断言签名比较确实走 `hmac.compare_digest`（而非 `!=`）。

    验签有两次常量时间比较：secret-id + signature。`!=` 变异后只剩 secret-id
    那一次 → 本用例红。
    """
    calls = []
    real = hmac.compare_digest

    def spy(a, b):
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(signature.hmac, "compare_digest", spy)
    auth = _auth()
    assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY) is True

    fields = signature._parse_authorization(auth)
    expected = signature._compute_signature(
        SECRET_KEY, fields["sign_time"], fields["key_time"],
    )
    assert len(calls) == 2, (
        "验签应做 secret-id 与 signature 两次 compare_digest；"
        f"实得 {len(calls)} 次：{calls!r}"
    )
    assert (fields["signature"], expected) in calls, (
        "signature 比较未走 hmac.compare_digest（时序侧信道回归）"
    )
