"""HMAC 签名验签单元测试, 含 2026-07 重放保护 (B 方案)。

覆盖:
- 旧路径 (无 nonce): 兼容未升级客户端;
- 新路径 (有 nonce): 重放被拒绝;
- 跨路径: nonce 在签名材料里, 改 nonce 即破坏签名;
- nonce 缓存按 sign_expire 过期, 不会无限增长。
"""
import sys
from pathlib import Path
from time import time

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import signature  # noqa: E402
from plaita.client import generate_signature  # noqa: E402

SECRET_ID = "test-id"
SECRET_KEY = "test-secret-key"


@pytest.fixture(autouse=True)
def _reset_nonce_cache():
    signature.reset_nonce_cache()
    yield
    signature.reset_nonce_cache()


def _make_auth(nonce=None, validity=3, when=None, secret_key=SECRET_KEY,
               secret_id=SECRET_ID) -> str:
    return generate_signature(secret_key, secret_id, validity, int(when or time()),
                              nonce=nonce)


class TestLegacyPathNoNonce:
    def test_valid_signature_passes(self):
        assert signature.verify_authorization(_make_auth(), SECRET_ID, SECRET_KEY)

    def test_wrong_secret_rejected(self):
        assert not signature.verify_authorization(
            _make_auth(secret_key="wrong"), SECRET_ID, SECRET_KEY,
        )

    def test_wrong_secret_id_rejected(self):
        assert not signature.verify_authorization(
            _make_auth(), "other-id", SECRET_KEY,
        )

    def test_expired_rejected(self):
        # 签发时间早就过了有效期
        auth = _make_auth(when=int(time()) - 100)
        assert not signature.verify_authorization(auth, SECRET_ID, SECRET_KEY)

    def test_future_skew_rejected(self):
        # 客户端时钟超前过多 (> _MAX_SKEW=300s)
        auth = _make_auth(when=int(time()) + 10_000)
        assert not signature.verify_authorization(auth, SECRET_ID, SECRET_KEY)

    def test_tampered_authorization_rejected(self):
        auth = _make_auth()
        # 改一个字符
        tampered = auth[:-1] + ("a" if auth[-1] != "a" else "b")
        assert not signature.verify_authorization(tampered, SECRET_ID, SECRET_KEY)

    def test_missing_auth_rejected(self):
        assert not signature.verify_authorization("", SECRET_ID, SECRET_KEY)


class TestReplayProtection:
    def test_first_use_of_nonce_passes(self):
        auth = _make_auth(nonce="nonce-1")
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY)

    def test_replayed_nonce_rejected(self):
        auth = _make_auth(nonce="nonce-2")
        # 第一次放行
        assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY)
        # 第二次 (相同 nonce) 拒绝
        assert not signature.verify_authorization(auth, SECRET_ID, SECRET_KEY)

    def test_different_nonces_each_valid_once(self):
        for i in range(5):
            auth = _make_auth(nonce=f"unique-{i}")
            assert signature.verify_authorization(auth, SECRET_ID, SECRET_KEY), (
                f"nonce unique-{i} 应通过"
            )
            # 重放相同的被拒
            assert not signature.verify_authorization(auth, SECRET_ID, SECRET_KEY)

    def test_nonce_in_signature_material(self):
        # 同一 sign_time, 不同 nonce → 不同 signature
        a1 = _make_auth(nonce="A", when=1_000_000)
        a2 = _make_auth(nonce="B", when=1_000_000)
        assert a1 != a2, "改 nonce 应改 signature"

    def test_legacy_auth_still_works_after_nonce_path_used(self):
        # 用了 nonce 后, 旧式 auth (无 nonce) 仍能通过
        signature.verify_authorization(_make_auth(nonce="once"), SECRET_ID, SECRET_KEY)
        # 旧式
        legacy = _make_auth()
        assert signature.verify_authorization(legacy, SECRET_ID, SECRET_KEY)

    def test_tampered_nonce_rejected(self):
        # 拿一个有效的带 nonce 的 auth, 把 nonce 字段改掉
        # 简单做法: 替换 nonce=nonce-1 → nonce=nonce-X, 签名应该不再匹配
        auth = _make_auth(nonce="legit")
        tampered = auth.replace("nonce=legit", "nonce=tampered")
        assert tampered != auth, "测试前提: replace 应该真改了字符串"
        assert not signature.verify_authorization(tampered, SECRET_ID, SECRET_KEY)

    def test_nonce_cache_expires_after_sign_window(self):
        # 签发时 validity=2s, 把 now 推进 5s 之后, 旧 nonce 过期清掉,
        # 此时用同一 nonce 重新签 (新的时效) 应该能通过——模拟"nonce 自然过期
        # 后, 缓存不再阻碍"。
        # 注意: 新签名 sign_time 也得推进, 否则时效验证会失败。
        t0 = 1_000_000
        auth1 = _make_auth(nonce="recyclable", validity=2, when=t0)
        assert signature.verify_authorization(
            auth1, SECRET_ID, SECRET_KEY, now=float(t0 + 1),
        )
        # 5s 后, 用同一 nonce 重新签 (sign_time 也推进)
        auth2 = _make_auth(nonce="recyclable", validity=2, when=t0 + 5)
        # 在 t0+6 验, auth1 的 nonce 应该已经被惰性清理 (它过期于 t0+2)
        assert signature.verify_authorization(
            auth2, SECRET_ID, SECRET_KEY, now=float(t0 + 6),
        )
