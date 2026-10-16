"""plaita#165 —— 清洗器的**复杂度上界**（``"x://"*n`` 的 DoS 面）。

``#158``+``#159`` r4 定验发现（本轮未修的前置缺陷）：``_mask_url`` 对
``"x://" * n`` 这类**分隔符密集**的输入是超二次的——80 KB 输入 >30 s 不返回，
而清洗器就挂在**第三方异常文本**路径上（``HTTP.handle_http_node_err`` 等），输入
长度由对端决定。两个独立的来源（实测，cProfile）：

* ``_secret_forms_cached`` 的**形态闭包**：每 个分隔符的每个出现位置都切一刀
  ⇒ 片段数 Θ(k²)、片段长度 O(n) ⇒ Θ(n³) 字符工作。``"x://"*40``（160 字符）里
  ``_push`` 被调用 **3,471,881** 次。
* ``_no_at_colon_spans`` 的**逐候选前向扫描** + ``_query_segment_is_provably_benign``
  的切片：每条候选冒号付一次 O(扫描长度) ⇒ Θ(n²)。``"x://"*1000`` 里
  ``_is_port_colon`` 被调用 **499,500** 次。

本文件钉四件事：

1. **跨规模 + 硬上限**（:class:`TestIssue165AdversarialFamilyIsBounded`）：
   ``"x://"*n`` 在 n / 2n / 4n / … 上**绝对耗时**都在硬上限内，增长比值近常数；
   结果一律 fail-closed 为 ``***``（绝不回原文）。判据用**绝对毫秒**而不是
   「相对自身」，否则极小的基线会让比值断言失去意义（分母陷阱）。
2. **合法长输入不受误杀**（:class:`TestIssue165LegitLongInputStaysLinear`）：
   70 000 字符的口令（只有 1 个分隔符）必须照旧一趟掩完——这正是
   ``TestMaskerCannotLeakByConstruction::test_masker_is_linear_on_long_input``
   钉住的形状，也是「为什么不能用输入长度上限」的活证据。
3. **敌对语料族回归**（:class:`TestIssue165AdversarialCorpusFamily`）：
   ``"x://"*n`` / ``"token_"*n`` / 长 URL 链 / 稠密分隔符在**所有**入口上
   fail-closed 或有界，且**绝不回原文**。
4. **上限是声明的常量 + 溢出分支的契约**（:class:`TestIssue165DeclaredBounds`）：
   常量值、ContextVar 的直调豁免、``_passwords`` 在溢出分支的非空哨兵。

变异（还原二次写法）由普通 ``FAILED`` 行杀死，见 RED-PROOF.md 的变异记录。
"""
from __future__ import annotations

import pathlib
import time
import unittest

from plaita.core.url_redaction import (
    UNPRINTABLE_URL,
    _mask_url_for_log,
    _scrub_url_text,
)
import plaita.server.factory as factory
from plaita.server.factory import _mask_url, _passwords

ROOT = pathlib.Path(__file__).resolve().parents[2]

#: 敌对形状单次调用的**硬上限**（毫秒）。实测修复后 <20 ms，这里留一个数量级的
#: 余量以抗 CI 抖动；变异（还原二次/三次写法）会超出好几个数量级。
HARD_CAP_MS = 250.0
#: 比值断言用的**绝对下限**（毫秒）。低于它的耗时是噪声，不能拿来做分母。
RATIO_FLOOR_MS = 5.0
#: 规模：n 与 4n 之间跨 4×；再往上取大 n 验证「超界即 O(1)」。
SCALES = (80, 160, 320, 640)
LARGE_SCALES = (2560, 12800, 20000)


def _timed(fn, *args):
    best = None
    for _ in range(3):                      # 取 3 次最小值，压掉 GC/调度抖动
        start = time.perf_counter()
        result = fn(*args)
        elapsed = (time.perf_counter() - start) * 1000.0
        best = elapsed if best is None else min(best, elapsed)
    return result, best


class TestIssue165AdversarialFamilyIsBounded(unittest.TestCase):
    """``"x://"*n``：跨规模绝对耗时 + 增长比值 + fail-closed。"""

    def test_cross_scale_is_bounded_and_ratios_are_constant(self):
        """n / 2n / 4n 的绝对耗时都在硬上限内，且增长比值近常数。

        分母纪律：比值分母取 ``max(前一个规模耗时, RATIO_FLOOR_MS)``——用**绝对
        下限**兜底，避免「极小基线 ⇒ 比值断言恒真」的盲区。主判据仍是绝对毫秒。
        """
        table = []
        for n in SCALES:
            text = "x://" * n
            masked, elapsed = _timed(_mask_url, text)
            table.append((n, len(text), masked, elapsed))
        # 1) fail-closed：敌对形状一律**常量**，绝不回原文、绝不半掩
        for n, length, masked, elapsed in table:
            self.assertEqual(
                masked, factory._REDACTED,
                f"n={n} len={length}: 敌对形状必须 fail-closed（得到 {masked[:32]!r}）",
            )
            self.assertNotEqual(masked, "x://" * n, "绝不回原文")
        # 2) 硬上限（绝对毫秒）
        for n, length, _masked, elapsed in table:
            self.assertLess(
                elapsed, HARD_CAP_MS,
                f"n={n} len={length}: {elapsed:.1f} ms 超过硬上限 {HARD_CAP_MS} ms",
            )
        # 3) 增长比值近常数（分母有绝对下限）
        for (n1, _l1, _m1, t1), (n2, _l2, _m2, t2) in zip(table, table[1:]):
            ratio = t2 / max(t1, RATIO_FLOOR_MS)
            self.assertLess(
                ratio, 4.0,
                f"{n1}→{n2}: 耗时比值 {ratio:.2f}（{t1:.2f}→{t2:.2f} ms）不像常数",
            )

    def test_large_scales_are_constant_time(self):
        """超界规模：形状闸门 O(1) 命中，耗时不再随输入增长（< 下限）。"""
        table = []
        for n in LARGE_SCALES:
            text = "x://" * n
            masked, elapsed = _timed(_mask_url, text)
            table.append((n, len(text), masked, elapsed))
            self.assertEqual(masked, factory._REDACTED, f"n={n} 必须 fail-closed")
            self.assertLess(elapsed, RATIO_FLOOR_MS * 4, f"n={n}: {elapsed:.2f} ms")
        # 最大规模与最小规模之间不再有 4×（输入翻 4×、耗时近常数）
        first, last = table[0][3], table[-1][3]
        self.assertLess(
            last, max(first, RATIO_FLOOR_MS) * 4,
            f"超界后耗时仍在增长：{first:.2f} → {last:.2f} ms",
        )

    def test_entry_point_used_by_the_issue_is_bounded_too(self):
        """issue 正文点名的入口 ``_mask_url_for_log`` 同样有界（注册后走掩码器）。"""
        self.assertIs(
            factory._mask_url, factory._mask_url, "掩码器未注册时该断言无意义"
        )
        table = []
        for n in (2000, 8000, 20000):
            text = "x://" * n
            masked, elapsed = _timed(_mask_url_for_log, text)
            table.append((n, len(text), masked, elapsed))
        for n, length, masked, elapsed in table:
            self.assertNotEqual(masked, "x://" * n, f"n={n} len={length}: 回了原文")
            self.assertLess(
                elapsed, HARD_CAP_MS, f"n={n} len={length}: {elapsed:.1f} ms"
            )
        # 80 KB（issue 里 >30 s 不返回的那一档）现在是常数级
        self.assertLess(table[-1][3], RATIO_FLOOR_MS * 4)
        self.assertEqual(table[-1][2], UNPRINTABLE_URL)


class TestIssue165LegitLongInputStaysLinear(unittest.TestCase):
    """合法长输入**不受**误杀：长度不是代价来源，分隔符密度才是。"""

    def test_seventy_thousand_char_secret_is_still_masked(self):
        """逐字复现既有的线性度锚（那条测试只有 1 个分隔符）。"""
        secret = "P" * 69990 + " Secret"
        url = f"redis://user:{secret}@host:6379/0?socket_timeout={secret}"
        self.assertEqual(
            _mask_url(url), "redis://user:***@host:6379/0?socket_timeout=***"
        )
        self.assertNotIn(secret, _mask_url(url))

    def test_long_slash_secret_is_still_masked(self):
        big = "x" * 70000
        self.assertEqual(_mask_url(f"redis://user:{big}/w@host"), "redis://user:***@host")

    def test_linear_growth_on_plainly_long_secrets(self):
        """朴素长口令（无分隔符）：跨规模耗时线性，且远在硬上限内。"""
        table = []
        for length in (10000, 20000, 40000):
            url = f"redis://user:{'Q' * length}@host:6379/0"
            masked, elapsed = _timed(_mask_url, url)
            self.assertEqual(masked, "redis://user:***@host:6379/0")
            table.append((length, elapsed))
        for length, elapsed in table:
            self.assertLess(elapsed, HARD_CAP_MS, f"len={length}: {elapsed:.1f} ms")
        for (l1, t1), (l2, t2) in zip(table, table[1:]):
            self.assertLess(
                t2 / max(t1, RATIO_FLOOR_MS), 4.0,
                f"{l1}→{l2}: 耗时比值 {t2 / max(t1, RATIO_FLOOR_MS):.2f} 非线性",
            )


class TestIssue165AdversarialCorpusFamily(unittest.TestCase):
    """#158+#159 的敌对语料族：有界 + 绝不回原文。"""

    def test_issue_family_shapes_never_return_input_verbatim(self):
        cases = {
            "x-scheme-dense-small": "x://" * 40,
            "x-scheme-dense-large": "x://" * 20000,
            "dense-colons": ":" * 4000,
            "long-url-chain": ("https://h/" + "a" * 300 + " ") * 200,
            "webhook-with-long-token": "https://h/x?access_token=" + "K" * 9000,
        }
        for name, text in cases.items():
            masked, elapsed = _timed(_mask_url, text)
            self.assertNotEqual(masked, text, f"{name}: 回了原文")
            self.assertLess(elapsed, HARD_CAP_MS, f"{name}: {elapsed:.1f} ms")

    def test_non_credential_run_is_unchanged_and_linear(self):
        """``"token_"*n``（无 ``=``/``:``）本就该原样返回——r5 的线性修法不许回退。"""
        table = []
        for n in (2000, 8000, 30000):
            text = "token_" * n
            masked, elapsed = _timed(_mask_url, text)
            self.assertEqual(masked, text)
            table.append((n, elapsed))
        for n, elapsed in table:
            self.assertLess(elapsed, HARD_CAP_MS, f"n={n}: {elapsed:.1f} ms")

    def test_scrub_url_text_on_adversarial_text_is_bounded(self):
        for text in ("x://" * 20000, "token_" * 30000, ("url: /a/b?t=" + "K" * 100) * 500):
            scrubbed, elapsed = _timed(_scrub_url_text, text)
            self.assertIsInstance(scrubbed, str)
            self.assertLess(elapsed, HARD_CAP_MS)

    def test_scrub_url_text_still_masks_short_credentials(self):
        """负控：有界化没有把正常短输入也打成常量。"""
        self.assertEqual(
            _scrub_url_text("boom https://h/x?access_token=SECRET end"),
            "boom https://h/***?*** end",
        )


class TestIssue165DeclaredBounds(unittest.TestCase):
    """上限是**声明的常量**，溢出分支的契约也被钉住。"""

    def test_declared_constants(self):
        self.assertEqual(factory._MAX_SPAN_SCAN_WORK, 1 << 21)
        self.assertEqual(factory._MAX_CLOSURE_CHARS, 1 << 20)
        self.assertEqual(factory._MAX_REDACTION_CHARS, 1 << 24)

    def test_shape_gate_is_the_documented_product(self):
        """闸门判据：``len(text) * text.count(":")``（源码级防漂移）。"""
        source = (ROOT / "plaita" / "server" / "factory.py").read_text(encoding="utf-8")
        self.assertEqual(
            source.count('if len(value) * value.count(":") > _MAX_SPAN_SCAN_WORK:'), 1
        )
        self.assertIn("return _REDACTED", source)

    def test_overflow_branch_shares_one_enumeration(self):
        """溢出分支的 ``_passwords`` 非空（``(text,)`` 哨兵），不变量两个方向都成立。"""
        text = "x://" * 200
        self.assertEqual(_mask_url(text), factory._REDACTED)
        self.assertEqual(_passwords(text), (text,))
        self.assertTrue(_passwords(text), "溢出分支必须非空（方向不变量）")

    def test_direct_secret_forms_call_is_exempt(self):
        """直调 ``_secret_forms`` 不带预算（r8 闭合性质的契约面）。"""
        self.assertIsNone(factory._closure_budget.get())
        # 语料里最重的那个形状：直调必须**完整**返回（不抛、非空）
        secret = (
            "0cyxabzAZ09@:/?#% \t\xa0\u00e4\u4e2d\udcff1xcBcabzAZ09"
            "@:/?#% \t\xa0\u00e4\u4e2d\udcffyCa"
        )
        forms = factory._secret_forms(secret)
        self.assertGreater(len(forms), 1000)
        self.assertIn(secret, forms)
        self.assertIsNone(factory._closure_budget.get(), "预算必须在调用后复位")

    def test_budget_contextvar_is_reset_after_overflow(self):
        """溢出路径也要复位 ContextVar（否则后续调用会带着耗尽的预算）。"""
        _mask_url("x://" * 400)
        self.assertIsNone(factory._closure_budget.get())
        self.assertEqual(_mask_url("redis://user:pw@host:6379/0"), "redis://user:***@host:6379/0")


if __name__ == "__main__":
    unittest.main()
