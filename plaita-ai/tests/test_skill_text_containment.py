"""Security containment tests for get_skill_text / flow_get_skill (#248).

``get_skill_text`` concatenated caller-supplied ``skill_name`` into both the
package-resource path and the editable fallback path with no containment:
``../../...`` escaped ``plaita_ai/skills`` and read any host ``SKILL.md``.
The MCP tool ``flow_get_skill`` passed the name through unchanged.

These tests assert the NAMED refusal (``PathTraversalError`` /
``error_type: "PathTraversalError"`` refusal payload), that the leaked host
content never appears in the MCP output, and that the legitimate bundled
skills (default ``flow-coder`` included) still load byte-for-byte unchanged.

Zero author-code involvement: the traversal target is a temp directory with a
sentinel ``SKILL.md``; containment is checked from real filesystem state.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

# Ensure plaita-ai is importable (same layout as test_skill_reference_containment.py)
_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)


def _inject_stub_mcp():
    """Insert a minimal stub mcp.server.fastmcp into sys.modules (mcp absent)."""
    if "mcp" in sys.modules:
        return

    class _MockFastMCP:
        def __init__(self, name, **kwargs):
            self.name = name
            self.instructions = kwargs.get("instructions", "")

        def tool(self, *args, **kwargs):
            def decorator(fn):
                return fn

            if args and callable(args[0]):
                return args[0]
            return decorator

        def run(self):
            pass

    mcp_pkg = types.ModuleType("mcp")
    mcp_server = types.ModuleType("mcp.server")
    mcp_fastmcp = types.ModuleType("mcp.server.fastmcp")
    mcp_fastmcp.FastMCP = _MockFastMCP
    mcp_pkg.server = mcp_server
    mcp_server.fastmcp = mcp_fastmcp

    sys.modules.setdefault("mcp", mcp_pkg)
    sys.modules.setdefault("mcp.server", mcp_server)
    sys.modules.setdefault("mcp.server.fastmcp", mcp_fastmcp)


_inject_stub_mcp()

from plaita_ai.flow_runner import PathTraversalError, get_skill_text  # noqa: E402
from plaita_ai.mcp.server import flow_get_skill  # noqa: E402

import plaita_ai  # noqa: E402

SKILLS_ROOT = Path(plaita_ai.__file__).resolve().parent / "skills"
SENTINEL = "SENTINEL_SKILL_TEXT_248_DO_NOT_LEAK"


def _refusal_payload(output: str) -> dict:
    try:
        data = json.loads(output)
    except ValueError as exc:
        raise AssertionError(
            f"expected refusal payload, got raw output (content leak?): {output[:120]!r}"
        ) from exc
    assert data.get("ok") is False, data
    assert data.get("error_type") == "PathTraversalError", data
    return data


class TestSkillTextTraversalRefused(unittest.TestCase):
    def _assert_refused(self, name):
        with self.assertRaises(PathTraversalError) as ctx:
            get_skill_text(name)
        self.assertIn("skill_name", str(ctx.exception))
        return ctx.exception

    def test_parent_traversal_refused(self):
        self._assert_refused("../..")

    def test_deep_traversal_refused(self):
        self._assert_refused("../../" * 12 + "tmp")

    def test_absolute_path_refused(self):
        self._assert_refused("/etc")

    def test_non_bare_names_refused(self):
        for name in (
            "flow/coder",
            "flow.coder",
            "Flow-Coder",
            "flow_coder",
            "",
            ".",
            "..",
            "../flow-coder",
            None,
        ):
            with self.subTest(name=name):
                with self.assertRaises(PathTraversalError):
                    get_skill_text(name)

    def test_mcp_refuses_traversal_without_leaking_content(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "SKILL.md").write_text(SENTINEL, encoding="utf-8")
            # Old vulnerable fallback: plaita_ai/skills/<skill_name>/SKILL.md
            traversal = os.path.relpath(temp_dir, SKILLS_ROOT)
            self.assertIn("..", traversal)

            # MCP tool: named refusal JSON, no host content. (Negative control
            # must red here first: the old code returns the sentinel verbatim.)
            output = flow_get_skill(traversal)
            _refusal_payload(output)
            self.assertNotIn(SENTINEL, output)

            # Library function: named refusal, no content.
            with self.assertRaises(PathTraversalError):
                get_skill_text(traversal)

    def test_mcp_refuses_absolute_path(self):
        output = flow_get_skill("/etc")
        data = _refusal_payload(output)
        self.assertNotIn("root:", output)  # /etc/passwd would contain "root:"
        self.assertIn("PathTraversalError", data["error_type"])


class TestLegitimateSkillsUnchanged(unittest.TestCase):
    def test_default_flow_coder_verbatim(self):
        expected = (SKILLS_ROOT / "flow-coder" / "SKILL.md").read_text(encoding="utf-8")
        self.assertTrue(expected.strip())
        self.assertEqual(get_skill_text(), expected)
        self.assertEqual(get_skill_text("flow-coder"), expected)
        self.assertEqual(flow_get_skill(), expected)
        self.assertEqual(flow_get_skill("flow-coder"), expected)

    def test_all_bundled_skills_load_verbatim(self):
        skill_dirs = sorted(p for p in SKILLS_ROOT.iterdir() if (p / "SKILL.md").is_file())
        self.assertTrue(skill_dirs, "no bundled skills found")
        for skill_dir in skill_dirs:
            with self.subTest(skill=skill_dir.name):
                expected = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
                self.assertEqual(get_skill_text(skill_dir.name), expected)


if __name__ == "__main__":
    unittest.main()
