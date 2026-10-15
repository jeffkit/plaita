"""Security containment tests for flow_get_skill_reference (#54).

The MCP tool ``flow_get_skill_reference`` (and the library function
``get_skill_reference`` it wraps) used to concatenate caller-supplied
``skill_name``/``reference`` into a filesystem path with no containment:

- absolute paths (``/etc/passwd``) replaced the reference root entirely,
- ``..`` segments walked out of the references directory,
- symlinks planted inside the references directory escaped it.

These tests assert the NAMED refusal (``PathTraversalError`` /
``error_type: "PathTraversalError"`` refusal payload) and that legitimate
in-root references still resolve. Zero author-code involvement: containment
is checked from real filesystem state only.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path

# Ensure plaita-ai is importable (same layout as test_mcp_integration.py)
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

try:
    from plaita_ai.flow_runner import PathTraversalError
except ImportError:  # pre-fix tree (#54): the named error does not exist yet
    PathTraversalError = None  # type: ignore[assignment]
from plaita_ai.flow_runner import get_skill_reference  # noqa: E402
from plaita_ai.mcp.server import flow_get_skill_reference  # noqa: E402

import plaita_ai  # noqa: E402

REFERENCES_DIR = (
    Path(plaita_ai.__file__).resolve().parent / "skills" / "flow-coder" / "references"
)


def _assert_refusal_payload(output: str) -> dict:
    """Assert the MCP tool returned the named refusal, not file content."""
    try:
        data = json.loads(output)
    except ValueError as exc:
        raise AssertionError(
            f"expected refusal payload, got raw output (content leak?): {output[:120]!r}"
        ) from exc
    if not (isinstance(data, dict) and data.get("ok") is False):
        raise AssertionError(f"expected ok:false refusal, got: {output[:200]!r}")
    if data.get("error_type") != "PathTraversalError":
        raise AssertionError(
            f"expected named error_type PathTraversalError, got: {output[:200]!r}"
        )
    return data


class TestSkillReferenceContainment(unittest.TestCase):
    """#54: caller-supplied paths must stay inside the references root."""

    # --- bounce cases: traversal must be refused with the NAMED error ------

    def test_mcp_tool_refuses_absolute_path(self):
        output = flow_get_skill_reference(
            skill_name="flow-coder", reference="/etc/passwd"
        )
        data = _assert_refusal_payload(output)
        # refusal is a named message about containment, not the file content
        self.assertIn("escapes the references root", data["error"])

    def test_mcp_tool_refuses_relative_traversal(self):
        # depth well past the package root; clamps to <root-of-fs>/etc/hosts
        deep_reference = "../" * 16 + "etc/hosts"
        output = flow_get_skill_reference(
            skill_name="flow-coder", reference=deep_reference
        )
        _assert_refusal_payload(output)

    def test_mcp_tool_refuses_traversal_via_skill_name(self):
        # skill_name itself must not be able to move the containment root
        output = flow_get_skill_reference(
            skill_name="../../etc", reference="passwd"
        )
        _assert_refusal_payload(output)

    def test_mcp_tool_refuses_symlink_escape(self):
        """Symlink planted inside the references dir must not escape it."""
        outside_dir = tempfile.mkdtemp(prefix="b54-outside-")
        outside_file = Path(outside_dir) / "outside.txt"
        outside_file.write_text("B54-OUTSIDE-CONTENT", encoding="utf-8")
        link = REFERENCES_DIR / "_b54_escape_probe.md"
        os.symlink(str(outside_file), str(link))
        try:
            output = flow_get_skill_reference(
                skill_name="flow-coder", reference="_b54_escape_probe.md"
            )
            _assert_refusal_payload(output)
            self.assertNotIn("B54-OUTSIDE-CONTENT", output)
        finally:
            link.unlink()
            shutil.rmtree(outside_dir, ignore_errors=True)

    def test_library_raises_named_path_traversal_error(self):
        if PathTraversalError is None:
            self.fail(
                "PathTraversalError not implemented in plaita_ai.flow_runner "
                "(pre-fix tree: no named refusal exists)"
            )
        with self.assertRaises(PathTraversalError):
            get_skill_reference("flow-coder", "/etc/passwd")
        with self.assertRaises(PathTraversalError):
            get_skill_reference("flow-coder", "../" * 16 + "etc/hosts")
        with self.assertRaises(PathTraversalError):
            get_skill_reference("../..", "passwd")

    # --- legitimate use must stay green ------------------------------------

    def test_legit_reference_still_resolves(self):
        expected = (REFERENCES_DIR / "codeflow-reference.md").read_text(
            encoding="utf-8"
        )
        output = flow_get_skill_reference(
            skill_name="flow-coder", reference="codeflow-reference.md"
        )
        self.assertEqual(output, expected)

    def test_second_legit_reference_still_resolves(self):
        expected = (REFERENCES_DIR / "authoring-spec.md").read_text(
            encoding="utf-8"
        )
        output = get_skill_reference("flow-coder", "authoring-spec.md")
        self.assertEqual(output, expected)

    def test_nested_reference_inside_root_still_allowed(self):
        sub = REFERENCES_DIR / "_b54_sub"
        sub.mkdir(exist_ok=True)
        (sub / "inner.md").write_text("B54-INSIDE-CONTENT", encoding="utf-8")
        try:
            output = get_skill_reference("flow-coder", "_b54_sub/inner.md")
            self.assertEqual(output, "B54-INSIDE-CONTENT")
        finally:
            (sub / "inner.md").unlink()
            sub.rmdir()

    def test_missing_reference_still_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            get_skill_reference("flow-coder", "definitely-not-here.md")


if __name__ == "__main__":
    unittest.main()
