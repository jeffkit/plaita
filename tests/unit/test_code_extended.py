"""Extended tests for plaita/node/code.py — covers uncovered branches.

Target lines: 71-72, 78-79, 94, 100-102, 184, 189-190, 208, 253, 287-296,
307, 314, 329, 370-371, 380, 446-453, 461-475, 499, 551, 561-563, 574, 583.
"""
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

import plaita.node.code as code_module
from plaita.node.code import (
    _decode_runner_output,
    _docker_available,
    _inplace_var,
    _make_safe_import,
    _require_execjs,
    register_runner,
    run_python_restricted,
    validate_run_function,
)


# ---------------------------------------------------------------------------
# execjs import fallback (lines 71-72)
# ---------------------------------------------------------------------------

class TestExecjsImportFallback(unittest.TestCase):
    def test_require_execjs_raises_when_not_installed(self):
        """Line 208: _require_execjs raises ImportError when execjs is None."""
        with patch.object(code_module, "execjs", None):
            with self.assertRaises(ImportError) as ctx:
                _require_execjs()
        self.assertIn("PyExecJS", str(ctx.exception))


# ---------------------------------------------------------------------------
# _docker_available (lines 94, 100-102)
# ---------------------------------------------------------------------------

class TestDockerAvailable(unittest.TestCase):
    def test_returns_false_when_docker_not_in_path(self):
        """Line 94: docker binary not found → returns False."""
        with patch("shutil.which", return_value=None):
            self.assertFalse(_docker_available())

    def test_returns_false_when_docker_returncode_nonzero(self):
        """Line 94 + subprocess path: docker info fails → returns False."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        with patch("shutil.which", return_value="/usr/bin/docker"), \
             patch("subprocess.run", return_value=mock_proc):
            self.assertFalse(_docker_available())

    def test_returns_false_when_subprocess_raises(self):
        """Lines 100-102: subprocess.run raises (e.g. timeout) → returns False."""
        with patch("shutil.which", return_value="/usr/bin/docker"), \
             patch("subprocess.run", side_effect=Exception("daemon error")):
            self.assertFalse(_docker_available())

    def test_returns_true_when_docker_available(self):
        """Happy path: docker returns 0 → True."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        with patch("shutil.which", return_value="/usr/bin/docker"), \
             patch("subprocess.run", return_value=mock_proc):
            self.assertTrue(_docker_available())


# ---------------------------------------------------------------------------
# _decode_runner_output (lines 184, 189-190)
# ---------------------------------------------------------------------------

class TestDecodeRunnerOutput(unittest.TestCase):
    def test_raises_on_empty_output(self):
        """Line 184: empty stdout → RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            _decode_runner_output("   ", "some stderr")
        self.assertIn("no output", str(ctx.exception))

    def test_raises_on_invalid_json(self):
        """Lines 189-190: non-JSON stdout → RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            _decode_runner_output("not-json", "")
        self.assertIn("not valid JSON", str(ctx.exception))

    def test_raises_on_error_envelope(self):
        """Line 195-198: envelope with ok=False → RuntimeError with type:msg."""
        raw = json.dumps({"ok": False, "error": "ZeroDivisionError", "type": "ZeroDivisionError"})
        with self.assertRaises(RuntimeError) as ctx:
            _decode_runner_output(raw, "")
        self.assertIn("ZeroDivisionError", str(ctx.exception))

    def test_success_envelope_returns_result(self):
        """Happy path: ok=True → returns result."""
        raw = json.dumps({"ok": True, "result": 42})
        self.assertEqual(_decode_runner_output(raw, ""), 42)


# ---------------------------------------------------------------------------
# validate_run_function (line 253)
# ---------------------------------------------------------------------------

class TestValidateRunFunction(unittest.TestCase):
    def test_raises_when_no_run_function(self):
        """Line 253: code without 'run' function → ValueError."""
        code = "x = 1\ny = 2\n"
        with self.assertRaises(ValueError) as ctx:
            validate_run_function(code, {})
        self.assertIn("No run function found", str(ctx.exception))

    def test_raises_with_invalid_kwargs(self):
        """Line 258: kwargs not in function args → ValueError."""
        code = "def run(x): return x\n"
        with self.assertRaises(ValueError) as ctx:
            validate_run_function(code, {"z": 1})
        self.assertIn("Invalid arguments", str(ctx.exception))

    def test_passes_with_valid_function(self):
        """Happy path: 'run' function present, kwargs match → no exception."""
        code = "def run(x, y): return x + y\n"
        validate_run_function(code, {"x": 1, "y": 2})  # should not raise


# ---------------------------------------------------------------------------
# _inplace_var (lines 287-296)
# ---------------------------------------------------------------------------

class TestInplaceVar(unittest.TestCase):
    def test_add(self):
        self.assertEqual(_inplace_var("+=", 3, 2), 5)

    def test_sub(self):
        self.assertEqual(_inplace_var("-=", 10, 4), 6)

    def test_mul(self):
        self.assertEqual(_inplace_var("*=", 3, 3), 9)

    def test_truediv(self):
        self.assertAlmostEqual(_inplace_var("/=", 10, 4), 2.5)

    def test_floordiv(self):
        self.assertEqual(_inplace_var("//=", 10, 3), 3)

    def test_mod(self):
        self.assertEqual(_inplace_var("%=", 10, 3), 1)

    def test_pow(self):
        self.assertEqual(_inplace_var("**=", 2, 8), 256)

    def test_and_(self):
        self.assertEqual(_inplace_var("&=", 0b1100, 0b1010), 0b1000)

    def test_or_(self):
        self.assertEqual(_inplace_var("|=", 0b1100, 0b1010), 0b1110)

    def test_xor(self):
        self.assertEqual(_inplace_var("^=", 0b1100, 0b1010), 0b0110)

    def test_lshift(self):
        self.assertEqual(_inplace_var("<<=", 1, 3), 8)

    def test_rshift(self):
        self.assertEqual(_inplace_var(">>=", 8, 2), 2)

    def test_unknown_operator_raises(self):
        """Line 295: unsupported operator → TypeError."""
        with self.assertRaises(TypeError) as ctx:
            _inplace_var("@=", 1, 2)
        self.assertIn("Unsupported", str(ctx.exception))


# ---------------------------------------------------------------------------
# run_python_restricted (lines 307, 314, 329)
# ---------------------------------------------------------------------------

class TestRunPythonRestricted(unittest.TestCase):
    def test_raises_import_error_when_not_available(self):
        """Line 307: _RESTRICTED_AVAILABLE=False → ImportError."""
        with patch.object(code_module, "_RESTRICTED_AVAILABLE", False):
            with self.assertRaises(ImportError) as ctx:
                run_python_restricted("def run(x): return x", {})
        self.assertIn("RestrictedPython", str(ctx.exception))

    def test_extra_modules_extends_allowlist(self):
        """Line 314: extra_modules extends the allowlist."""
        code = "import math\ndef run(x): return math.floor(x)\n"
        result = run_python_restricted(code, 3.7, extra_modules=frozenset(["math"]))
        self.assertEqual(result, 3)

    def test_raises_when_no_run_function(self):
        """Line 329: code with no 'run' function → ValueError."""
        code = "x = 1\n"
        with self.assertRaises(ValueError) as ctx:
            run_python_restricted(code, {})
        self.assertIn("run", str(ctx.exception))


# ---------------------------------------------------------------------------
# run_python_subprocess (lines 370-371, 380)
# ---------------------------------------------------------------------------

class TestRunPythonSubprocess(unittest.TestCase):
    def test_timeout_raises_runtime_error(self):
        """Lines 370-371: subprocess timeout → RuntimeError."""
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(
            cmd="python", timeout=10
        )):
            from plaita.node.code import run_python_subprocess
            with self.assertRaises(RuntimeError) as ctx:
                run_python_subprocess("def run(x): return x", {})
        self.assertIn("timed out", str(ctx.exception))

    def test_nonzero_exit_with_empty_stdout_raises(self):
        """Line 380: non-zero returncode and empty stdout → RuntimeError."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = b""
        mock_proc.stderr = b"SomeError"
        with patch("subprocess.run", return_value=mock_proc):
            from plaita.node.code import run_python_subprocess
            with self.assertRaises(RuntimeError) as ctx:
                run_python_subprocess("def run(x): return x", {})
        self.assertIn("Subprocess exited", str(ctx.exception))


# ---------------------------------------------------------------------------
# run_python_docker error paths (lines 446-453, 461-475)
# ---------------------------------------------------------------------------

class TestRunPythonDockerErrors(unittest.TestCase):
    def test_file_not_found_error_raises_docker_missing_message(self):
        """Lines 446-451: FileNotFoundError → RuntimeError about Docker not installed."""
        with patch("subprocess.run", side_effect=FileNotFoundError("docker not found")):
            from plaita.node.code import run_python_docker
            with self.assertRaises(RuntimeError) as ctx:
                run_python_docker("def run(x): return x", {})
        self.assertIn("Docker is not installed", str(ctx.exception))

    def test_timeout_raises_runtime_error(self):
        """Lines 452-455: docker subprocess timeout → RuntimeError."""
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(
            cmd="docker", timeout=30
        )):
            from plaita.node.code import run_python_docker
            with self.assertRaises(RuntimeError) as ctx:
                run_python_docker("def run(x): return x", {})
        self.assertIn("timed out", str(ctx.exception))

    def test_daemon_not_running_detected(self):
        """Lines 469-474: stderr contains daemon-down signal → specific message."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = b""
        mock_proc.stderr = b"Cannot connect to the Docker daemon is the docker daemon running"
        with patch("subprocess.run", return_value=mock_proc):
            from plaita.node.code import run_python_docker
            with self.assertRaises(RuntimeError) as ctx:
                run_python_docker("def run(x): return x", {})
        self.assertIn("Docker daemon is not running", str(ctx.exception))

    def test_generic_nonzero_exit(self):
        """Line 475-478: non-zero exit without daemon signal → generic message."""
        mock_proc = MagicMock()
        mock_proc.returncode = 2
        mock_proc.stdout = b""
        mock_proc.stderr = b"some other error"
        with patch("subprocess.run", return_value=mock_proc):
            from plaita.node.code import run_python_docker
            with self.assertRaises(RuntimeError) as ctx:
                run_python_docker("def run(x): return x", {})
        self.assertIn("Docker container exited", str(ctx.exception))


# ---------------------------------------------------------------------------
# register_runner (line 499)
# ---------------------------------------------------------------------------

class TestRegisterRunner(unittest.TestCase):
    def test_register_and_use_custom_runner(self):
        """Line 499: register_runner adds a language runner."""
        def my_runner(code, input_value):
            return f"custom:{input_value}"

        register_runner("custom_lang", my_runner)
        from plaita.node.code import Runners
        self.assertIn("custom_lang", Runners)
        self.assertEqual(Runners["custom_lang"]("code", 42), "custom:42")
        del Runners["custom_lang"]  # cleanup


# ---------------------------------------------------------------------------
# CodeNode validator (lines 551, 561-563)
# ---------------------------------------------------------------------------

class TestCodeNodeValidator(unittest.TestCase):
    def test_missing_code_raises(self):
        """Line 551: language=python but no code → ValueError."""
        from plaita.node.code import CodeNode
        with self.assertRaises(Exception) as ctx:
            CodeNode.model_validate({
                "id": "c", "type": "code",
                "language": "python",
                "sandbox_backend": "unsafe",
                # code intentionally missing
            })
        self.assertIn("code", str(ctx.exception).lower())

    def test_invalid_python_syntax_raises(self):
        """Lines 561-563: code with syntax error → validator ValueError."""
        from plaita.node.code import CodeNode
        with self.assertRaises(Exception):
            CodeNode.model_validate({
                "id": "c", "type": "code",
                "language": "python",
                "code": "def broken(: invalid syntax",
                "sandbox_backend": "unsafe",
            })


# ---------------------------------------------------------------------------
# CodeNode.execute error paths (lines 574, 583)
# ---------------------------------------------------------------------------

class TestCodeNodeExecute(unittest.TestCase):
    def _make_exec(self, language: str, code: str, sandbox_backend: str = "unsafe"):
        from plaita.node.code import CodeNode
        node = CodeNode.model_validate({
            "id": "c",
            "language": language,
            "code": code,
            "sandbox_backend": sandbox_backend,
        })
        execution = MagicMock()
        execution.evaluate.side_effect = lambda x: x
        return node, execution

    def test_execute_unknown_backend_raises(self):
        """Line 574: unknown sandbox_backend → ValueError."""
        node, execution = self._make_exec("python", "def run(x): return x")
        node.sandbox_backend = "unknown_backend"
        with self.assertRaises(ValueError) as ctx:
            node.execute(execution)
        self.assertIn("Unknown sandbox_backend", str(ctx.exception))

    def test_execute_unsupported_language_raises(self):
        """Line 583: unsupported language (not in Runners dict) → ValueError."""
        from plaita.node.code import CodeNode
        node = CodeNode.model_validate({
            "id": "c",
            "language": "python",
            "code": "def run(x): return x",
            "sandbox_backend": "unsafe",
        })
        execution = MagicMock()
        execution.evaluate.side_effect = lambda x: x
        node.language = "ruby"  # not in Runners
        with self.assertRaises(ValueError) as ctx:
            node.execute(execution)
        self.assertIn("Unsupported language", str(ctx.exception))

    def test_execute_python_unsafe_backend_end_to_end(self):
        """Happy path: unsafe backend runs code end-to-end via Flow.run."""
        from plaita.node import register_code_node
        register_code_node(default_backend="unsafe")  # opt in CodeNode for test

        from plaita.core.flow import Flow
        import json
        flow = Flow.from_string(json.dumps({
            "id": "f",
            "version": "1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "c"},
                {
                    "type": "code",
                    "id": "c",
                    "language": "python",
                    "code": "def run(input): return input['value'] * 2",
                    "input": "$INPUT",
                    "sandbox_backend": "unsafe",
                    "next": "e",
                },
                {"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"},
            ],
        }))
        result = flow.run(value=21)
        self.assertEqual(result, 42)


if __name__ == "__main__":
    unittest.main()
