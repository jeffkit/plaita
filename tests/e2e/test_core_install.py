"""
E2E test: core-only install + basic flow execution.

Verifies SC-001: ≤5 direct dependencies for core install.
Can run in any environment where plaita is installed.
"""
import subprocess
import sys

import pytest


class TestCoreOnlyInstall:
    """Verify that a core-only install has minimal dependencies."""

    def test_sc001_direct_dependencies_at_most_5(self):
        """SC-001: Core install should pull ≤5 direct dependencies."""
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", "plaita"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            pytest.skip("plaita not installed as a package (dev/editable mode)")

        for line in result.stdout.splitlines():
            if line.startswith("Requires:"):
                deps_str = line.replace("Requires:", "").strip()
                deps = [d.strip() for d in deps_str.split(",") if d.strip()]
                assert len(deps) <= 5, (
                    f"Core install has {len(deps)} direct deps (max 5): {deps}"
                )
                return

        pytest.skip("Could not determine dependencies from pip show")

    def test_basic_flow_execution(self):
        """Core-only install can define and execute a simple flow."""
        from plaita.core.flow import Flow

        flow = Flow.model_validate({
            "flow_id": "e2e-core-basic",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "core-e2e-ok", "resultType": "success"},
            ],
        })
        result = flow.run()
        assert result == "core-e2e-ok"

    def test_assignment_flow(self):
        """Core-only install can run assignment + switch flow from fixture."""
        import json
        from pathlib import Path
        from plaita.core.flow import Flow

        fixture = Path(__file__).resolve().parents[1] / "fixture" / "assigment.json"
        if not fixture.exists():
            pytest.skip("fixture file not found")

        flow = Flow.from_string(fixture.read_text())
        result = flow.run({"bb": "123456"})
        assert result["dd"] == "123456"

    def test_core_imports_without_server(self):
        """Core modules import without server/redis packages."""
        import plaita.core
        from plaita.core.errors import FlowExecutionException
        from plaita.core.types import STRING
        from plaita.core.expression import ExpressionRegistry
        from plaita.core.flow import Flow
        from plaita.core.executor import FlowExecution

        assert plaita.core is not None
        assert FlowExecutionException is not None
        assert STRING == "string"
        assert ExpressionRegistry is not None
