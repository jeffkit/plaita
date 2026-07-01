"""
T105: Quickstart.md validation.

Execute all code examples from quickstart.md and verify they work.
"""
import warnings

import pytest


class TestQuickstartBasicUsage:
    """Validate basic usage examples from quickstart.md."""

    def test_define_and_run_flow(self):
        """quickstart: Define and run a flow — adapted for actual Assignment node API."""
        from plaita.core.flow import Flow

        flow_def = {
            "flow_id": "hello-world",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }

        flow = Flow.model_validate(flow_def)
        result = flow.run(name="Alice")
        assert result == "Alice", f"Expected 'Alice', got {result}"

    @pytest.mark.asyncio
    async def test_async_execution(self):
        """quickstart: Async execution."""
        from plaita.core.flow import Flow

        flow_def = {
            "flow_id": "async-hello",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }

        flow = Flow.model_validate(flow_def)
        result = await flow.arun(name="AsyncAlice")
        assert result == "AsyncAlice"


class TestQuickstartScopedRegistry:
    """Validate scoped node registry examples from quickstart.md."""

    def test_isolated_registry(self):
        """quickstart: Create isolated registry."""
        from plaita.node import NodeRegistry

        reg = NodeRegistry(auto_discover=False)
        assert "start" in reg
        assert "end" in reg

    def test_registry_register_custom_node(self):
        """quickstart: Register custom node."""
        from plaita.node import NodeRegistry
        from plaita.node.basic import Node

        class MockNode(Node):
            node_type = "mock_qs"
            node_name = "Mock"
            def execute(self, execution=None):
                return {"mocked": True}

        registry = NodeRegistry(auto_discover=False)
        registry.register(MockNode)
        assert "mock_qs" in registry


class TestQuickstartExpressions:
    """Validate expression function examples from quickstart.md."""

    def test_query_by_category(self):
        """quickstart: Query expression functions by category."""
        from plaita.core.expression import get_default_expression_registry, FunctionCategory

        reg = get_default_expression_registry()
        string_funcs = list(reg.by_category(FunctionCategory.STRING))
        assert len(string_funcs) > 0, "Should have string functions"

    def test_side_effect_functions(self):
        """quickstart: List side-effect functions."""
        from plaita.core.expression import get_default_expression_registry

        reg = get_default_expression_registry()
        se_funcs = list(reg.side_effect_functions())
        assert len(se_funcs) > 0, "Should have side-effect functions"

    def test_register_custom_function(self):
        """quickstart: Register custom expression function."""
        from plaita.core.expression import ExpressionRegistry, FunctionCategory

        reg = ExpressionRegistry()
        reg.register(
            "qs_test_upper_first",
            lambda s: s[0].upper() + s[1:] if s else s,
            FunctionCategory.STRING,
            description="Uppercase first character",
        )
        assert "qs_test_upper_first" in reg


class TestQuickstartMigration:
    """Validate migration (backward compat) examples from quickstart.md."""

    def test_old_import_plaita_flow(self):
        """quickstart: Old import path plaita.flow should still work."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from plaita.flow import Flow, FlowExecution, parse
        assert Flow is not None
        assert FlowExecution is not None
        assert parse is not None

    def test_old_import_plaita_io(self):
        """quickstart: Old import path plaita.io should still work."""
        from plaita.io import REGISTERED_FUNCTIONS, evaluate
        assert REGISTERED_FUNCTIONS is not None
        assert evaluate is not None

    def test_old_import_plaita_node(self):
        """quickstart: Old import path plaita.node should still work."""
        from plaita.node import nodes, get_default_registry
        assert nodes is not None
        assert get_default_registry is not None

    def test_old_node_register(self):
        """quickstart: Old node_register function should work with deprecation."""
        from plaita.node import node_register
        from plaita.node.basic import Node

        class QSTestNode(Node):
            node_type = "qs_test_old"
            node_name = "QS Test"
            def execute(self, execution=None):
                return None

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            node_register(QSTestNode)

        from plaita.node import get_default_registry
        assert "qs_test_old" in get_default_registry()

    def test_new_import_paths(self):
        """quickstart: New recommended import paths should work."""
        from plaita.core.flow import Flow
        from plaita.core.executor import FlowExecution
        from plaita.core.flow import parse
        from plaita.core.expression import get_default_expression_registry
        from plaita.node import NodeRegistry, get_default_registry

        assert Flow is not None
        assert FlowExecution is not None
        assert parse is not None
        assert get_default_expression_registry is not None
        assert NodeRegistry is not None
        assert get_default_registry is not None


class TestQuickstartRunningTests:
    """Validate that test commands from quickstart.md work."""

    def test_tests_directory_exists(self):
        """quickstart: tests/ directory should exist."""
        from pathlib import Path
        tests_dir = Path(__file__).resolve().parents[1]
        assert tests_dir.exists()
        assert (tests_dir / "unit").exists()
        assert (tests_dir / "integration").exists()
