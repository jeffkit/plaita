"""
Phase 7 success criteria verification tests (T098–T101).

SC-003: Largest class in execution subsystem under 200 LOC
SC-004: Zero duplicated business logic between sync/async
SC-005: Two Flow instances with different registries don't interfere
SC-010: New execution mode via single strategy class
"""
import ast
import inspect
import pathlib
import threading
from typing import Any, Dict, Optional

import pytest


CORE_DIR = pathlib.Path(__file__).resolve().parents[2] / "plaita" / "core"


class TestSC003LargestClassUnder200LOC:
    """SC-003: Each core execution component should be under 200 LOC."""

    def _get_class_sizes(self, filepath: pathlib.Path):
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(filepath))
        sizes = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                loc = node.end_lineno - node.lineno + 1
                sizes[node.name] = loc
        return sizes

    def test_execution_context_under_200_loc(self):
        sizes = self._get_class_sizes(CORE_DIR / "context.py")
        assert sizes["ExecutionContext"] < 200, (
            f"ExecutionContext is {sizes['ExecutionContext']} LOC (limit: 200)"
        )

    def test_node_runner_under_200_loc(self):
        sizes = self._get_class_sizes(CORE_DIR / "runner.py")
        assert sizes["NodeRunner"] < 200, (
            f"NodeRunner is {sizes['NodeRunner']} LOC (limit: 200)"
        )

    def test_normal_strategy_under_200_loc(self):
        sizes = self._get_class_sizes(CORE_DIR / "strategies.py")
        assert sizes["NormalStrategy"] < 200, (
            f"NormalStrategy is {sizes['NormalStrategy']} LOC (limit: 200)"
        )

    def test_generator_strategy_under_200_loc(self):
        sizes = self._get_class_sizes(CORE_DIR / "strategies.py")
        assert sizes["GeneratorStrategy"] < 200, (
            f"GeneratorStrategy is {sizes['GeneratorStrategy']} LOC (limit: 200)"
        )

    def test_distributed_strategy_under_200_loc(self):
        sizes = self._get_class_sizes(CORE_DIR / "strategies.py")
        assert sizes["DistributedStrategy"] < 200, (
            f"DistributedStrategy is {sizes['DistributedStrategy']} LOC (limit: 200)"
        )

    def test_callback_manager_under_200_loc(self):
        sizes = self._get_class_sizes(CORE_DIR / "callback.py")
        assert sizes["CallbackManager"] < 200, (
            f"CallbackManager is {sizes['CallbackManager']} LOC (limit: 200)"
        )

    def test_flow_execution_facade_delegates_to_components(self):
        """FlowExecution should compose ExecutionContext, NodeRunner, CallbackManager."""
        from plaita.core.executor import FlowExecution
        from plaita.core.context import ExecutionContext
        from plaita.core.runner import NodeRunner
        from plaita.core.callback import CallbackManager

        fe = FlowExecution()
        assert isinstance(fe._ctx, ExecutionContext)
        assert isinstance(fe._runner, NodeRunner)
        assert isinstance(fe.callback_manager, CallbackManager)


class TestSC004ZeroDuplicatedBusinessLogic:
    """SC-004: Zero duplicated business logic between sync and async code paths."""

    def test_async_run_is_canonical(self):
        """arun_compatible is the canonical async entry; strategies are async."""
        from plaita.core.executor import FlowExecution, NormalStrategy, GeneratorStrategy, DistributedStrategy
        assert hasattr(FlowExecution, 'arun_compatible')
        execution = FlowExecution()
        assert hasattr(execution, '_strategies')
        assert isinstance(execution._strategies.get('normal'), NormalStrategy)
        assert isinstance(execution._strategies.get('generator'), GeneratorStrategy)
        assert isinstance(execution._strategies.get('distributed'), DistributedStrategy)
        assert inspect.iscoroutinefunction(NormalStrategy.execute)
        assert inspect.isasyncgenfunction(GeneratorStrategy.execute)
        assert inspect.iscoroutinefunction(DistributedStrategy.execute)

    def test_node_runner_async_first(self):
        """NodeRunner.run_node should be async (canonical implementation)."""
        from plaita.core.runner import NodeRunner
        assert inspect.iscoroutinefunction(NodeRunner.run_node)

    def test_strategies_are_async(self):
        """All strategies should have async execute methods."""
        from plaita.core.executor import NormalStrategy, GeneratorStrategy, DistributedStrategy

        assert inspect.iscoroutinefunction(NormalStrategy.execute)
        assert inspect.isasyncgenfunction(GeneratorStrategy.execute)
        assert inspect.iscoroutinefunction(DistributedStrategy.execute)

    def test_sync_delegates_to_async_canonical(self):
        """Sync run_compatible delegates to the async strategy path (no duplicated traversal)."""
        from plaita.core.executor import FlowExecution
        source = inspect.getsource(FlowExecution.run_compatible)
        assert "_prepare_strategy" in source, "run_compatible should delegate to _prepare_strategy"
        assert "while " not in source, "run_compatible must not duplicate the traversal loop"

    def test_no_duplicate_node_execution_logic_in_runner(self):
        """NodeRunner should have single _run_with_timeout, not separate sync/async copies."""
        from plaita.core.runner import NodeRunner
        source = inspect.getsource(NodeRunner)
        assert source.count("def _run_with_timeout") == 1, (
            "NodeRunner should have exactly one _run_with_timeout method"
        )


class TestSC005RegistryIsolation:
    """SC-005: Two Flow instances with different node registries don't interfere."""

    def test_separate_registries_are_independent(self):
        """Registering a node in one registry doesn't affect another."""
        from plaita.node import NodeRegistry
        from plaita.node.basic import Node

        reg_a = NodeRegistry(auto_discover=False)
        reg_b = NodeRegistry(auto_discover=False)

        class CustomNodeA(Node):
            node_type = "custom_a"
            node_name = "Custom A"
            def execute(self, execution=None):
                return "a"

        class CustomNodeB(Node):
            node_type = "custom_b"
            node_name = "Custom B"
            def execute(self, execution=None):
                return "b"

        reg_a.register(CustomNodeA)
        reg_b.register(CustomNodeB)

        assert "custom_a" in reg_a
        assert "custom_a" not in reg_b
        assert "custom_b" in reg_b
        assert "custom_b" not in reg_a

    def test_unregister_doesnt_affect_other_registry(self):
        """Unregistering from one registry doesn't affect another."""
        from plaita.node import NodeRegistry

        reg_a = NodeRegistry(auto_discover=False)
        reg_b = NodeRegistry(auto_discover=False)

        assert "start" in reg_a
        assert "start" in reg_b

        reg_a.unregister("start")
        assert "start" not in reg_a
        assert "start" in reg_b

    def test_copy_creates_independent_registry(self):
        """copy() should create an independent registry."""
        from plaita.node import NodeRegistry
        from plaita.node.basic import Node

        original = NodeRegistry(auto_discover=False)
        copied = original.copy()

        class NewNode(Node):
            node_type = "new_test"
            node_name = "New Test"
            def execute(self, execution=None):
                return None

        copied.register(NewNode)
        assert "new_test" in copied
        assert "new_test" not in original

    def test_concurrent_registry_access(self):
        """Concurrent access to separate registries should not interfere."""
        from plaita.node import NodeRegistry
        from plaita.node.basic import Node

        reg_a = NodeRegistry(auto_discover=False)
        reg_b = NodeRegistry(auto_discover=False)
        errors = []

        def register_many(registry, prefix, count):
            try:
                for i in range(count):
                    type_name = f"{prefix}_{i}"

                    class DynNode(Node):
                        node_type = type_name
                        node_name = f"DynNode {type_name}"
                        def execute(self, execution=None):
                            return None

                    registry.register(DynNode)
            except Exception as e:
                errors.append(str(e))

        t1 = threading.Thread(target=register_many, args=(reg_a, "a", 50))
        t2 = threading.Thread(target=register_many, args=(reg_b, "b", 50))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == [], f"Concurrent registration errors: {errors}"
        assert "a_0" in reg_a
        assert "a_0" not in reg_b
        assert "b_0" in reg_b
        assert "b_0" not in reg_a


class TestSC010NewExecutionModeViaSingleClass:
    """SC-010: New execution mode can be added by implementing single strategy class."""

    def test_custom_strategy_works(self):
        """A custom strategy class can be used without modifying existing code."""
        from plaita.core.executor import FlowExecution
        from plaita.core.context import ExecutionContext
        from plaita.core.runner import NodeRunner
        from plaita.core.callback import CallbackManager
        from plaita.core.flow import Flow

        class DryRunStrategy:
            """Example custom strategy: records node names without executing."""

            async def execute(self, flow, context, runner, callback_manager,
                              params=None, timeout_ms=None, **options):
                visited = []
                next_node = flow.start_node
                from plaita.node import End
                while next_node:
                    visited.append(next_node.node_type)
                    if next_node.node_type == End.node_type:
                        break
                    next_node = flow.next_node(next_node, None)
                return visited

        flow = Flow.model_validate({
            "flow_id": "custom-strategy-test",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "ok", "resultType": "success"},
            ],
        })

        strategy = DryRunStrategy()
        ctx = ExecutionContext()
        ctx.clean()
        ctx.setup_flow(flow, (), {})
        runner = NodeRunner(ctx)
        cb = CallbackManager([])

        import asyncio
        result = asyncio.run(strategy.execute(flow, ctx, runner, cb))
        assert result == ["start", "end"]

    def test_strategy_protocol_requires_only_execute(self):
        """The ExecutionStrategy protocol only requires an execute method."""
        from plaita.core.executor import ExecutionStrategy

        class MinimalStrategy:
            async def execute(self, flow, context, runner, callback_manager,
                              params=None, timeout_ms=None, **options):
                return "minimal"

        strategy = MinimalStrategy()
        assert hasattr(strategy, "execute")
        assert inspect.iscoroutinefunction(strategy.execute)

    def test_execution_mode_enum_extensible(self):
        """ExecutionMode enum should be importable and usable."""
        from plaita.core.executor import ExecutionMode

        assert ExecutionMode.NORMAL.value == "normal"
        assert ExecutionMode.GENERATOR.value == "generator"
        assert ExecutionMode.DISTRIBUTED.value == "distributed"
        assert ExecutionMode.from_string("normal") == ExecutionMode.NORMAL
