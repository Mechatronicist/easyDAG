"""
Unit tests for the EasyDAG multiprocessing DAG executor.

Run with:
    pytest test_easydag.py -v
    pytest test_easydag.py -v --timeout=30  # with pytest-timeout installed
"""

import os
import pickle
import tempfile
import time
from multiprocessing import Manager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.EasyDAG import DAGNode, EasyDAG, EasyInterface, MultiprocessQueue, QueueMessage
from src.EasyDAG.dag_types import DAGQueue, NodeError, NodeJob, NodeJobResult
from src.EasyDAG.node import _node_worker


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _add(a, b):
    return a + b

def _identity(x):
    return x

def _double(x):
    return x * 2

def _sum_inputs(inputs=None, **kwargs):
    """Sum all dependency outputs."""
    if inputs is None:
        inputs = {}
    return sum(inputs.values())

def _fail():
    raise ValueError("Intentional failure")

def _sleep_then_return(seconds, value):
    time.sleep(seconds)
    return value

def _send_message(message_queue=None):
    if message_queue:
        message_queue.put(QueueMessage(type="test_msg", payload="hello"))
    return "done"

def _return_constant(*args):
    return 42

def _return_five(*args):
    return 5

def _return_ten(*args):
    return 10

def _takes_inputs(a=None, b=None, inputs=None):
    return (a or 0) + (b or 0)

def add_static(a, b):
    return a + b

def greet(name):
    return f"Hello, {name}!"

manager = Manager()
call_count = manager.list([0]) # This is now a shared proxy object

def flaky():
    call_count[0] += 1
    if call_count[0] < 3:
        raise ValueError("Not yet")
    return "success"

def counted():
    call_count[0] += 1
    return "cached_value"

def slow():
    time.sleep(5)
    return "done"

def node_a():
    return 10


def node_b(a=None):
    return a + 1


def node_c(a=None):
    return a + 2


def node_d(b=None, c=None):
    return b + c

def returns_none():
    return None

def big_list():
    return list(range(10_000))

# ---------------------------------------------------------------------------
# DAGNode tests
# ---------------------------------------------------------------------------

class TestDAGNode:
    def test_basic_creation(self):
        node = DAGNode("my_node", _add, args=(1, 2))
        assert node.id == "my_node"
        assert node.func is _add
        assert node.args == (1, 2)
        assert node.kwargs == {}
        assert node.max_retries == 0

    def test_creation_with_kwargs(self):
        node = DAGNode("n", _add, kwargs={"a": 1, "b": 2})
        assert node.kwargs == {"a": 1, "b": 2}

    def test_creation_with_max_retries(self):
        node = DAGNode("n", _add, max_retries=3)
        assert node.max_retries == 3

    def test_repr(self):
        node = DAGNode("my_node", _add)
        assert "my_node" in repr(node)

    def test_default_args_are_empty(self):
        node = DAGNode("n", _add)
        assert node.args == ()
        assert node.kwargs == {}


# ---------------------------------------------------------------------------
# EasyDAG structural tests (no execution)
# ---------------------------------------------------------------------------

class TestEasyDAGStructure:
    def setup_method(self):
        self.dag = EasyDAG(processes=2)

    def test_add_node(self):
        node = DAGNode("A", _add)
        self.dag.add_node(node)
        assert "A" in self.dag.nodes

    def test_duplicate_node_raises(self):
        self.dag.add_node(DAGNode("A", _add))
        with pytest.raises(ValueError, match="already exists"):
            self.dag.add_node(DAGNode("A", _add))

    def test_add_edge(self):
        self.dag.add_node(DAGNode("A", _add))
        self.dag.add_node(DAGNode("B", _add))
        self.dag.add_edge("A", "B")
        assert "B" in self.dag.adj["A"]
        assert "A" in self.dag.rev_adj["B"]

    def test_add_edge_unknown_from_node_raises(self):
        self.dag.add_node(DAGNode("B", _add))
        with pytest.raises(KeyError):
            self.dag.add_edge("MISSING", "B")

    def test_add_edge_unknown_to_node_raises(self):
        self.dag.add_node(DAGNode("A", _add))
        with pytest.raises(KeyError):
            self.dag.add_edge("A", "MISSING")

    def test_self_loop_raises(self):
        self.dag.add_node(DAGNode("A", _add))
        with pytest.raises(ValueError, match="Self-loop"):
            self.dag.add_edge("A", "A")

    def test_duplicate_edge_raises(self):
        self.dag.add_node(DAGNode("A", _add))
        self.dag.add_node(DAGNode("B", _add))
        self.dag.add_edge("A", "B")
        with pytest.raises(ValueError, match="Duplicate edge"):
            self.dag.add_edge("A", "B")

    def test_cycle_detected_on_run(self):
        self.dag.add_node(DAGNode("A", _add))
        self.dag.add_node(DAGNode("B", _add))
        self.dag.add_edge("A", "B")
        # Manually inject reverse edge to create cycle
        self.dag.adj["B"].append("A")
        self.dag.rev_adj["A"].append("B")
        with pytest.raises(ValueError, match="cycle"):
            self.dag.run()

    def test_empty_dag_returns_empty(self):
        result = self.dag.run()
        assert result == {}

    def test_to_graphviz(self):
        self.dag.add_node(DAGNode("A", _add))
        self.dag.add_node(DAGNode("B", _add))
        self.dag.add_edge("A", "B")
        gv = self.dag.to_graphviz()
        assert "digraph DAG" in gv
        assert '"A"' in gv
        assert '"B"' in gv
        assert '"A" -> "B"' in gv

    def test_descendants(self):
        for nid in ["A", "B", "C", "D"]:
            self.dag.add_node(DAGNode(nid, _add))
        self.dag.add_edge("A", "B")
        self.dag.add_edge("B", "C")
        self.dag.add_edge("A", "D")
        desc = self.dag._descendants("A")
        assert desc == {"B", "C", "D"}

    def test_descendants_leaf_node(self):
        self.dag.add_node(DAGNode("A", _add))
        assert self.dag._descendants("A") == set()


# ---------------------------------------------------------------------------
# EasyDAG execution tests
# ---------------------------------------------------------------------------

class TestEasyDAGExecution:
    def setup_method(self):
        self.dag = EasyDAG(processes=2)

    def test_single_node(self):
        self.dag.add_node(DAGNode("A", _return_constant))
        result = self.dag.run()
        assert result == {"A": 42}

    def test_linear_chain(self):
        """A -> B -> C where each node depends on the previous output."""
        self.dag.add_node(DAGNode("A", _return_five))
        self.dag.add_node(DAGNode("B", _double))
        self.dag.add_node(DAGNode("C", _double))
        self.dag.add_edge("A", "B")
        self.dag.add_edge("B", "C")
        result = self.dag.run()
        assert "A" in result
        assert "B" in result
        assert "C" in result
        assert result["C"] == 20

    def test_parallel_independent_nodes(self):
        """A and B have no dependency — should run in parallel."""
        self.dag.add_node(DAGNode("A", _return_five))
        self.dag.add_node(DAGNode("B", _return_ten))
        result = self.dag.run()
        assert result["A"] == 5
        assert result["B"] == 10

    def test_fan_out(self):
        """A -> B and A -> C."""
        self.dag.add_node(DAGNode("A", _return_five))
        self.dag.add_node(DAGNode("B", _return_ten))
        self.dag.add_node(DAGNode("C", _return_constant))
        self.dag.add_edge("A", "B")
        self.dag.add_edge("A", "C")
        result = self.dag.run()
        assert "A" in result
        assert "B" in result
        assert "C" in result

    def test_fan_in(self):
        """B and C -> D."""
        self.dag.add_node(DAGNode("B", _return_five))
        self.dag.add_node(DAGNode("C", _return_ten))
        self.dag.add_node(DAGNode("D", _return_constant))
        self.dag.add_edge("B", "D")
        self.dag.add_edge("C", "D")
        result = self.dag.run()
        assert "D" in result

    def test_node_with_args(self):
        self.dag.add_node(DAGNode("A", add_static, args=(3, 4)))
        result = self.dag.run()
        assert result["A"] == 7

    def test_node_with_kwargs(self):
        self.dag.add_node(DAGNode("A", greet, kwargs={"name": "World"}))
        result = self.dag.run()
        assert result["A"] == "Hello, World!"

    def test_node_failure_raises_runtime_error(self):
        self.dag.add_node(DAGNode("A", _fail))
        with pytest.raises(RuntimeError):
            self.dag.run()

    def test_fail_fast_stops_execution(self):
        """When fail_fast=True and a root node fails, descendants shouldn't run."""
        executed = []

        def track_execution():
            executed.append("downstream")
            return "ran"

        dag = EasyDAG(processes=2, fail_fast=True)
        dag.add_node(DAGNode("A", _fail))
        dag.add_node(DAGNode("B", track_execution))
        dag.add_edge("A", "B")

        with pytest.raises(RuntimeError):
            dag.run()

        # B should not have run since A failed and fail_fast=True
        assert "downstream" not in executed

    def test_node_retry_on_failure(self):
        """A node with max_retries=2 should retry before raising."""
        self.dag.add_node(DAGNode("A", flaky, max_retries=2))
        call_count[0] = 0
        result = self.dag.run()
        assert result["A"] == "success"
        assert call_count[0] == 3

    def test_node_retry_exhausted_raises(self):
        """A node that always fails should raise after exhausting retries."""
        self.dag.add_node(DAGNode("A", _fail, max_retries=1))
        with pytest.raises(RuntimeError):
            self.dag.run()

    def test_progress_callback(self):
        updates = []

        def cb(completed, total, node_id):
            updates.append((completed, total, node_id))

        self.dag.add_node(DAGNode("A", _return_five))
        self.dag.add_node(DAGNode("B", _return_ten))
        self.dag.run(progress_callback=cb)
        assert len(updates) == 2
        totals = {u[1] for u in updates}
        assert totals == {2}

    def test_dag_timeout(self):
        self.dag.add_node(DAGNode("A", slow))
        with pytest.raises(TimeoutError):
            self.dag.run(dag_timeout=0.5)


# ---------------------------------------------------------------------------
# Caching tests
# ---------------------------------------------------------------------------

class TestCaching:
    def test_cache_stores_and_reloads(self):
        call_count[0] = 0
        with tempfile.TemporaryDirectory() as cache_dir:
            dag1 = EasyDAG(processes=1)
            dag1.add_node(DAGNode("A", counted))
            result1 = dag1.run(cache_dir=cache_dir)
            assert result1["A"] == "cached_value"
            assert call_count[0] == 1

            dag2 = EasyDAG(processes=1)
            dag2.add_node(DAGNode("A", counted))
            result2 = dag2.run(cache_dir=cache_dir)
            assert result2["A"] == "cached_value"
            # Function should NOT have been called again
            assert call_count[0] == 1

    def test_corrupted_cache_is_rebuilt(self):
        with tempfile.TemporaryDirectory() as cache_dir:
            cache_file = Path(cache_dir) / "A.pkl"
            cache_file.write_bytes(b"not valid pickle data")

            dag = EasyDAG(processes=1)
            dag.add_node(DAGNode("A", _return_constant))
            result = dag.run(cache_dir=cache_dir)
            assert result["A"] == 42

    def test_cache_dir_is_created_if_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "new_subdir", "cache")
            dag = EasyDAG(processes=1)
            dag.add_node(DAGNode("A", _return_constant))
            dag.run(cache_dir=cache_dir)
            assert Path(cache_dir).exists()

    def test_get_cached_returns_none_when_no_cache(self):
        result = EasyDAG._get_cached("A", None)
        assert result is None

    def test_get_cached_returns_none_when_file_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = EasyDAG._get_cached("A", Path(tmp))
            assert result is None

    def test_save_to_cache_noop_when_no_path(self):
        # Should not raise
        EasyDAG._save_to_cache("A", 42, None)


# ---------------------------------------------------------------------------
# load_dag_from_spec tests
# ---------------------------------------------------------------------------

class TestLoadFromSpec:
    def test_basic_spec_loading(self):
        spec = [
            {"id": "A", "func": "ret_five"},
            {"id": "B", "func": "ret_ten", "parents": ["A"]},
        ]
        lookup = {"ret_five": _return_five, "ret_ten": _return_ten}
        dag = EasyDAG(processes=1)
        dag.load_dag_from_spec(spec, lookup)
        assert "A" in dag.nodes
        assert "B" in dag.nodes
        assert "B" in dag.adj["A"]

    def test_spec_with_args_and_kwargs(self):
        spec = [
            {"id": "A", "func": "add", "args": [1, 2], "kwargs": {}},
        ]
        lookup = {"add": _add}
        dag = EasyDAG(processes=1)
        dag.load_dag_from_spec(spec, lookup)
        result = dag.run()
        assert result["A"] == 3

    def test_spec_with_max_retries(self):
        spec = [{"id": "A", "func": "add", "max_retries": 3}]
        lookup = {"add": _add}
        dag = EasyDAG(processes=1)
        dag.load_dag_from_spec(spec, lookup)
        assert dag.nodes["A"].max_retries == 3


# ---------------------------------------------------------------------------
# _node_worker unit tests
# ---------------------------------------------------------------------------

class TestNodeWorker:
    def _make_job(self, func, args=(), kwargs=None, resolved_inputs=None):
        return NodeJob(
            node_id="test_node",
            func=func,
            args=args,
            kwargs=kwargs or {},
            resolved_inputs=resolved_inputs or {},
            message_queue=None,
            cancelled_nodes=None,
            node_pids=None,
        )

    def test_successful_execution(self):
        job = self._make_job(_return_constant)
        result = _node_worker(job)
        assert result.node_id == "test_node"
        assert result.result == 42
        assert result.error_info is None
        assert not result.cancelled

    def test_error_returns_error_info(self):
        job = self._make_job(_fail)
        result = _node_worker(job)
        assert result.error_info is not None
        assert "Intentional failure" in result.error_info.exception
        assert result.result is None

    def test_cancelled_node_returns_cancelled_result(self):
        cancelled = {"test_node": "Test cancel reason"}
        job = NodeJob(
            node_id="test_node",
            func=_return_constant,
            args=(),
            kwargs={},
            resolved_inputs={},
            message_queue=None,
            cancelled_nodes=cancelled,
            node_pids=None,
        )
        result = _node_worker(job)
        assert result.cancelled is True
        assert result.cancel_reason == "Test cancel reason"

    def test_node_registers_and_unregisters_pid(self):
        node_pids = {}
        job = NodeJob(
            node_id="test_node",
            func=_return_constant,
            args=(),
            kwargs={},
            resolved_inputs={},
            message_queue=None,
            cancelled_nodes=None,
            node_pids=node_pids,
        )
        _node_worker(job)
        # PID should be cleaned up after completion
        assert "test_node" not in node_pids

    def test_inputs_passed_to_function(self):
        def takes_dep(dep=None):
            return dep * 3

        job = NodeJob(
            node_id="test_node",
            func=takes_dep,
            args=(),
            kwargs={},
            resolved_inputs={"dep": 7},
            message_queue=None,
            cancelled_nodes=None,
            node_pids=None,
        )
        result = _node_worker(job)
        assert result.result == 21

    def test_message_queue_passed_if_param_exists(self):
        received = []

        def func_with_queue(message_queue=None):
            if message_queue:
                received.append("got_queue")
            return "ok"

        mock_queue = MagicMock()
        job = NodeJob(
            node_id="test_node",
            func=func_with_queue,
            args=(),
            kwargs={},
            resolved_inputs={},
            message_queue=mock_queue,
            cancelled_nodes=None,
            node_pids=None,
        )
        result = _node_worker(job)
        assert result.result == "ok"
        assert "got_queue" in received


# ---------------------------------------------------------------------------
# QueueMessage / DAGQueue tests
# ---------------------------------------------------------------------------

class TestQueueTypes:
    def test_queue_message_creation(self):
        msg = QueueMessage(type="test", payload={"key": "value"}, node_id="A")
        assert msg.type == "test"
        assert msg.payload == {"key": "value"}
        assert msg.node_id == "A"
        assert msg.timestamp > 0

    def test_queue_message_stop_signal(self):
        stop = QueueMessage.stop_signal()
        assert stop.type == "__stop__"

    def test_queue_message_is_frozen(self):
        msg = QueueMessage(type="test")
        with pytest.raises((AttributeError, TypeError)):
            msg.type = "changed"  # type: ignore

    def test_dag_queue_rejects_non_message(self):
        from multiprocessing import Manager
        m = Manager()
        q = DAGQueue(m.Queue())
        with pytest.raises(TypeError):
            q.put("not a QueueMessage")
        m.shutdown()

    def test_dag_queue_put_and_get(self):
        from multiprocessing import Manager
        m = Manager()
        q = DAGQueue(m.Queue())
        msg = QueueMessage(type="ping", payload=123)
        q.put(msg)
        received = q.get(timeout=1)
        assert received.type == "ping"
        assert received.payload == 123
        m.shutdown()


# ---------------------------------------------------------------------------
# MultiprocessQueue tests
# ---------------------------------------------------------------------------

class TestMultiprocessQueue:
    def test_register_and_dispatch_handler(self):
        from multiprocessing import Manager
        received_payloads = []

        mq = MultiprocessQueue()
        m = Manager()
        dag_queue = DAGQueue(m.Queue())
        mq.register_queue(dag_queue)
        mq.register_message_handler("my_type", lambda p: received_payloads.append(p))

        mq.start_message_listener()
        dag_queue.put(QueueMessage(type="my_type", payload="test_payload"))
        time.sleep(0.3)
        mq.stop_message_listener()
        m.shutdown()

        assert "test_payload" in received_payloads

    def test_unknown_message_type_does_not_crash(self):
        from multiprocessing import Manager
        mq = MultiprocessQueue()
        m = Manager()
        dag_queue = DAGQueue(m.Queue())
        mq.register_queue(dag_queue)

        mq.start_message_listener()
        dag_queue.put(QueueMessage(type="unregistered_type", payload="data"))
        time.sleep(0.2)
        mq.stop_message_listener()
        m.shutdown()
        # Should not raise

    def test_stop_listener_sends_sentinel(self):
        from multiprocessing import Manager
        mq = MultiprocessQueue()
        m = Manager()
        dag_queue = DAGQueue(m.Queue())
        mq.register_queue(dag_queue)
        mq.start_message_listener()
        mq.stop_message_listener()
        # Listener thread should be gone
        assert mq._listener_thread is None
        m.shutdown()


# ---------------------------------------------------------------------------
# EasyInterface tests
# ---------------------------------------------------------------------------

class ConcreteInterface(EasyInterface):
    """Minimal concrete implementation for testing."""

    def __init__(self, dag):
        super().__init__(dag)
        self.events = []

    def dag_started(self, dag_id, metadata=None):
        self.events.append(("dag_started", dag_id))

    def dag_finished(self, dag_id, success, metadata=None):
        self.events.append(("dag_finished", dag_id, success))

    def node_started(self, node_id, metadata=None):
        self.events.append(("node_started", node_id))

    def node_progress(self, node_id, progress, metadata=None):
        self.events.append(("node_progress", node_id, progress))

    def node_finished(self, node_id, result=None, metadata=None):
        self.events.append(("node_finished", node_id))

    def node_errored(self, node_id, error, metadata=None):
        self.events.append(("node_errored", node_id))

    def node_cancelled(self, node_id, reason, metadata=None):
        self.events.append(("node_cancelled", node_id, reason))


class TestEasyInterface:
    def _make_dag_and_interface(self, processes=2):
        dag = EasyDAG(processes=processes)
        iface = ConcreteInterface(dag)
        return dag, iface

    def test_dag_lifecycle_events_fired(self):
        dag, iface = self._make_dag_and_interface()
        dag.add_node(DAGNode("A", _return_constant))
        iface.run_dag()
        event_types = [e[0] for e in iface.events]
        assert "dag_started" in event_types
        assert "dag_finished" in event_types
        assert "node_started" in event_types
        assert "node_finished" in event_types

    def test_node_errored_event_fired(self):
        dag, iface = self._make_dag_and_interface()
        dag.add_node(DAGNode("A", _fail))
        try:
            iface.run_dag()
        except RuntimeError:
            pass
        event_types = [e[0] for e in iface.events]
        assert "node_errored" in event_types

    def test_dag_finished_success_false_on_error(self):
        dag, iface = self._make_dag_and_interface()
        dag.add_node(DAGNode("A", _fail))
        try:
            iface.run_dag()
        except RuntimeError:
            pass
        finished_events = [e for e in iface.events if e[0] == "dag_finished"]
        assert len(finished_events) == 1
        assert finished_events[0][2] is False  # success=False

    def test_cancel_dag_stops_execution(self):
        def slow():
            time.sleep(10)
            return "done"

        dag, iface = self._make_dag_and_interface()
        dag.add_node(DAGNode("A", slow))

        import threading

        def cancel_after_delay():
            time.sleep(0.2)
            iface.cancel_dag("Test cancel")

        t = threading.Thread(target=cancel_after_delay)
        t.start()

        try:
            iface.run_dag()
        except RuntimeError:
            pass
        finally:
            t.join(timeout=5)

        assert iface.cancel_dag_flag is not None

    def test_trim_dag_outside_run_raises(self):
        dag, iface = self._make_dag_and_interface()
        with pytest.raises(RuntimeError, match="outside of an active DAG run"):
            iface.trim_dag("some_node")

    def test_trim_dag_cancels_node_and_descendants(self):
        """Trimming a node should cancel it and all downstream nodes."""
        dag = EasyDAG(processes=2)
        iface = ConcreteInterface(dag)

        # A -> B -> C; we will trim B, so C should also be cancelled
        dag.add_node(DAGNode("A", _return_five))
        dag.add_node(DAGNode("B", _return_ten))
        dag.add_node(DAGNode("C", _return_constant))
        dag.add_edge("A", "B")
        dag.add_edge("B", "C")

        import threading

        def trim_after_start():
            time.sleep(0.1)
            try:
                iface.trim_dag("B", reason="pruned")
            except RuntimeError:
                pass

        t = threading.Thread(target=trim_after_start)
        t.start()

        try:
            iface.run_dag()
        except RuntimeError:
            pass
        finally:
            t.join(timeout=5)

        cancelled_ids = {e[1] for e in iface.events if e[0] == "node_cancelled"}
        # B and/or C should have been cancelled
        assert cancelled_ids & {"B", "C"}

    def test_run_dag_resets_cancel_flag(self):
        dag, iface = self._make_dag_and_interface()
        dag.add_node(DAGNode("A", _return_constant))
        iface.cancel_dag_flag = "stale_cancel"
        iface.run_dag()
        # After a fresh run the flag should have been reset before execution started
        # (set to None in run_dag, then possibly set again if cancelled)
        # Since we didn't actually cancel, dag_result should exist
        assert iface.dag_result is not None


# ---------------------------------------------------------------------------
# NodeJobResult / NodeError dataclass tests
# ---------------------------------------------------------------------------

class TestDataclasses:
    def test_node_job_result_default(self):
        r = NodeJobResult(node_id="X")
        assert r.node_id == "X"
        assert r.result is None
        assert r.error_info is None
        assert r.cancelled is False
        assert r.cancel_reason is None

    def test_node_error_fields(self):
        err = NodeError(traceback="tb", inputs="inp", exception="exc")
        assert err.traceback == "tb"
        assert err.inputs == "inp"
        assert err.exception == "exc"

    def test_node_job_serializable(self):
        """NodeJob must survive pickle (required for multiprocessing)."""
        job = NodeJob(
            node_id="A",
            func=_return_constant,
            args=(),
            kwargs={},
            resolved_inputs={},
            message_queue=None,
        )
        data = pickle.dumps(job)
        restored = pickle.loads(data)
        assert restored.node_id == "A"

    def test_node_job_result_serializable(self):
        result = NodeJobResult(node_id="A", result={"key": [1, 2, 3]})
        data = pickle.dumps(result)
        restored = pickle.loads(data)
        assert restored.result == {"key": [1, 2, 3]}


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_diamond_dag(self):
        """
             A
            / \\
           B   C
            \\ /
             D
        """

        dag = EasyDAG(processes=2)
        dag.add_node(DAGNode("A", node_a))
        dag.add_node(DAGNode("B", node_b))
        dag.add_node(DAGNode("C", node_c))
        dag.add_node(DAGNode("D", node_d))
        dag.add_edge("A", "B")
        dag.add_edge("A", "C")
        dag.add_edge("B", "D")
        dag.add_edge("C", "D")
        result = dag.run()
        assert result["A"] == 10
        assert result["B"] == 11
        assert result["C"] == 12
        assert result["D"] == 23

    def test_wide_parallel_dag(self):
        """One root, many independent leaves."""
        dag = EasyDAG(processes=4)
        dag.add_node(DAGNode("root", _return_five))
        for i in range(8):
            dag.add_node(DAGNode(f"leaf_{i}", _return_ten))
            dag.add_edge("root", f"leaf_{i}")
        result = dag.run()
        assert result["root"] == 5
        assert all(result[f"leaf_{i}"] == 10 for i in range(8))

    def test_multiple_root_nodes(self):
        """Both A and B are roots with no dependencies."""
        dag = EasyDAG(processes=2)
        dag.add_node(DAGNode("A", _return_five))
        dag.add_node(DAGNode("B", _return_ten))
        dag.add_node(DAGNode("C", _return_constant))
        dag.add_edge("A", "C")
        dag.add_edge("B", "C")
        result = dag.run()
        assert result["A"] == 5
        assert result["B"] == 10
        assert result["C"] == 42

    def test_node_returns_none(self):
        """Nodes that return None should be stored correctly."""
        dag = EasyDAG(processes=1)
        dag.add_node(DAGNode("A", returns_none))
        result = dag.run()
        assert "A" in result
        assert result["A"] is None

    def test_large_return_value(self):
        """Nodes returning large data structures should work."""
        dag = EasyDAG(processes=1)
        dag.add_node(DAGNode("A", big_list))
        result = dag.run()
        assert len(result["A"]) == 10_000

    def test_processes_default_to_cpu_minus_one(self):
        import multiprocessing
        dag = EasyDAG()
        expected = max(1, multiprocessing.cpu_count() - 1)
        assert dag.processes == expected

    def test_processes_minimum_one(self):
        with patch("multiprocessing.cpu_count", return_value=1):
            dag = EasyDAG()
            assert dag.processes >= 1

if __name__ == "__main__":
    flaky()
    flaky()
    flaky()
    call_count[0] = 0
    # pytest.main([__file__])