# -----------------------------------------------
# FILE: core.py
# -----------------------------------------------

import multiprocessing as mp
import os
import pickle
import signal
import time
import traceback
from collections import defaultdict, deque
from contextlib import contextmanager
from multiprocessing import Pool, Manager
from multiprocessing.pool import Pool as PoolType
from pathlib import Path
from queue import Queue
from typing import Any, Callable, Dict, List, Optional

from .queue import MultiprocessQueue
from .node import DAGNode, _node_worker
from .dag_types import DAGQueue, NodeJobResult, NodeJob, NodeError
from .interface import DagInterface, EasyInterface


class EasyDAG(DagInterface):
    """A lightweight DAG executor using multiprocessing.Pool.

    Usage:
        dag = EasyDAG(processes=4)
        dag.add_node(DAGNode('A', func_a))
        dag.add_node(DAGNode('B', func_b))
        dag.add_edge('A', 'B')  # B depends on A
        outputs = dag.run()  # dict of node_id -> output

    Node functions receive all dependency outputs packed in a dict under the keyword 'inputs'.
    For example, if B depends on A and C, then 'inputs' passed to B will be {'A': <outA>, 'C': <outC>}.
    """

    def __init__(self, processes: int = None, fail_fast: bool = True, mp_queue: MultiprocessQueue | None = None):
        """
        processes: number of worker processes (default: CPU count - 1)
        fail_fast: if True, stop scheduling new nodes when any node fails (default: True)
        """
        self.nodes: Dict[str, DAGNode] = {}
        self.adj: Dict[str, List[str]] = defaultdict(list)
        self.rev_adj: Dict[str, List[str]] = defaultdict(list)
        self.processes = processes or max(1, mp.cpu_count() - 1)
        self.fail_fast = fail_fast
        self._message_queue = mp_queue
        self.dag_id = None

    def add_node(self, node: DAGNode) -> None:
        if node.id in self.nodes:
            raise ValueError(f"Node with id '{node.id}' already exists")
        self.nodes[node.id] = node

    def add_edge(self, from_id: str, to_id: str) -> None:
        if from_id not in self.nodes:
            raise KeyError(f"Unknown from-node '{from_id}'")
        if to_id not in self.nodes:
            raise KeyError(f"Unknown to-node '{to_id}'")

        if from_id == to_id:
            raise ValueError(f"Self-loop detected: {from_id} -> {to_id}")

        if to_id in self.adj[from_id]:
            raise ValueError(f"Duplicate edge: {from_id} -> {to_id}")

        self.adj[from_id].append(to_id)
        self.rev_adj[to_id].append(from_id)

    def _descendants(self, node_id: str) -> set:
        """Return the set of all nodes that (transitively) depend on node_id,
        excluding node_id itself."""
        visited = set()
        queue = deque(self.adj.get(node_id, []))
        while queue:
            nid = queue.popleft()
            if nid not in visited:
                visited.add(nid)
                queue.extend(self.adj.get(nid, []))
        return visited

    @contextmanager
    def _create_pool(self):
        """Context manager for proper pool resource management."""
        pool = Pool(processes=self.processes)
        try:
            yield pool
        finally:
            pool.terminate()
            pool.join()

    @staticmethod
    def _get_cached(node_id: str, cache_path: Optional[Path]) -> Optional[Any]:
        """Retrieve cached result for a node if available."""
        if cache_path is None:
            return None
        cache_file = cache_path / f"{node_id}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"DAG Cache corrupted, rebuilding. {repr(e)}")
                return None
        return None

    @staticmethod
    def _save_to_cache(node_id: str, result: Any, cache_path: Optional[Path]) -> None:
        """Save node result to cache."""
        if cache_path is None:
            return
        try:
            cache_file = cache_path / f"{node_id}.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
        except Exception as e:
            print(f"DAG Cache failed: {repr(e)}")

    def run(
            self,
            dag_timeout: Optional[float] = None,
            cache_dir: Optional[str] = None,
            progress_callback: Optional[Callable[[int, int, str], None]] = None,
            interface: Optional[EasyInterface] = None
    ) -> Dict[str, Any]:
        """Execute the DAG and return a dict mapping node_id -> output.

        dag_timeout: maximum seconds to wait for the whole DAG to complete.
        cache_dir: if provided, cache node outputs to disk and reuse on subsequent runs.
        progress_callback: optional callable(completed, total, node_id) after each node completes.
        interface: optional EasyInterface for lifecycle callbacks and control (cancel, trim).
        """
        if not self.nodes:
            return {}

        self._topological_check()

        cache_path = None
        if cache_dir:
            cache_path = Path(cache_dir)
            cache_path.mkdir(parents=True, exist_ok=True)

        manager = Manager()
        mp_queue = None
        if self._message_queue:
            mp_queue = DAGQueue(manager.Queue())
            self._message_queue.register_queue(mp_queue)

        outputs = manager.dict()
        errors = manager.dict()
        times = manager.dict()
        stop_state = manager.dict()

        # Shared dicts for per-node cancellation:
        #   cancelled_nodes: node_id -> cancel_reason  (read by workers to self-abort)
        #   node_pids:       node_id -> os.pid          (written by workers; read by parent to kill)
        cancelled_nodes = manager.dict()
        node_pids = manager.dict()

        if self._message_queue:
            self._message_queue.start_message_listener()

        # Compute initial in-degrees
        indeg: Dict[str, int] = {nid: 0 for nid in self.nodes}
        for u, outs in self.adj.items():
            for v in outs:
                indeg[v] += 1

        ready: Queue = Queue()
        for n in [n for n, d in indeg.items() if d == 0]:
            ready.put(n)

        total_nodes = len(self.nodes)
        completed = [0]
        retry_counts = defaultdict(int)
        pending = set()  # node ids currently dispatched to the pool

        # ------------------------------------------------------------------ #
        # Internal helpers                                                     #
        # ------------------------------------------------------------------ #

        def _cancel_node_and_descendants(node_id: str, reason: str) -> None:
            """
            Mark node_id and all its descendants as cancelled.

            - If the node is already in `outputs` (completed successfully) the
              request is silently ignored for that specific node — descendants
              that haven't started yet will still be cancelled.
            - If the node is currently running (in `pending`), we record it in
              cancelled_nodes so the worker exits early on its next check, and
              we also SIGKILL the worker process directly via node_pids.
            - Nodes that are merely queued (in `ready`) will be skipped at
              submit time because they appear in cancelled_nodes.
            """
            targets = {node_id} | self._descendants(node_id)

            for nid in targets:
                if nid in outputs:
                    # Already completed — do not disturb it or its result.
                    # We still want to cancel its descendants though, so we
                    # leave them in `targets` but skip marking this one.
                    continue

                cancelled_nodes[nid] = reason

                if interface:
                    interface.node_cancelled(nid, reason)

                # If the node is actively running, try to kill its worker process
                if nid in pending:
                    pid = node_pids.get(nid)
                    if pid:
                        try:
                            os.kill(pid, signal.SIGTERM)
                        except (ProcessLookupError, PermissionError):
                            pass  # Worker already finished — harmless

        def submit(node_id: str, p: PoolType):
            # Skip nodes that have been cancelled before they start
            if node_id in cancelled_nodes:
                return

            times[node_id] = time.time()

            cached_result = self._get_cached(node_id, cache_path)
            if cached_result is not None:
                outputs[node_id] = cached_result
                on_done(NodeJobResult(node_id, cached_result))
                return

            node = self.nodes[node_id]
            dep_ids = self.rev_adj.get(node_id, [])
            resolved_inputs = {dep.lower(): outputs[dep] for dep in dep_ids}

            payload = NodeJob(
                node_id,
                node.func,
                node.args,
                node.kwargs,
                resolved_inputs,
                mp_queue,
                cancelled_nodes=cancelled_nodes,
                node_pids=node_pids,
            )
            pending.add(node_id)

            try:
                p.apply_async(
                    _node_worker,
                    args=(payload,),
                    callback=on_done,
                    error_callback=lambda ecb, nid=node_id: _handle_async_error(ecb, nid),
                )
            except Exception as e:
                times[node_id] = time.time() - times[node_id]
                pending.discard(node_id)
                tb = traceback.format_exc()
                errors[node_id] = NodeError(tb, str(resolved_inputs)[:500], f"Schedule error: {str(e)}")
                if self.fail_fast:
                    stop_state["Schedule Error"] = node_id
                if interface:
                    interface.node_errored(node_id, f"Schedule Error: {e}", {"time": times[node_id], "traceback": tb})

        def _handle_async_error(e, node_id):
            times[node_id] = time.time() - times[node_id]
            pending.discard(node_id)
            # If the node was cancelled, don't log it as an infrastructure error
            if node_id in cancelled_nodes:
                return
            tb = "".join(traceback.format_exception_only(type(e), e))
            errors[node_id] = NodeError(tb, "<unknown - worker infrastructure error>", str(e))
            if self.fail_fast:
                stop_state["Infrastructure Error"] = node_id
            if interface:
                interface.node_errored(node_id, f"Infrastructure Error: {e}", {"time": times[node_id], "traceback": tb})

        def on_done(job_result: NodeJobResult):
            node_id = job_result.node_id
            times[node_id] = time.time() - times[node_id]
            pending.discard(node_id)

            # ── Cancelled result ────────────────────────────────────────────
            if job_result.cancelled:
                # Worker self-reported cancellation; interface already notified
                # by _cancel_node_and_descendants, nothing more to do.
                return

            # ── Late cancellation: node finished but was concurrently cancelled ─
            # (race: cancel arrived after worker had already computed its result)
            if node_id in cancelled_nodes:
                # Discard the result; treat as cancelled, not successful.
                return

            # ── Error path ──────────────────────────────────────────────────
            if job_result.error_info is not None:
                node = self.nodes[node_id]
                if retry_counts[node_id] < node.max_retries:
                    retry_counts[node_id] += 1
                    ready.put(node_id)
                    return

                errors[node_id] = job_result.error_info
                if self.fail_fast:
                    stop_state["Node Runtime Error"] = node_id
                if interface:
                    interface.node_errored(
                        node_id,
                        f"Node error: {job_result.error_info.exception}",
                        {
                            "time": times[node_id],
                            "traceback": job_result.error_info.traceback,
                            "inputs": str(job_result.error_info.inputs),
                        },
                    )
                return

            # ── Success path ────────────────────────────────────────────────
            result = job_result.result
            outputs[node_id] = result
            self._save_to_cache(node_id, result, cache_path)

            completed[0] += 1
            if progress_callback:
                progress_callback(completed[0], total_nodes, node_id)

            for successor in self.adj.get(node_id, []):
                # If this successor is already cancelled, don't re-queue it
                if successor in cancelled_nodes:
                    continue
                indeg[successor] -= 1
                if indeg[successor] == 0:
                    ready.put(successor)

            if interface:
                interface.node_finished(node_id, result, {"time": times[node_id]})

        # ------------------------------------------------------------------ #
        # Wire trim_dag on the interface so it can reach our shared state     #
        # ------------------------------------------------------------------ #
        if interface:
            def _trim_dag_impl(node_id: str, reason: str = "Trimmed") -> None:
                _cancel_node_and_descendants(node_id, reason)

            interface._trim_dag_impl = _trim_dag_impl

        # ------------------------------------------------------------------ #
        # Main execution loop                                                 #
        # ------------------------------------------------------------------ #
        if interface:
            interface.dag_started(self.dag_id)

        with self._create_pool() as pool:
            try:
                dag_start = time.time()

                while not ready.empty():
                    nid = ready.get()
                    if nid not in cancelled_nodes:
                        if interface:
                            interface.node_started(nid)
                        submit(nid, pool)

                while pending or not ready.empty():
                    if interface and interface.cancel_dag_flag:
                        stop_state["Cancelled"] = interface.cancel_dag_flag
                        break

                    if self.fail_fast and len(stop_state) > 0:
                        break

                    while not ready.empty():
                        nid = ready.get()
                        if nid not in cancelled_nodes:
                            if interface:
                                interface.node_started(nid)
                            submit(nid, pool)

                    if dag_timeout is not None and (time.time() - dag_start) > dag_timeout:
                        raise TimeoutError(f"Dag_ID: {self.dag_id} timed out.")

                    time.sleep(0.1)

                if len(stop_state) > 0 and (
                        "Cancelled" not in stop_state or (interface and not interface.cancel_graceful)
                ):
                    pool.terminate()
                else:
                    pool.close()
                pool.join()

            except Exception:
                pool.terminate()
                pool.join()
                raise

            finally:
                if self._message_queue:
                    self._message_queue.stop_message_listener()
                dag_time_elapsed = time.time() - dag_start
                if interface:
                    dag_md = {
                        "outputs": dict(outputs),
                        "errors": dict(errors),
                        "time": dag_time_elapsed,
                        "num_nodes": total_nodes,
                        "num_success": len(outputs),
                        "num_fail": len(errors),
                        "num_skipped": total_nodes - len(outputs) - len(errors),
                        "stop_state": dict(stop_state),
                        "num_cancelled": len(cancelled_nodes),
                    }
                    interface.dag_finished(self.dag_id, len(errors) == 0, metadata=dag_md)

        error_messages = []
        if len(errors) > 0:
            for k in errors.keys():
                error_info: NodeError = errors[k]
                msg = (
                    f"Node {k} failed:\n"
                    f"  Exception: {error_info.exception}\n"
                    f"  Inputs: {error_info.inputs}\n"
                    f"  Traceback:\n{error_info.traceback}"
                )
                error_messages.append(msg)
            combined = "\n\n".join(error_messages)
            raise RuntimeError(f"One or more nodes failed:\n{combined}")

        if len(stop_state) > 0:
            for cancel_reason in stop_state.keys():
                n: str = stop_state[cancel_reason]
                msg = f"From node: {n}\nWith reason: {cancel_reason}"
                error_messages.append(msg)
            combined = "\n\n".join(error_messages)
            raise RuntimeError(f"Stop state triggered:\n{combined}")

        return dict(outputs)

    def _topological_check(self) -> None:
        indeg = {nid: 0 for nid in self.nodes}
        for u, outs in self.adj.items():
            for v in outs:
                indeg[v] += 1
        q = deque([n for n, d in indeg.items() if d == 0])
        seen = 0
        while q:
            u = q.popleft()
            seen += 1
            for v in self.adj.get(u, []):
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        if seen != len(self.nodes):
            raise ValueError("Graph contains cycles or unreachable nodes!")

    def to_graphviz(self) -> str:
        lines = ["digraph DAG {", "  rankdir=LR;", "  node [shape=box, style=rounded];"]
        for node_id in self.nodes:
            lines.append(f'  "{node_id}";')
        for from_id, to_ids in self.adj.items():
            for to_id in to_ids:
                lines.append(f'  "{from_id}" -> "{to_id}";')
        lines.append("}")
        return "\n".join(lines)

    def load_dag_from_spec(self, spec_list: List[Dict], function_lookup: Dict[str, Callable]):
        for spec in spec_list:
            node = DAGNode(
                spec["id"],
                function_lookup[spec["func"]],
                args=tuple(spec.get("args", [])),
                kwargs=spec.get("kwargs", {}),
                max_retries=spec.get("max_retries", 0),
            )
            self.add_node(node)
        for spec in spec_list:
            node_id = spec["id"]
            for parent in spec.get("parents", []):
                self.add_edge(parent, node_id)

# -----------------------------------------------
# END FILE: interface.py
# -----------------------------------------------