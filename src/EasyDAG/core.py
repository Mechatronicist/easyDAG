import multiprocessing as mp
import pickle
import time
import traceback
from collections import defaultdict, deque
from contextlib import contextmanager
from multiprocessing import Pool, Manager
from multiprocessing.pool import Pool as PoolType
from pathlib import Path
from queue import Queue
from typing import Any, Callable, Dict, List, Optional

from src.EasyDAG.messages import MultiprocessQueueWatcher
from src.EasyDAG.node import DAGNode, _node_worker
from src.EasyDAG.types import DAGQueue, NodeJobResult, NodeJob, NodeError


class EasyDAG:
    """A lightweight DAG executor using multiprocessing.Pool.

    Usage:
        dag = DistributedDAG(processes=4)
        dag.add_node(DAGNode('A', func_a))
        dag.add_node(DAGNode('B', func_b))
        dag.add_edge('A', 'B')  # B depends on A
        outputs = dag.run()  # dict of node_id -> output

    Node functions receive all dependency outputs packed in a dict under the keyword 'inputs'.
    For example, if B depends on A and C, then 'inputs' passed to B will be {'A': <outA>, 'C': <outC>}.
    """

    def __init__(self, processes: int = None, fail_fast: bool = True,
                 watch_queue: MultiprocessQueueWatcher | None = None):
        """
        processes: number of worker processes (default: CPU count - 1)
        fail_fast: if True, stop scheduling new nodes when any node fails (default: True)
        """
        self.nodes: Dict[str, DAGNode] = {}
        self.adj: Dict[str, List[str]] = defaultdict(list)
        self.rev_adj: Dict[str, List[str]] = defaultdict(list)
        self.processes = processes or max(1, mp.cpu_count() - 1)
        self.fail_fast = fail_fast
        self._message_queue = watch_queue

    def add_node(self, node: DAGNode) -> None:
        if node.id in self.nodes:
            raise ValueError(f"Node with id '{node.id}' already exists")
        self.nodes[node.id] = node

    def add_edge(self, from_id: str, to_id: str) -> None:
        if from_id not in self.nodes:
            raise KeyError(f"Unknown from-node '{from_id}'")
        if to_id not in self.nodes:
            raise KeyError(f"Unknown to-node '{to_id}'")

        # Check for self-loops
        if from_id == to_id:
            raise ValueError(f"Self-loop detected: {from_id} -> {to_id}")

        # Check for duplicate edges
        if to_id in self.adj[from_id]:
            raise ValueError(f"Duplicate edge: {from_id} -> {to_id}")

        self.adj[from_id].append(to_id)
        self.rev_adj[to_id].append(from_id)

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
                print(f"DAG Cache corrupted, rebuilding. {repr(e)}''")
                # If cache is corrupted, ignore and recompute
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
                pickle.dump(result, f)  # type: ignore[arg-type]
        except Exception as e:
            # Non-critical if caching fails
            print(f"DAG Cache failed: {repr(e)}''")

    def run(
            self,
            timeout: Optional[float] = None,
            cache_dir: Optional[str] = None,
            progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Dict[str, Any]:
        """Execute the DAG and return a dict mapping node_id -> output.

        timeout: maximum seconds to wait for the whole DAG to complete. None means wait forever.
        cache_dir: if provided, cache node outputs to disk and reuse on subsequent runs.
        progress_callback: optional callable(completed, total, node_id) called after each node completes.

        Note: Node functions can send messages to the main thread by using the 'message_queue'
              parameter that is automatically provided to them. Use register_message_handler()
              to set up handlers for different message types before calling run().
        """
        if not self.nodes:
            return {}

        # Validate DAG
        self._topological_check()

        # Setup caching
        cache_path = None
        if cache_dir:
            cache_path = Path(cache_dir)
            cache_path.mkdir(parents=True, exist_ok=True)

        manager = Manager()
        mp_queue = None
        if self._message_queue:
            mp_queue = DAGQueue(manager.Queue())
            self._message_queue.register_queue(mp_queue)  # register queue to wrapper
        outputs = manager.dict()  # shared dict: node_id -> result
        errors = manager.dict()

        # Setup message queue for child processes to communicate with main thread
        if self._message_queue:
            self._message_queue.start_message_listener()

        # Local state in the parent process
        indeg: Dict[str, int] = {nid: 0 for nid in self.nodes}
        for u, outs in self.adj.items():
            for v in outs:
                indeg[v] += 1

        # Use thread-safe queue for ready nodes
        ready: Queue = Queue()
        for n in [n for n, d in indeg.items() if d == 0]:
            ready.put(n)

        # Track progress and retries
        total_nodes = len(self.nodes)
        completed = [0]  # Use list for closure mutability
        retry_counts = defaultdict(int)

        # For callback bookkeeping
        pending = set()  # node ids currently running

        # Helper to submit a node
        def submit(node_id: str, p: PoolType):
            # Check cache first
            cached_result = self._get_cached(node_id, cache_path)
            if cached_result is not None:
                outputs[node_id] = cached_result
                on_done(NodeJobResult(node_id, cached_result))
                return

            node = self.nodes[node_id]
            # Build inputs dict from dependencies' outputs
            dep_ids = self.rev_adj.get(node_id, [])
            resolved_inputs = {dep.lower(): outputs[dep] for dep in dep_ids}

            payload = NodeJob(node_id, node.func, node.args, node.kwargs, resolved_inputs, mp_queue)
            pending.add(node_id)

            try:
                # IMPORTANT: provide error_callback so exceptions from worker get routed
                p.apply_async(_node_worker, args=(payload,), callback=on_done,
                              error_callback=lambda ecb, nid=node_id: _handle_async_error(ecb, nid))
            except Exception as e:
                # This catches immediate scheduling errors (e.g. pickling error when serializing payload)
                pending.discard(node_id)
                # record error info
                tb = traceback.format_exc()
                errors[node_id] = {
                    'traceback': tb,
                    'inputs': str(resolved_inputs)[:500],
                    'exception': str(e)
                }
                if self.fail_fast:
                    errors['__stop__'] = True

        def _handle_async_error(exc, node_id):
            # print(f"Async error for node {node_id}: {exc}")
            # Remove from pending and record error similar to on_done's behavior
            pending.discard(node_id)
            tb = "".join(traceback.format_exception_only(type(exc), exc))
            errors[node_id] = NodeError(tb, "<unknown - scheduling error>", str(exc))
            if self.fail_fast:
                errors['__stop__'] = True

        # Callback executed in parent process when a worker finishes
        def on_done(job_result: NodeJobResult):
            node_id = job_result.node_id
            pending.discard(node_id)

            if job_result.error_info is not None:
                node = self.nodes[node_id]
                # Check if we should retry
                if retry_counts[node_id] < node.max_retries:
                    retry_counts[node_id] += 1
                    # Don't mark as completed, resubmit
                    ready.put(node_id)
                    return

                # No more retries, record error
                errors[node_id] = job_result.error_info
                if self.fail_fast:
                    # Signal to stop submitting new work
                    errors['__stop__'] = True
            else:
                result = job_result.result
                outputs[node_id] = result
                # Save to cache
                self._save_to_cache(node_id, result, cache_path)

                # Update progress
                completed[0] += 1
                if progress_callback:
                    progress_callback(completed[0], total_nodes, node_id)

                # Decrease in-degree of successors and enqueue any that reach 0
                for successor in self.adj.get(node_id, []):
                    indeg[successor] -= 1
                    if indeg[successor] == 0:
                        ready.put(successor)

        # Main execution loop
        with self._create_pool() as pool:
            try:
                start = time.time()

                # Submit initial ready nodes
                while not ready.empty():
                    nid = ready.get()
                    submit(nid, pool)

                # Main loop: as nodes finish, new nodes become ready via callback
                while pending or not ready.empty():
                    # Check for fail-fast condition
                    if self.fail_fast and '__stop__' in errors:
                        break

                    # Submit newly ready nodes
                    while not ready.empty():
                        nid = ready.get()
                        submit(nid, pool)

                    if timeout is not None and (time.time() - start) > timeout:
                        raise TimeoutError("DistributedDAG.run timed out")

                    # Small sleep to yield control and allow callbacks to run
                    time.sleep(0.1)

                pool.close()
                pool.join()

            except Exception:
                raise
            finally:
                # Stop the message listener
                if self._message_queue:
                    self._message_queue.stop_message_listener()

        # If any errors, raise an aggregated exception with tracebacks
        if len(errors) > 0:
            error_messages = []
            for k in errors.keys():
                if k == '__stop__':
                    continue
                error_info: NodeError = errors[k]
                msg = f"Node {k} failed:\n"
                msg += f"  Exception: {error_info.exception}\n"
                msg += f"  Inputs: {error_info.inputs}\n"
                msg += f"  Traceback:\n{error_info.traceback}"
                error_messages.append(msg)

            combined = "\n\n".join(error_messages)
            raise RuntimeError(f"One or more nodes failed:\n{combined}")

        # Convert manager.dict to regular dict
        return dict(outputs)

    def _topological_check(self) -> None:
        # Check acyclic using Kahn's algorithm (without modifying internal state)
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
        """
        Generate DOT format for visualization.

        Can be rendered with graphviz:
            graphviz.Source(dag.to_graphviz()).render('dag', view=True)
        """
        lines = ["digraph DAG {", "  rankdir=LR;", "  node [shape=box, style=rounded];"]

        for node_id in self.nodes:
            lines.append(f'  "{node_id}";')

        for from_id, to_ids in self.adj.items():
            for to_id in to_ids:
                lines.append(f'  "{from_id}" -> "{to_id}";')

        lines.append("}")
        return "\n".join(lines)

    def load_dag_from_spec(self, spec_list: List[Dict], function_lookup: Dict[str, Callable]):
        """
        spec_list: list of dicts with keys:
            - id: node identifier
            - func: string name of function
            - args: optional list of positional arguments
            - kwargs: optional dict of keyword arguments
            - parents: list of parent node ids (dependencies)
            - max_retries: optional number of retries on failure

        function_lookup: dict mapping string -> actual function
                         e.g. {"task_a": task_a}
        """

        # Unpack and add all nodes
        for spec in spec_list:
            node = DAGNode(
                spec["id"],
                function_lookup[spec["func"]],
                args=tuple(spec.get("args", [])),
                kwargs=spec.get("kwargs", {}),
                max_retries=spec.get("max_retries", 0)
            )
            self.add_node(node)

        # Add edges
        for spec in spec_list:
            node_id = spec["id"]
            for parent in spec.get("parents", []):
                self.add_edge(parent, node_id)
