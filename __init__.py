"""
distributed_dag.py

A simple distributed (multiprocessing) Directed Acyclic Graph executor.

Features:
- Define nodes with callables (must be picklable, i.e. top-level functions).
- Define edges (dependencies).
- Executes independent nodes in parallel using multiprocessing.Pool.
- Collects outputs and passes them to dependent nodes by node id.
- Provides basic error handling and graceful shutdown.

Limitations / notes:
- Functions must be picklable (defined at top-level of a module).
- This is a lightweight executor, not a full scheduler. It runs in a single machine
  across multiple CPU cores using multiprocessing.

Example usage is at the bottom of this file.
"""
from __future__ import annotations

import multiprocessing as mp
import traceback
from collections import defaultdict, deque
from multiprocessing import Pool, Manager
from typing import Any, Callable, Dict, List, Optional, Tuple


class DAGNode:
    def __init__(self, node_id: str, func: Callable[..., Any], *, args: Optional[Tuple] = None,
                 kwargs: Optional[Dict] = None):
        """Create a DAG node.

        node_id: unique identifier for the node (string)
        func: a picklable callable. It will be called as func(*args, **kwargs)
              where additional keyword 'inputs' may be provided (see executor).
        args/kwargs: static arguments that will be provided to func in addition
                     to resolved inputs.
        """
        self.id = node_id
        self.func = func
        self.args = args or ()
        self.kwargs = kwargs or {}

    def __repr__(self):
        return f"DAGNode({self.id})"


# Worker wrapper must be top-level so it is picklable.
def _node_worker(callable_and_payload):
    """Unpack payload and run the node function.

    Expects a tuple: (node_id, func, args, kwargs, resolved_inputs)
    Returns: (node_id, result, maybe_exception_str)
    """
    node_id, func, args, kwargs, inputs = callable_and_payload
    try:
        # Provide inputs as kwargs under 'inputs' key. Users' kwargs may include
        # the same key name; inputs will override if present.
        if inputs is None:
            inputs = {}
        # Call the function. It's up to the function to accept 'inputs' if it needs them.
        result = func(*args, **{**kwargs, "inputs": inputs})
        return (node_id, result, None)
    except Exception:
        tb = traceback.format_exc()
        return (node_id, None, tb)


class DistributedDAG:
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

    def __init__(self, processes: Optional[int] = None):
        self.nodes: Dict[str, DAGNode] = {}
        self.adj: Dict[str, List[str]] = defaultdict(list)
        self.rev_adj: Dict[str, List[str]] = defaultdict(list)
        self.processes = processes or max(1, mp.cpu_count() - 1)

    def add_node(self, node: DAGNode) -> None:
        if node.id in self.nodes:
            raise ValueError(f"Node with id '{node.id}' already exists")
        self.nodes[node.id] = node

    def add_edge(self, from_id: str, to_id: str) -> None:
        if from_id not in self.nodes:
            raise KeyError(f"Unknown from-node '{from_id}'")
        if to_id not in self.nodes:
            raise KeyError(f"Unknown to-node '{to_id}'")
        self.adj[from_id].append(to_id)
        self.rev_adj[to_id].append(from_id)

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
            for v in self.adj.get(u, ()):  # type: ignore
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        if seen != len(self.nodes):
            raise ValueError("Graph contains cycles or unreachable nodes (not a DAG)")

    def run(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Execute the DAG and return a dict mapping node_id -> output.

        timeout: maximum seconds to wait for the whole DAG to complete. None means wait forever.
        """
        if not self.nodes:
            return {}

        # Validate DAG
        self._topological_check()

        manager = Manager()
        outputs = manager.dict()  # shared dict: node_id -> result
        errors = manager.dict()

        # Local state in the parent process
        indeg: Dict[str, int] = {nid: 0 for nid in self.nodes}
        for u, outs in self.adj.items():
            for v in outs:
                indeg[v] += 1

        ready = deque([n for n, d in indeg.items() if d == 0])

        # For callback bookkeeping
        pending = set()  # node ids currently running
        pool = Pool(processes=self.processes)

        # Helper to submit a node
        def submit(node_id: str):
            node = self.nodes[node_id]
            # Build inputs dict from dependencies' outputs
            dep_ids = self.rev_adj.get(node_id, [])
            resolved_inputs = {dep: outputs[dep] for dep in dep_ids}
            payload = (node_id, node.func, node.args, node.kwargs, resolved_inputs)
            pending.add(node_id)
            pool.apply_async(_node_worker, args=(payload,), callback=on_done)

        # Callback executed in parent process when a worker finishes
        def on_done(result_tuple):
            node_id, result, tb = result_tuple
            pending.discard(node_id)
            if tb is not None:
                errors[node_id] = tb
            else:
                outputs[node_id] = result
            # Decrease indegree of successors and enqueue any that reach 0
            for succ in self.adj.get(node_id, []):
                indeg[succ] -= 1
                if indeg[succ] == 0:
                    ready.append(succ)

        # Submit initial ready nodes
        for nid in list(ready):
            ready.popleft()
            submit(nid)

        # Main loop: as nodes finish, new nodes become ready via callback
        try:
            import time
            start = time.time()
            while pending or ready:
                # Submit newly ready nodes
                while ready:
                    nid = ready.popleft()
                    submit(nid)

                if timeout is not None and (time.time() - start) > timeout:
                    raise TimeoutError("DistributedDAG.run timed out")

                # small sleep to yield control and allow callbacks to run
                time.sleep(0.01)

            pool.close()
            pool.join()

        except Exception:
            pool.terminate()
            pool.join()
            raise

        # If any errors, raise an aggregated exception with tracebacks
        if len(errors) > 0:
            combined = "\n\n".join(f"Node {k} failed:\n{errors[k]}" for k in errors.keys())
            raise RuntimeError(f"One or more nodes failed:\n{combined}")

        # Convert manager.dict to regular dict
        return dict(outputs)
