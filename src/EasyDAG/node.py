# -----------------------------------------------
# FILE: node.py
# -----------------------------------------------

import inspect
import os
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

from .dag_types import NodeJob, NodeJobResult, NodeError


class DAGNode:
    def __init__(
            self,
            node_id: str,
            func: Callable[..., Any],
            *,
            args: Optional[Tuple] = None,
            kwargs: Optional[Dict] = None,
            max_retries: int = 0
    ):
        """Create a DAG node.

        node_id: unique identifier for the node (string)
        func: callable. It will be called as func(*args, **kwargs)
              where additional keyword 'inputs' may be provided (see executor).
        args/kwargs: static arguments that will be provided to func in addition
                     to resolved inputs.
        max_retries: number of times to retry this node on failure (default: 0)
        """
        self.id = node_id
        self.func = func
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.max_retries = max_retries

    def __repr__(self):
        return f"DAGNode({self.id})"


def _node_worker(job: NodeJob) -> NodeJobResult:
    """Unpack payload and run the node function.

    Expects a NodeJob dataclass.
    Returns: NodeJobResult with result or error_info populated.
    """
    node_id = job.node_id
    func = job.func
    args = job.args
    kwargs = job.kwargs
    inputs = job.resolved_inputs
    params = inspect.signature(func).parameters

    # Register this worker's PID so the parent can kill it if needed
    if job.node_pids is not None:
        job.node_pids[node_id] = os.getpid()

    # Check if this node was cancelled before we even started doing real work
    if job.cancelled_nodes is not None and node_id in job.cancelled_nodes:
        reason = job.cancelled_nodes[node_id]
        return NodeJobResult(node_id, cancelled=True, cancel_reason=reason)

    # Inject resolved inputs to args
    if inputs:
        # First, add inputs that match known parameter names in order
        kwargs.update({key: value for key, value in inputs.items() if key in params})

        # Then, append any remaining inputs that weren't in params
        args = tuple([value for key, value in inputs.items()
            if key not in params]) + args

    # Also provide the message queue so functions can send messages to main thread
    if job.message_queue is not None and "message_queue" in params:
        kwargs["message_queue"] = job.message_queue

    # Call the function
    try:
        result = func(*args, **kwargs)
        return NodeJobResult(node_id, result)
    except Exception as e:
        tb = traceback.format_exc()
        error_info = NodeError(tb, str(job.resolved_inputs)[:500], str(e))
        return NodeJobResult(node_id, error_info=error_info)
    finally:
        # Unregister PID on completion
        if job.node_pids is not None:
            job.node_pids.pop(node_id, None)

# -----------------------------------------------
# END FILE: node.py
# -----------------------------------------------