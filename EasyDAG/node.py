import inspect
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

from EasyDAG.types import NodeJob, NodeJobResult, NodeError


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
        func: a picklable callable. It will be called as func(*args, **kwargs)
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

    Expects a tuple: (node_id, func, args, kwargs, resolved_inputs, message_queue)
    Returns: (node_id, result | None, error_info | None)
    """
    node_id = job.node_id
    func = job.func
    args = job.args
    kwargs = job.kwargs
    # Provide inputs as kwargs
    if job.resolved_inputs:
        kwargs.update(job.resolved_inputs)
    # Also provide the message queue so functions can send messages to main thread
    if job.message_queue is not None and "message_queue" in inspect.signature(func).parameters:
        kwargs["message_queue"] = job.message_queue
    # Call the function
    try:
        result = func(*args, **kwargs)
        return NodeJobResult(node_id, result)
    except Exception as e:
        tb = traceback.format_exc()
        error_info = NodeError(tb, str(job.resolved_inputs)[:500], str(e))
        return NodeJobResult(node_id, error_info=error_info)
