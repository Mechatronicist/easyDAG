import time
from dataclasses import dataclass, field
from multiprocessing import Queue as MPQueue
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class QueueMessage:
    """
    Structured message for multiprocessing queues.
    Serialized as a (message_type, payload) tuple so the listener remains unchanged.
    """
    type: str
    payload: Any = None
    node_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def stop_signal(cls):
        """Standard stop message."""
        return cls("__stop__")


@dataclass
class DAGQueue:
    """Type-safe wrapper for Manager.Queue() proxy."""

    _queue: MPQueue

    def put(self, msg: QueueMessage) -> None:
        if not isinstance(msg, QueueMessage):
            raise TypeError(
                f"TypedQueue.put expects QueueMessage, got {type(msg)}"
            )
        self._queue.put(msg)

    def get(self, *args, **kwargs) -> Any:
        return self._queue.get(*args, **kwargs)

    def __getattr__(self, name: str):
        queue_obj = object.__getattribute__(self, "_queue")
        return getattr(queue_obj, name)


@dataclass
class NodeJob:
    node_id: str
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    resolved_inputs: Optional[Dict[str, Any]]
    message_queue: Optional[DAGQueue]


@dataclass
class NodeError:
    traceback: str
    inputs: str
    exception: str


@dataclass
class NodeJobResult:
    node_id: str
    result: Optional[Any] = None
    error_info: Optional[NodeError] = None


@dataclass
class NodeSpec:
    id: str
    func: str
    parents: List[str] = field(default_factory=list)
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    max_retries: int = 0
