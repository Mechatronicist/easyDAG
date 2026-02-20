# -----------------------------------------------
# FILE: interface.py
# -----------------------------------------------

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable

class DagInterface(ABC):
    dag_id: Optional[str]

    def run(self, timeout: Optional[float] = None,
            cache_dir: Optional[str] = None,
            progress_callback: Optional[Callable[[int, int, str], None]] = None,
            interface: Optional["EasyInterface"] = None):
        pass


class EasyInterface(ABC):
    """
    Abstract interface for observing and controlling DAG execution.

    Implementations may forward events to:
    - Web APIs
    - WebSockets
    - Databases
    - Logs
    - Message queues
    """
    cancel_graceful: bool = True
    cancel_dag_flag: str | None = None
    dag: DagInterface
    dag_result: Optional[Any]

    # Injected by EasyDAG.run() so trim_dag can reach live shared state
    _trim_dag_impl: Optional[Callable[[str, str], None]] = None

    def __init__(self, dag: DagInterface) -> None:
        self.dag = dag

    # -----------------------
    # DAG lifecycle
    # -----------------------

    @abstractmethod
    def dag_started(
            self,
            dag_id: str,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Called once when a DAG run begins."""
        raise NotImplementedError

    @abstractmethod
    def dag_finished(
            self,
            dag_id: str,
            success: bool,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Called once when a DAG run completes or cancels."""
        raise NotImplementedError

    # -----------------------
    # Node lifecycle
    # -----------------------

    @abstractmethod
    def node_started(
            self,
            node_id: str,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Node execution started."""
        raise NotImplementedError

    @abstractmethod
    def node_progress(
            self,
            node_id: str,
            progress: float,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Node progress update (0.0 → 1.0)."""
        raise NotImplementedError

    @abstractmethod
    def node_finished(
            self,
            node_id: str,
            result: Optional[Any] = None,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Node completed successfully."""
        raise NotImplementedError

    @abstractmethod
    def node_errored(
            self,
            node_id: str,
            error: str,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Node failed."""
        raise NotImplementedError

    @abstractmethod
    def node_cancelled(
            self,
            node_id: str,
            reason: str,
            metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Node cancelled."""
        raise NotImplementedError

    # -----------------------
    # Control hooks
    # -----------------------

    def run_dag(self, **kwargs) -> Any:
        """Label the dag with an interface ID and initiate DAG execution."""
        self.cancel_dag_flag = None
        self._trim_dag_impl = None  # reset before each run
        self.dag_result = self.dag.run(interface=self, **kwargs)

    def cancel_dag(self, cancel_message: Optional[str], graceful: bool = True) -> None:
        """Cancel the entire DAG execution."""
        if not cancel_message:
            cancel_message = "Canceled"
        self.cancel_graceful = graceful
        self.cancel_dag_flag = cancel_message

    def trim_dag(self, node_id: str, reason: str = "Trimmed") -> None:
        """
        Cancel a specific node and all of its descendants.

        Rules:
        - If the node has already completed successfully, this call is a no-op
          for that node, but any not-yet-started descendants will still be cancelled.
        - If the node is currently running, its worker process is sent SIGTERM and
          the result (if it arrives) is discarded.
        - Nodes that do not transitively depend on `node_id` are not affected.
        - Can be called from any thread (e.g. a background monitoring thread)
          while the DAG is executing.

        Raises RuntimeError if called before run_dag() has started (no active run).
        """
        if self._trim_dag_impl is None:
            raise RuntimeError(
                "trim_dag() called outside of an active DAG run. "
                "Call run_dag() first."
            )
        self._trim_dag_impl(node_id, reason)

# -----------------------------------------------
# END FILE: interface.py
# -----------------------------------------------