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
    def __init__(self, dag: DagInterface) -> None:
        self.dag: DagInterface = dag
        self.cancel_dag_flag = False
        self.dag_result = None
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
        """Called once when a DAG run completes."""
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

    # -----------------------
    # Optional control hooks
    # -----------------------

    def run_dag(self, dag_id, **kwargs) -> Any:
        """
        Label the dag with an interface ID and initiate DAG execution.
        """
        self.cancel_dag_flag = False
        self.dag.dag_id = dag_id
        self.dag_result = self.dag.run(interface=self, **kwargs)

    def cancel_dag(self):
        """
        Cancel DAG execution.
        """
        self.cancel_dag_flag = True

    def trim_dag(self, node_id: str):
        """
        Cancel a specific node and all dependents from running
        """
        # TODO: investigate and implement
        raise NotImplementedError
