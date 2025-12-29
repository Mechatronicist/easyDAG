import asyncio
from typing import Any, Dict


class DagEventEmitter:
    """
    Sync-safe wrapper around DagEventBus.
    """

    def __init__(self, bus):
        self.bus = bus

    def emit(self, event: Dict[str, Any]) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.bus.emit(event))
        except RuntimeError:
            asyncio.run(self.bus.emit(event))

    def node_started(self, node_id: str, **extra):
        self.emit({
            "type": "node_started",
            "nodeId": node_id,
            **extra,
        })

    def node_progress(self, node_id: str, progress: float, **extra):
        self.emit({
            "type": "node_progress",
            "nodeId": node_id,
            "progress": progress,
            **extra,
        })

    def node_finished(self, node_id: str, **extra):
        self.emit({
            "type": "node_finished",
            "nodeId": node_id,
            **extra,
        })

    def node_errored(self, node_id: str, error: str, **extra):
        self.emit({
            "type": "node_errored",
            "nodeId": node_id,
            "error": error,
            **extra,
        })
