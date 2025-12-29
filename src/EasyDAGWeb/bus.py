import asyncio
from typing import Any, Awaitable, Callable, Dict, Set


class DagEventBus:
    """
    Fan-out event bus.
    Allows multiple transports (WS, logs, etc.) to receive DAG events.
    """

    def __init__(self):
        self._senders: Set[Callable[[Dict[str, Any]], Awaitable[None]]] = set()

    def register_sender(
        self,
        sender: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._senders.add(sender)

    def unregister_sender(
        self,
        sender: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._senders.discard(sender)

    async def emit(self, event: Dict[str, Any]) -> None:
        if not self._senders:
            return

        await asyncio.gather(
            *(sender(event) for sender in list(self._senders)),
            return_exceptions=True,
        )
