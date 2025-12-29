from fastapi import WebSocket
from .bus import DagEventBus


class WebSocketManager:
    def __init__(self, bus: DagEventBus):
        self.bus = bus

    async def connect(self, ws: WebSocket):
        await ws.accept()

        async def sender(event):
            await ws.send_json(event)

        ws._sender = sender
        self.bus.register_sender(sender)

    async def disconnect(self, ws: WebSocket):
        self.bus.unregister_sender(ws._sender)
