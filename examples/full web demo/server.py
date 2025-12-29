import threading
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

from src.EasyDAG import EasyDAG, MultiprocessQueue, DAGNode
from src.EasyDAGWeb import DagEventBus, DagEventEmitter

from dag import simple_process, process_data, aggregate
from interface import WebInterface

# DAG setup
queue = MultiprocessQueue()
dag = EasyDAG(processes=4, mp_queue=queue)

dag.add_node(DAGNode("A", process_data, args=(10,)))
dag.add_node(DAGNode("B", process_data, args=(20,)))
dag.add_node(DAGNode("C", aggregate))
dag.add_node(DAGNode("D", simple_process))

dag.add_edge("A", "C")
dag.add_edge("B", "C")
dag.add_edge("D", "C")

bus = DagEventBus()
emitter = DagEventEmitter(bus)
interface = WebInterface(dag, emitter)

app = FastAPI()

@app.get("/")
async def index():
    return HTMLResponse("<h1>EasyDAG Web Demo</h1>")

@app.websocket("/ws/dag")
async def dag_ws(ws: WebSocket):
    await ws.accept()

    async def sender(event):
        await ws.send_json(event)

    bus.register_sender(sender)

    try:
        while True:
            msg = await ws.receive_json()
            if msg["type"] == "run":
                threading.Thread(
                    target=interface.run_dag,
                    daemon=True
                ).start()
    finally:
        bus.unregister_sender(sender)
