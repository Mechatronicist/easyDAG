import threading
import time
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

from src.EasyDAG import EasyDAG, DAGNode, MultiprocessQueue, DAGQueue, QueueMessage, EasyInterface
from src.EasyDAGWeb import DagEventBus, DagEventEmitter, WebSocketManager

def simple_process():
    """Define a simple function that doesn't return anything, but should be run before its dependants"""
    return


def process_data(x, message_queue: DAGQueue = None):
    """Node function that sends messages during execution."""
    result = x * 2
    print("test", x)

    # Send progress update
    if message_queue:
        message_queue.put(QueueMessage('progress', {'node': 'process_data', 'status': 'running'}))

    # Do some work
    time.sleep(x/4)

    # Upload result to database asynchronously
    if message_queue:
        message_queue.put(QueueMessage('upload', {'table': 'results', 'data': result}))

    return result


def aggregate(a: int, b: int, message_queue: DAGQueue = None):
    """Aggregate results from dependencies."""
    total = a + b
    if message_queue:
        message_queue.put(QueueMessage('progress', (a, b)))
    if message_queue:
        message_queue.put(QueueMessage('upload', {'table': 'aggregates', 'data': total}))

    return total


# Define message handlers (run in main thread)
def upload_to_database(payload):
    """This runs in a thread in the main process, not blocking DAG execution."""
    table = payload['table']
    data = payload['data']
    print(f"Uploading to {table}: {data}")
    # db.insert(table, data)  # Your actual DB code
    time.sleep(0.5)  # Simulate DB operation


def log_progress(payload):
    """Log progress updates."""
    print(f"Progress: {payload}")

class MyInterface(EasyInterface):
    def __init__(self, e_dag, emitter):
        self.emitter = emitter
        super().__init__(e_dag)

    def _connected_callback(self, dag_id):
        self.emitter.emit({
            "type": "connected",
            "dagId": dag_id
        })
        self.dag.dag_id = dag_id

    def dag_started(self, dag_id, metadata=None):
        self.emitter.emit({
            "type": "dag_started",
            "dagId": dag_id,
            "metadata": metadata,
        })

    def dag_finished(self, dag_id, success, metadata=None):
        self.emitter.emit({
            "type": "dag_finished",
            "success": success,
            "metadata": str(metadata),
        })

    def node_started(self, node_id, metadata=None):
        self.emitter.node_started(node_id, metadata=metadata)

    def node_progress(self, node_id, progress, metadata=None):
        self.emitter.node_progress(node_id, progress, metadata=metadata)

    def node_finished(self, node_id, result=None, metadata=None):
        self.emitter.node_finished(node_id, metadata=metadata)

    def node_errored(self, node_id, error, metadata=None):
        self.emitter.node_errored(node_id, str(error), metadata=metadata)

# ===========================================================================

# Create DAG and register handlers
q = MultiprocessQueue()
dag = EasyDAG(processes=4, mp_queue=q)

# Register message handlers before running
q.register_message_handler('upload', upload_to_database)
q.register_message_handler('progress', log_progress)

# Build the DAG
dag.add_node(DAGNode('A', process_data, args=(10,)))
dag.add_node(DAGNode('B', process_data, args=(20,)))
dag.add_node(DAGNode('C', aggregate))
dag.add_node(DAGNode('D', simple_process))

dag.add_edge('A', 'C')
dag.add_edge('B', 'C')
dag.add_edge('D', 'C')

# Use optional interface for real-time view and control
bus = DagEventBus()
em = DagEventEmitter(bus)
ws_manager = WebSocketManager(bus)
interface = MyInterface(dag, em)

# Barebones websocket app - run and monitor from an external system
app = FastAPI()

@app.get("/")
async def index():
    return HTMLResponse("""
    <!doctype html>
    <html>
    <body>
      <h1>DAG Test</h1>
      <script>
        const ws = new WebSocket("ws://localhost:8000/ws/dag");

        ws.onopen = () => {
          console.log("WS connected");
          ws.send(JSON.stringify({ type: "run" }));
        };

        ws.onmessage = (e) => {
          console.log("EVENT:", JSON.parse(e.data));
        };
      </script>
    </body>
    </html>
    """)

@app.websocket("/ws/dag")
async def dag_ws(ws: WebSocket):
    await ws.accept()
    async def sender(event):
        await ws.send_json(event)
    bus.register_sender(sender)
    await bus.emit({"type": "test", "msg": "bus alive"})
    try:
        while True:
            msg = await ws.receive_json()
            if msg["type"] == "run":
                threading.Thread(
                    target=interface.run_dag,
                    kwargs={"interface": interface},
                    daemon=True
                ).start()
            elif msg["type"] == "cancel":
                interface.cancel()
    finally:
        bus.unregister_sender(sender)

if __name__ == '__main__':
    # Run inline - messages will be processed asynchronously
    outputs = dag.run()
    print(f"Final outputs: {outputs}")
