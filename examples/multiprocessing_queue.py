import time
from src.EasyDAG import EasyDAG, DAGNode, MultiprocessQueue, DAGQueue, QueueMessage


# -----------------------------
# Message handlers (main process)
# -----------------------------

def log_progress(payload):
    print(f"[PROGRESS] {payload}")


def upload_result(payload):
    table = payload["table"]
    data = payload["data"]
    print(f"[UPLOAD] table={table} data={data}")
    time.sleep(0.3)  # simulate I/O


# -----------------------------
# DAG tasks (worker processes)
# -----------------------------

def process_data(x, message_queue: DAGQueue = None):
    if message_queue:
        message_queue.put(
            QueueMessage("progress", {"step": "start", "value": x})
        )

    time.sleep(1)
    result = x * 2

    if message_queue:
        message_queue.put(
            QueueMessage(
                "upload",
                {"table": "results", "data": result}
            )
        )

    return result


def aggregate(a, b):
    return a + b


# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":
    # Create the queue
    queue = MultiprocessQueue()

    # Register handlers
    queue.register_message_handler("progress", log_progress)
    queue.register_message_handler("upload", upload_result)

    # Create DAG
    dag = EasyDAG(processes=2, mp_queue=queue)

    dag.add_node(DAGNode("A", process_data, args=(10,)))
    dag.add_node(DAGNode("B", process_data, args=(20,)))
    dag.add_node(DAGNode("C", aggregate))

    dag.add_edge("A", "C")
    dag.add_edge("B", "C")

    outputs = dag.run()

    print("\nFinal outputs:")
    print(outputs)
