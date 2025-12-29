import time
from src.EasyDAG import DAGNode, DAGQueue, QueueMessage

def simple_process():
    return

def process_data(x, message_queue: DAGQueue = None):
    result = x * 2

    if message_queue:
        message_queue.put(QueueMessage("progress", {
            "node": "process_data",
            "status": "running"
        }))

    time.sleep(x / 4)

    if message_queue:
        message_queue.put(QueueMessage("upload", {
            "table": "results",
            "data": result
        }))

    return result

def aggregate(a: int, b: int, message_queue: DAGQueue = None):
    total = a + b

    if message_queue:
        message_queue.put(QueueMessage("progress", (a, b)))
        message_queue.put(QueueMessage("upload", {
            "table": "aggregates",
            "data": total
        }))

    return total
