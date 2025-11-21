import time
from EasyDAG import EasyDAG, DAGNode, MultiprocessQueueWatcher, DAGQueue, QueueMessage


def pull_data():
    return 10


def process_data(x, message_queue: DAGQueue = None):
    """Node function that sends messages during execution."""
    result = x * 2
    print("test", x)

    # Send progress update
    if message_queue:
        message_queue.put(QueueMessage('progress', {'node': 'process_data', 'status': 'running'}))

    # Do some work
    time.sleep(1)

    # Upload result to database asynchronously
    if message_queue:
        message_queue.put(QueueMessage('upload', {'table': 'results', 'data': result}))

    return result


def aggregate(a: int, b: int, d, message_queue: DAGQueue = None):
    """Aggregate results from dependencies."""
    total = a + b + d
    if message_queue:
        message_queue.put(QueueMessage('progress', (a, b, d)))
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


if __name__ == '__main__':
    # Create DAG and register handlers
    q = MultiprocessQueueWatcher()
    dag = EasyDAG(processes=4, watch_queue=q)

    # Register message handlers before running
    q.register_message_handler('upload', upload_to_database)
    q.register_message_handler('progress', log_progress)

    # Build the DAG
    dag.add_node(DAGNode('A', process_data, args=(10,)))
    dag.add_node(DAGNode('B', process_data, args=(20,)))
    dag.add_node(DAGNode('C', aggregate))
    dag.add_node(DAGNode('D', pull_data))

    dag.add_edge('A', 'C')
    dag.add_edge('B', 'C')
    dag.add_edge('D', 'C')

    # Run - messages will be processed asynchronously
    outputs = dag.run()
    print(f"Final outputs: {outputs}")
