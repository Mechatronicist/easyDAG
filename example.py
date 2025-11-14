import time

from easyDAG import DistributedDAG, DAGNode


def process_data(x, message_queue=None, **kwargs):
    """Node function that sends messages during execution."""
    result = x * 2

    # Send progress update
    if message_queue:
        message_queue.put(('progress', {'node': 'process_data', 'status': 'running'}))

    # Do some work
    time.sleep(1)

    # Upload result to database asynchronously
    if message_queue:
        message_queue.put(('upload', {'table': 'results', 'data': result}))

    return result


def aggregate(inputs=None, message_queue=None, **kwargs):
    """Aggregate results from dependencies."""
    total = sum(inputs.values())

    if message_queue:
        message_queue.put(('upload', {'table': 'aggregates', 'data': total}))

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
    dag = DistributedDAG(processes=4)

    # Register message handlers BEFORE running
    dag.register_message_handler('upload', upload_to_database)
    dag.register_message_handler('progress', log_progress)

    # Build the DAG
    dag.add_node(DAGNode('A', process_data, args=(10,)))
    dag.add_node(DAGNode('B', process_data, args=(20,)))
    dag.add_node(DAGNode('C', aggregate))
    dag.add_edge('A', 'C')
    dag.add_edge('B', 'C')

    # Run - messages will be processed asynchronously
    outputs = dag.run()
    print(f"Final outputs: {outputs}")