# EasyDAG

**EasyDAG** is a lightweight, multiprocessing-friendly Directed Acyclic Graph (DAG) execution engine for Python.

It lets you define task nodes, declare dependencies, and execute them in parallel — while emitting structured lifecycle events and inter-process messages for logging, progress reporting, or external systems such as web dashboards.

EasyDAG is designed to be **simple, explicit, and embeddable**, without the operational overhead of workflow schedulers.

---

## Key Features

* ⚙️ Define DAGs using plain Python functions
* ⚡ Parallel execution via `multiprocessing`
* 🧠 Automatic dependency resolution
* 📬 Multiprocess-safe message queue for side effects
* 🧵 Message handlers run safely in the main process
* 🪝 Lifecycle hooks via a clean interface (ABC)
* 🛑 Cancellation, fail-fast, and timeout support
* 🌐 Optional WebSocket interface for live monitoring & control
* 📦 No external runtime dependencies for the core engine

---

## Installation

```bash
pip install easydag
```

---

## Quick Start

```python
from EasyDAG import EasyDAG, DAGNode

def task_a():
    return 2

def task_b(x):
    return x * 10

dag = EasyDAG(processes=4)

dag.add_node(DAGNode("A", task_a))
dag.add_node(DAGNode("B", task_b))

dag.add_edge("A", "B")

outputs = dag.run()
print(outputs)
```

---

## Core Concepts

### DAG

A **DAG** is a set of nodes with directed dependencies. A node may only execute once all of its dependencies have completed successfully.

EasyDAG guarantees:

* No node runs before its dependencies
* Each node runs at most once (unless retried)
* Independent nodes run in parallel

---

### DAGNode

Each node wraps:

* A callable function
* Positional and keyword arguments
* Retry configuration (optional)

```python
DAGNode(
    node_id="A",
    func=process_data,
    args=(10,),
    kwargs={"foo": "bar"},
    max_retries=2
)
```

Dependencies are resolved automatically by matching upstream node IDs to function parameters.

---

## Message Queue System (Side Effects)

EasyDAG includes an optional **multiprocessing-safe message queue** designed for side effects:

* Logging
* Progress updates
* Metrics
* Database writes
* External notifications

This keeps compute nodes pure and avoids unsafe shared state.

---

### Defining a Queue

```python
from EasyDAG import MultiprocessQueue

queue = MultiprocessQueue()
dag = EasyDAG(processes=4, mp_queue=queue)
```

---

### Registering Handlers (Main Process)

Handlers always run in the **main process**, never in workers.

```python
def log_progress(payload):
    print("Progress:", payload)

queue.register_message_handler("progress", log_progress)
```

---

### Sending Messages from Nodes

If a node function includes the reserved `message_queue` parameter, EasyDAG injects it automatically.

```python
def process_data(x, message_queue=None):
    message_queue.put(
        QueueMessage("progress", {"value": x})
    )
    return x * 2
```

If the parameter is omitted, the queue is not passed.

---

## Lifecycle Interface (Execution Hooks)

EasyDAG exposes a formal **interface abstraction** via an abstract base class:

```python
from EasyDAG import EasyInterface
```

This allows you to observe and control execution without coupling logic to the engine.

### Supported Hooks

* `dag_started`
* `dag_finished`
* `node_started`
* `node_progress`
* `node_finished`
* `node_errored`
* `cancel()`

You can implement your own interface to:

* Emit events
* Drive UIs
* Collect metrics
* Integrate APIs

---

## Cancellation & Fail-Fast

EasyDAG supports:

* **User-initiated cancellation**
* **Fail-fast execution**
* **Execution timeouts**

Cancellation halts scheduling of new nodes and can be configured to terminate or safely complete in-flight tasks.

Execution outcome is tracked explicitly via DAG status (success, failed, cancelled, timeout).

---

## WebSocket + FastAPI Demo

A full working example is included:

📁 `examples/full_web_demo/server.py`

### What the demo shows

* Building a DAG
* Emitting node & DAG lifecycle events
* Streaming events over WebSockets
* Starting and cancelling execution from the browser
* Viewing live progress in real time

---

### Running the Demo

From the project root:

```bash
uvicorn example.server:app --reload
```

Open:

```
http://localhost:8000
```

---

### Demo UI Features

* ▶️ Run DAG button
* ⏹ Cancel DAG button
* Live event stream
* WebSocket-driven execution control
* Browser-based monitoring

---

## When to Use EasyDAG

EasyDAG is ideal when you need:

* A **local, Python-native DAG engine**
* Parallel execution with dependencies
* Fine-grained control over execution
* Lightweight orchestration without infrastructure
* A simpler alternative to:

  * Airflow
  * Prefect
  * Ray
  * Dask

