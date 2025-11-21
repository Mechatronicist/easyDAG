# EasyDAG

A lightweight, multiprocessing-friendly Directed Acyclic Graph (DAG) execution engine for Python.
EasyDAG lets you define tasks (nodes), their dependencies, and run them in parallel—while sending structured messages back to the main thread for logging, progress reporting, or asynchronous operations such as database uploads.

---

## Features

- 🔧 Define DAG nodes with simple Python callables
- ⚡ Parallel execution with multiprocessing
- 📬 Built-in message queue system for asynchronous logging or side-effects
- 🔄 Thread-safe message handlers running in the main process
- 🧩 Automatic dependency resolution
- 🧵 Clean separation of compute tasks and side effects
- 🛠 Minimal boilerplate, no external dependencies

---

## Installation

`pip install EasyDAG`

---

## Quick Example

Check out the included [example](example.py) demonstrating:
- Defining DAG nodes
- Defining a Message Queue
- Registering message handler functions
- Sending event messages
- Running tasks in parallel

---

## How It Works

### 1. Define the class

`dag = EasyDAG(processes, watch_queue)`
Params
- processes (optional): the maximum number of child processes to spawn.
- watch_queue (optional): a `MultiprocessQueueWatcher()` object (see **Message Queue System** below).

### 2. Define nodes using DAGNode

Each node wraps a callable function plus its arguments.

`DAGNode("A", process_data, args=(10,))`

### 3. Add them to the DAG 

`dag.add_node(...)`

### 4. Establish dependencies
`dag.add_edge("A", "C")  # C depends on A`

### 5. Run the DAG
`outputs = dag.run()`

---

## Message Queue System

EasyDAG provides the option for a lightweight interprocess message queue, enabling:
- live progress updates
- logging
- metrics
- events dispatched during task execution
- asynchronous DB writes
- keeping compute nodes pure (separation of concerns)


### 1. Define a queue

`q = MultiprocessQueueWatcher()`

### 2. Handlers are registered like:

`q.register_message_handler("handler_name", handler_function)`

### 3. Nodes can send messages:
The `message_queue` parameter in a dag node function is reserved for the queue. 
If no queue is defined, `message_queue` is `None`.
If it is excluded from the function, the queue won't be passed.

`message_queue.put(QueueMessage("handler_name", {handler_function_kwargs}))`

---

## When to Use EasyDAG

EasyDAG is ideal when you need:
- A parallel task pipeline with dependencies
- A simple alternative to Airflow, Prefect, Ray, or Dask
- Local, lightweight, Python-native DAG execution
- A multiprocessing-safe message bus for side effects
- To run independent tasks in parallel and merge their outputs


