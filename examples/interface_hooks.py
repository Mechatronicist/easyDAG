from typing import Optional, Dict, Any

from src.EasyDAG import EasyDAG, DAGNode, EasyInterface


# -----------------------------
# DAG tasks
# -----------------------------

def load_data():
    return 3


def process_data(load):
    return load * 4


def save_result(process):
    print(f"Saved result: {process}")


# -----------------------------
# Interface implementation
# -----------------------------

class LoggingInterface(EasyInterface):
    """Simple interface that logs DAG and node lifecycle events."""

    def dag_started(self, dag_id, metadata=None):
        print(f"[DAG STARTED] id={dag_id}")

    def dag_finished(self, dag_id, success, metadata=None):
        status = "SUCCESS" if success else "FAILED"
        print(f"[DAG FINISHED] id={dag_id} status={status}")

    def node_started(self, node_id, metadata=None):
        print(f"  [NODE STARTED] {node_id}")

    def node_progress(self, node_id: str, progress: float, metadata=None) -> None:
        print(f"  [NODE PROGRESSED] {node_id}")

    def node_finished(self, node_id, result=None, metadata=None):
        print(f"  [NODE FINISHED] {node_id} -> {result}")

    def node_errored(self, node_id, error, metadata=None):
        print(f"  [NODE ERROR] {node_id}: {error}")


# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":
    dag = EasyDAG(processes=2)

    dag.add_node(DAGNode("load", load_data))
    dag.add_node(DAGNode("process", process_data))
    dag.add_node(DAGNode("save", save_result))

    dag.add_edge("load", "process")
    dag.add_edge("process", "save")

    # Attach the interface
    interface = LoggingInterface(dag)

    outputs = dag.run(interface=interface)

    print("\nFinal outputs:")
    print(outputs)
