from . import DistributedDAG, DAGNode

def task_a(inputs=None):
    return "result-A"


def task_b(inputs=None):
    # depends on A
    a = inputs.get("A")
    return f"B got {a}"


def task_c(inputs=None):
    # depends on A
    a = inputs.get("A")
    return f"C got {a}"


def task_d(inputs=None):
    # depends on B and C
    b = inputs.get("B")
    c = inputs.get("C")
    return f"D got {b} and {c}"


if __name__ == "__main__":
    # Define some simple top-level functions (must be picklable)

    dag = DistributedDAG(processes=3)
    dag.add_node(DAGNode("A", task_a))
    dag.add_node(DAGNode("B", task_b))
    dag.add_node(DAGNode("C", task_c))
    dag.add_node(DAGNode("D", task_d))

    dag.add_edge("A", "B")
    dag.add_edge("A", "C")
    dag.add_edge("B", "D")
    dag.add_edge("C", "D")

    outputs = dag.run()
    print("Outputs:")
    for k, v in outputs.items():
        print(k, "->", v)
