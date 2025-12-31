from EasyDAG import EasyDAG, DAGNode


def load_data():
    print("Loading data")
    return 5


def process_data(load):
    print(f"Processing {load}")
    return load * 2


def save_result(process):
    print(f"Saving result: {process}")


if __name__ == "__main__":
    # Create the DAG
    dag = EasyDAG(processes=2)

    # Define nodes
    dag.add_node(DAGNode("load", load_data))
    dag.add_node(DAGNode("process", process_data))
    dag.add_node(DAGNode("save", save_result))

    # Define dependencies
    dag.add_edge("load", "process")
    dag.add_edge("process", "save")

    # Run the DAG
    outputs = dag.run()

    print("\nDAG outputs:")
    for node_id, output in outputs.items():
        print(f"{node_id}: {output}")
