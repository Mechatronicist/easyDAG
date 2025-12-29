from src.EasyDAG import EasyInterface

class WebInterface(EasyInterface):
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
