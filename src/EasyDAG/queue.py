import queue
import threading
import warnings
from typing import Any, Callable, Dict, Optional

from .dag_types import DAGQueue, QueueMessage


class MultiprocessQueue:
    def __init__(self):
        self.message_handlers: Dict[str, Callable] = {}
        self._listener_thread: Optional[threading.Thread] = None
        self._stop_listener = threading.Event()
        self._queue: Optional[DAGQueue] = None

    def register_queue(self, q: DAGQueue):
        self._queue = q

    def register_message_handler(self, message_type: str, handler: Callable[[Any], None]) -> None:
        """Register a handler for a specific message type from child processes.

        Args:
            message_type: string identifier for the message type
            handler: callable that takes the message payload as argument
                     Handler will be executed in a separate thread in the main process

        Example:
            def upload_to_db(data):
                # This runs in a thread in the main process
                db.insert(data)

            dag.register_message_handler('upload', upload_to_db)

            # In your node function:
            def my_task(message_queue=None, **kwargs):
                result = do_work()
                if message_queue:
                    message_queue.put(('upload', result))
                return result
        """
        self.message_handlers[message_type] = handler

    def start_message_listener(self) -> None:
        """Start the message listener thread."""
        if self._queue:
            self._stop_listener.clear()
            self._listener_thread = threading.Thread(target=self._message_listener, daemon=True)
            self._listener_thread.start()

    def stop_message_listener(self, timeout: Optional[int] = None) -> None:
        """Stop the message listener thread."""
        if self._listener_thread is not None:
            self._stop_listener.set()
            # Send sentinel to unblock the listener
            self._queue.put(QueueMessage.stop_signal())
            self._listener_thread.join(timeout=timeout)
            self._listener_thread = None

    def _message_listener(self) -> None:
        """Listen for messages from child processes and dispatch handlers in threads."""
        while not self._stop_listener.is_set():
            try:
                # Timeout to check stop flag periodically
                message: QueueMessage = self._queue.get(timeout=0.1)

                if message.type == QueueMessage.stop_signal().type:
                    break

                # Check if we have a handler for this message type
                if message.type in self.message_handlers:
                    handler = self.message_handlers[message.type]
                    handler(message.payload)
                else:
                    print(f"Warning: No handler registered for message type '{message.type}'")
            except queue.Empty:
                # Timeout
                continue
            except Exception as e:
                warnings.warn(f"The message queue was forced closed because of an error:\n{e}")
                self.stop_message_listener(0)
