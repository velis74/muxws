"""A logging handler that hands records to a background thread, which appends them to a file."""

from __future__ import annotations

import atexit
import logging
import queue

from logging.handlers import QueueHandler, QueueListener


class QueuedFileHandler(QueueHandler):
    """A `QueueHandler` whose listener thread writes each message to `filename`, one per line.

    `emit()` only puts the record on a queue, so a handler on the event loop does no file I/O.
    """

    def __init__(self, filename: str, encoding: str = "utf-8") -> None:
        records: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
        super().__init__(records)
        target = logging.FileHandler(filename, encoding=encoding)
        target.setFormatter(logging.Formatter("%(message)s"))
        self.listener = QueueListener(records, target)
        self.listener.start()
        self._listening = True
        atexit.register(self.close)

    def close(self) -> None:
        """Write the queued records, stop the thread and close the file."""
        if self._listening:
            self._listening = False
            self.listener.stop()
            for handler in self.listener.handlers:
                handler.close()
        super().close()
