from collections import namedtuple
from queue import Queue, Full, Empty

from .exception import CommandQueueFullException, CommandQueueEmptyException

QueuedCommand = namedtuple("QueuedCommand", ["command", "parameters"])


class CommandQueue:
    def __init__(self, maxsize=5):
        self._queue = Queue(maxsize=maxsize)

    def put(self, command: QueuedCommand, block=False, timeout: int = None):
        try:
            self._queue.put(command, block, timeout)
        except Full:
            raise CommandQueueFullException()

    def pop(self, block=False, timeout: int = None) -> QueuedCommand:
        try:
            return self._queue.get(block, timeout)
        except Empty:
            raise CommandQueueEmptyException()

    def __len__(self) -> int:
        return self._queue.qsize()
