from queue import Queue, Full, Empty

from .command import ICommand
from .exception import CommandQueueFullException, CommandQueueEmptyException


class QueuedCommand:
    def __init__(self, command: ICommand, parameters: dict):
        self._command = command
        self._parameters = parameters

    @property
    def command(self) -> ICommand:
        return self._command

    @property
    def parameters(self) -> dict:
        return self._parameters


class CommandQueue:
    def __init__(self, maxsize=5):
        self._queue = Queue(maxsize=maxsize)
        self._maxsize = maxsize

    def put(self, command: QueuedCommand, block=False, timeout: int = None):
        try:
            self._queue.put(command, block, timeout)
        except Full:
            raise CommandQueueFullException("Cannot accept more commands right now!")

    def pop(self, block=False, timeout: int = None) -> QueuedCommand:
        try:
            return self._queue.get(block, timeout)
        except Empty:
            raise CommandQueueEmptyException("No more command to process!")

    def __len__(self) -> int:
        return self._queue.qsize()
