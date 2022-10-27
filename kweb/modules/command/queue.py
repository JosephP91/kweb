from queue import Queue

from .command import ICommand


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

    def put(self, command: QueuedCommand, block=False, timeout:int=None):
        self._queue.put(command, block, timeout)

    def pop(self, block=False, timeout:int=None) -> ICommand:
        return self._queue.get(block, timeout)

    def __len__(self) -> int:
        return self._queue.qsize()

