from __future__ import annotations

from queue import Queue, Full, Empty
from typing import TYPE_CHECKING

from .exception import CommandQueueFullException, CommandQueueEmptyException

if TYPE_CHECKING:
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


class OutputQueue:
	def __init__(self, maxsize=1):
		self._queue = Queue(maxsize=maxsize)

	def put(self, output: dict):
		self._queue.put(output, block=True)

	def pop(self) -> dict:
		return self._queue.get(block=True)

	def __len__(self) -> int:
		return self._queue.qsize()
