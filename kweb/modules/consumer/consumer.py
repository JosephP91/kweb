from __future__ import annotations

from typing import TYPE_CHECKING

from tornado.ioloop import IOLoop

from ..command import CommandQueueEmptyException

if TYPE_CHECKING:
	from ..context import ConsumerContext


class AsyncConsumer:
	def __init__(self, context: ConsumerContext):
		self._should_stop = False
		self._context = context

	def start(self):
		IOLoop.current().run_in_executor(None, self._callback)

	def stop(self):
		self._should_stop = True

	def _callback(self):
		while not self._should_stop:
			try:
				queued_command = self._context.cmd_queue.pop(block=True, timeout=1)
				cmd_output = queued_command.command.execute(queued_command.parameters)
				self._context.out_queue.put(cmd_output)

			except CommandQueueEmptyException:
				if self._context.consumer is not None:
					try:
						cmd_output = self._context.consumer.poll()
						if len(cmd_output) > 0:
							IOLoop.spawn_callback(self._context.io_loop, self._context.on_consumer_data, cmd_output)
					except Exception as e:
						IOLoop.spawn_callback(self._context.io_loop, self._context.on_consumer_error, e)

		if self._context.consumer is not None:
			self._context.consumer.close(autocommit=False)
