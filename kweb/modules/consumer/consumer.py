from logging import Logger

from munch import DefaultMunch
from tornado.ioloop import IOLoop

from ..command import CommandQueue, CommandQueueEmptyException
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
                queued_command.command.execute(queued_command.parameters)

            except CommandQueueEmptyException:
                self._context.logger.info("Queue is empty!")
                pass

