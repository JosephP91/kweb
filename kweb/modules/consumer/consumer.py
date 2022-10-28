from logging import Logger

from munch import DefaultMunch
from tornado.ioloop import IOLoop

from ..command import CommandQueue, CommandQueueEmptyException


class AsyncConsumer:
    def __init__(self, config: DefaultMunch, logger: Logger, queue: CommandQueue, client_id: str):
        self._config = config
        self._logger = logger
        self._queue = queue
        self._client_id = client_id
        self._should_stop = False

    def start(self):
        IOLoop.current().run_in_executor(None, self._callback)

    def stop(self):
        self._should_stop = True

    def _callback(self):
        self._logger.info("Started thread for client {}".format(self._client_id))
        while not self._should_stop:
            try:
                queued_command = self._queue.pop(block=True, timeout=1)
                queued_command.command.execute(queued_command.parameters)

            except CommandQueueEmptyException:
                pass

        self._logger.info("Consumer thread {} has been stopped".format(self._client_id))
