from queue import Empty
from munch import DefaultMunch
from logging import Logger
from threading import Thread

from ..command import CommandQueue


class ConsumerThread(Thread):
    def __init__(self, config: DefaultMunch, logger: Logger, queue: CommandQueue, client_id: str):
        super().__init__(name="ConsumerThread")

        self._config = config
        self._logger = logger
        self._queue = queue
        self._client_id = client_id
        self._should_stop = False

    def run(self):
        self._logger.info("Started thread for client {}".format(self._client_id))

        while not self._should_stop:
            try:
                queued_command = self._queue.pop(block=True, timeout=1)
                queued_command.command.execute(queued_command.parameters)

            except Empty:
                pass

        self._logger.info("Consumer thread {} has been stopped".format(self._client_id))

    def stop(self):
        self._should_stop = True

