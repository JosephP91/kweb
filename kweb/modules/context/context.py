from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from logging import Logger
    from munch import DefaultMunch
    from kafka import KafkaConsumer

    from ..command import CommandQueue, OutputQueue


class Context:
    def __init__(self):
        self._logger = None
        self._config = None

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def config(self) -> DefaultMunch:
        return self._config

    @logger.setter
    def logger(self, logger: Logger):
        self._logger = logger

    @config.setter
    def config(self, config: DefaultMunch):
        self._config = config


class ConsumerContext(Context):
    def __init__(self):
        super().__init__()
        self._client_id = None
        self._cmd_queue = None
        self._out_queue = None
        self._consumer = None

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def cmd_queue(self) -> CommandQueue:
        return self._cmd_queue

    @property
    def out_queue(self) -> OutputQueue:
        return self._out_queue

    @property
    def consumer(self) -> KafkaConsumer:
        return self._consumer

    @client_id.setter
    def client_id(self, client_id):
        self._client_id = client_id

    @cmd_queue.setter
    def cmd_queue(self, cmd_queue: CommandQueue):
        self._cmd_queue = cmd_queue

    @out_queue.setter
    def out_queue(self, out_queue: OutputQueue):
        self._out_queue = out_queue

