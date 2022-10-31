from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from tornado.ioloop import IOLoop

if TYPE_CHECKING:
    from logging import Logger
    from munch import DefaultMunch
    from kafka import KafkaConsumer

    from ..command import CommandQueue, OutputQueue


class Context:
    def __init__(self):
        self._client_id = None
        self._logger = None
        self._config = None

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def config(self) -> DefaultMunch:
        return self._config

    @client_id.setter
    def client_id(self, client_id: str):
        self._client_id = client_id

    @logger.setter
    def logger(self, logger: Logger):
        self._logger = logger

    @config.setter
    def config(self, config: DefaultMunch):
        self._config = config


class ConsumerContext(Context):
    def __init__(self):
        super().__init__()
        self._cmd_queue = None
        self._out_queue = None
        self._consumer = None
        self._io_loop = None
        self._on_data_available = None
        self._on_consumer_error = None

    @property
    def cmd_queue(self) -> CommandQueue:
        return self._cmd_queue

    @property
    def out_queue(self) -> OutputQueue:
        return self._out_queue

    @property
    def consumer(self) -> KafkaConsumer:
        return self._consumer

    @property
    def io_loop(self) -> IOLoop:
        return self._io_loop

    @property
    def on_consumer_data(self) -> Callable:
        return self._on_data_available

    @property
    def on_consumer_error(self) -> Callable:
        return self.on_consumer_error

    @cmd_queue.setter
    def cmd_queue(self, cmd_queue: CommandQueue):
        self._cmd_queue = cmd_queue

    @out_queue.setter
    def out_queue(self, out_queue: OutputQueue):
        self._out_queue = out_queue

    @consumer.setter
    def consumer(self, consumer: KafkaConsumer):
        self._consumer = consumer

    @io_loop.setter
    def io_loop(self, io_loop: IOLoop):
        self._io_loop = io_loop

    @on_consumer_data.setter
    def on_consumer_data(self, on_consumer_data: Callable):
        self._on_data_available = on_consumer_data

    @on_consumer_error.setter
    def on_consumer_error(self, on_consumer_error: Callable):
        self._on_consumer_error = on_consumer_error
