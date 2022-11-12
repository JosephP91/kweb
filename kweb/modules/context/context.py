from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from tornado.ioloop import IOLoop

if TYPE_CHECKING:
    from logging import Logger
    from munch import DefaultMunch
    from kafka import KafkaConsumer

    from ..command import CommandQueue, AbstractCommandParser, IValidator


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


class ApplicationContext(Context):
    def __init__(self):
        super().__init__()
        self._parser = None

    @property
    def parser(self) -> AbstractCommandParser:
        return self._parser

    @parser.setter
    def parser(self, parser: AbstractCommandParser):
        self._parser = parser


class ActorContext(Context):
    def __init__(self):
        super().__init__()
        self._cmd_queue = None
        self._io_loop = None
        self._on_actor_data = None
        self._on_actor_error = None
        self._command_validator = None

    @property
    def cmd_queue(self) -> CommandQueue:
        return self._cmd_queue

    @property
    def io_loop(self) -> IOLoop:
        return self._io_loop

    @property
    def on_actor_data(self) -> Callable:
        return self._on_actor_data

    @property
    def on_actor_error(self) -> Callable:
        return self.on_actor_error

    @property
    def command_validator(self) -> IValidator:
        return self._command_validator

    @cmd_queue.setter
    def cmd_queue(self, cmd_queue: CommandQueue):
        self._cmd_queue = cmd_queue

    @io_loop.setter
    def io_loop(self, io_loop: IOLoop):
        self._io_loop = io_loop

    @on_actor_data.setter
    def on_actor_data(self, on_consumer_data: Callable):
        self._on_actor_data = on_consumer_data

    @on_actor_error.setter
    def on_actor_error(self, on_consumer_error: Callable):
        self._on_actor_error = on_consumer_error

    @command_validator.setter
    def command_validator(self, command_validator: IValidator):
        self._command_validator = command_validator


class ConsumerContext(ActorContext):
    def __init__(self):
        super().__init__()
        self._consumer = None

    @property
    def consumer(self) -> KafkaConsumer:
        return self._consumer

    @consumer.setter
    def consumer(self, consumer: KafkaConsumer):
        self._consumer = consumer
