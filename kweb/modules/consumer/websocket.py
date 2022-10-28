from json import dumps
from logging import Logger
from typing import Union
from uuid import uuid4

from munch import DefaultMunch
from tornado.websocket import WebSocketHandler

from .consumer import AsyncConsumer
from ..command import *


class ConsumerWebSocketHandler(WebSocketHandler):
    def initialize(self, config: DefaultMunch, logger: Logger):
        self._id = uuid4()
        self._config = config
        self._logger = logger
        self._parser = CommandParserFactory.get_parser(config)
        self._async_consumer = None
        self._cmd_queue = None

    def open(self):
        self._cmd_queue = CommandQueue()

        self._async_consumer = AsyncConsumer(self.id, self._cmd_queue)
        self._async_consumer.start()
        self._logger.info("[{}] - Started async consumer".format(self.id))

    def on_close(self):
        self._async_consumer.stop()
        self._logger.info("[{}] - Consumer has been stopped".format(self.id))

    def on_message(self, message: Union[str, bytes]):
        message = self._parser.cleanup(message)

        try:
            cmd_input = self._parser.parse(message)
            cmd_instance = CommandName.get_class(cmd_input.command_name).value(self._config, self._logger)
            self._cmd_queue.put(QueuedCommand(cmd_instance, cmd_input.parameters))

        except Exception as e:
            self.write_message(self._make_error(e, self.id))

    @property
    def id(self):
        return str(self._id)

    def _make_error(self, error, client_id):
        message = str(error)
        self._logger.error("[{}] - {}".format(client_id, message))
        return dumps({"error": message, "client_id": client_id})
