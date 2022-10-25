import sys
import logging
from munch import DefaultMunch
from logging import Logger
from json import dumps
from uuid import uuid4

from tornado.websocket import WebSocketHandler

from .clients import Clients
from ..logger import LoggerFactory, LoggerType
from ..command import ICommandParser, ParserException, CommandName


class ConsumerWebSocketHandler(WebSocketHandler):
    _clients = Clients()

    def initialize(self, config: DefaultMunch, logger: Logger, parser: ICommandParser):
        self._id = uuid4()
        self._config = config
        self._logger = logger
        self._parser = parser 

    def open(self):
        ConsumerWebSocketHandler._clients.add(self)
        self._log_info("New connection received.", self.id)

    def on_close(self):
        ConsumerWebSocketHandler._clients.remove(self)
        self._log_info("Closed connection.", self.id)

    def on_message(self, message):
        message = self._parser.cleanup(message)
        self._log_info("Received message '{}'".format(message), self.id)

        try:
            cmd_input = self._parser.parse(message)
            cmd_instance = CommandName[cmd_input.command_name.upper()].value(self._config, self._logger)
            cmd_instance.execute(cmd_input.parameters)
        
        except KeyError:
            error_message = "Unsupported command: '{}'".format(cmd_input.command_name)
            self.write_message(self._make_error(error_message, self.id))

        except ParserException as exception:
            self.write_message(self._make_error(str(exception), self.id))

    @property
    def id(self):
        return self._id

    def _log_info(self, message: str, client_id: str):
        self._logger.info("[{}] - {}".format(client_id, message))

    def _log_error(self, message: str, client_id: str):
        self._logger.error("[{}] - {}".format(client_id, message))

    def _make_error(self, message: str, client_id: str):
        self._log_error(message, client_id)
        return dumps({"error": message})
