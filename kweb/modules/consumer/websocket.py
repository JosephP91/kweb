import sys
import logging
from munch import DefaultMunch
from logging import Logger
from json import dumps
from uuid import uuid4
from queue import Full

from tornado.websocket import WebSocketHandler

from .clients import Clients
from .thread import ConsumerThread

from ..command import *
from ..logger import LoggerFactory, LoggerType


class ConsumerWebSocketHandler(WebSocketHandler):
    _clients = Clients()

    def initialize(self, config: DefaultMunch, logger: Logger, parser: ICommandParser):
        self._id = uuid4()
        self._config = config
        self._logger = logger
        self._parser = parser

    def open(self):
        self._log_info("New connection received.", self.id)
        
        queue = CommandQueue()
        
        thread = ConsumerThread(self._config, self._logger, queue, self.id)
        thread.start()

        ConsumerWebSocketHandler._clients.add(self, queue, thread)

    def on_close(self):
        ConsumerWebSocketHandler._clients.remove(self)
        self._log_info("Closed connection.", self.id)

    def on_message(self, message):
        message = self._parser.cleanup(message)

        try:
            cmd_input = self._parser.parse(message)
            cmd_instance = CommandName.get_class(cmd_input.command_name).value(self._config, self._logger)

            client_queue = ConsumerWebSocketHandler._clients.get(self)["queue"]
            client_queue.put(QueuedCommand(cmd_instance, cmd_input.parameters))

        except Full:
            self._log_error("Cannot accept more messages from {}".format(self.id))
            self.write_message(self._make_error("Cannot accept more commands right now!", self.id))

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
        return dumps({"error": message, "client_id": str(client_id)})

    def _make_message(self, message: str, client_id: str):
        return dumps({"message": message, "client_id": str(client_id)})


