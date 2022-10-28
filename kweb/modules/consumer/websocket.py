from json import dumps
from logging import Logger
from typing import Union
from uuid import uuid4

from munch import DefaultMunch
from tornado.websocket import WebSocketHandler

from .clients import Clients
from .consumer import AsyncConsumer
from ..command import *


class ConsumerWebSocketHandler(WebSocketHandler):
	_clients = Clients()

	def initialize(self, config: DefaultMunch, logger: Logger):
		self._id = uuid4()
		self._config = config
		self._logger = logger
		self._parser = CommandParserFactory.get_parser(config)

	def open(self):
		self._logger.info("[{}] - New connection received!".format(self.id))

		cmd_queue = CommandQueue()

		consumer = AsyncConsumer(self._config, self._logger, cmd_queue, self.id)
		consumer.start()

		ConsumerWebSocketHandler._clients.add(self, cmd_queue, consumer)

	def on_close(self):
		ConsumerWebSocketHandler._clients.remove(self)
		self._logger.info("[{}] - Connection closed!".format(self.id))

	def on_message(self, message: Union[str, bytes]):
		message = self._parser.cleanup(message)

		try:
			cmd_input = self._parser.parse(message)
			cmd_instance = CommandName.get_class(cmd_input.command_name).value(self._config, self._logger)

			client_queue = ConsumerWebSocketHandler._clients.get(self)["queue"]
			client_queue.put(QueuedCommand(cmd_instance, cmd_input.parameters))

		except CommandQueueFullException as e:
			self.write_message(self._make_error(str(e), self.id))

		except UnsupportedCommandException as e:
			self.write_message(self._make_error(str(e), self.id))

		except ParserException as e:
			self.write_message(self._make_error(str(e), self.id))

	@property
	def id(self):
		return str(self._id)

	def _make_error(self, message: str, client_id: str):
		self._logger.error("[{}] - {}".format(client_id, message))
		return dumps({"error": message, "client_id": client_id})
