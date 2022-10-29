from __future__ import annotations

from json import dumps
from typing import Union, TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
	from logging import Logger
	from munch import DefaultMunch

from tornado.websocket import WebSocketHandler

from .consumer import AsyncConsumer
from ..context import ConsumerContext
from ..command import *


class ConsumerWebSocketHandler(WebSocketHandler):
	def initialize(self, config: DefaultMunch, logger: Logger):
		self._id = uuid4()
		self._parser = CommandParserFactory.get_instance(config)
		self._async_consumer = None

		self._context = ConsumerContext()
		self._context.client_id = self.id
		self._context.logger = logger
		self._context.config = config
		self._context.cmd_queue = CommandQueue()
		self._context.out_queue = OutputQueue()

	def open(self):
		self._async_consumer = AsyncConsumer(self.context)
		self._async_consumer.start()
		self.logger.info("[{}] - Started async consumer".format(self.id))

	def on_close(self):
		self._async_consumer.stop()
		self.logger.info("[{}] - Consumer has been stopped".format(self.id))

	def on_message(self, message: Union[str, bytes]):
		message = self._parser.cleanup(message)
		try:
			cmd_input = self._parser.parse(message)

			cmd_instance = CommandFactory.get_instance(cmd_input.command_name, self.context)
			self.context.cmd_queue.put(QueuedCommand(cmd_instance, cmd_input.parameters))
			cmd_output = self.context.out_queue.pop()

			self.write_message(cmd_output)

		except Exception as e:
			self.write_message(self._make_error(e))

	@property
	def context(self) -> ConsumerContext:
		return self._context

	@property
	def config(self) -> DefaultMunch:
		return self.context.config

	@property
	def logger(self) -> Logger:
		return self.context.logger

	@property
	def id(self) -> str:
		return str(self._id)

	def _make_error(self, error):
		message = str(error)
		self.logger.error("[{}] - {}".format(self.id, message))
		return dumps({"error": message, "client_id": self.id})
