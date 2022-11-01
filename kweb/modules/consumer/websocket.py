from __future__ import annotations

import json
from json import dumps
from typing import TYPE_CHECKING, Union
from uuid import uuid4

from tornado.ioloop import IOLoop

if TYPE_CHECKING:
    from ..context import ApplicationContext

from tornado.websocket import WebSocketHandler

from .consumer import AsyncConsumer
from .validation import CommandParamsJsonValidator
from .command import CommandFactory
from ..command import QueuedCommand, CommandQueue
from ..context import ConsumerContext
from ..utils import Response


class ConsumerWebSocketHandler(WebSocketHandler):
    def initialize(self, app_context: ApplicationContext):
        self._id = uuid4()
        self._parser = app_context.parser 
        self._async_consumer = None

        self._ctx = ConsumerContext()
        self._ctx.logger = app_context.logger
        self._ctx.config = app_context.config
        self._ctx.client_id = self.id
        self._ctx.io_loop = IOLoop.current()
        self._ctx.cmd_queue = CommandQueue()
        self._ctx.on_actor_data = self._on_actor_data
        self._ctx.on_actor_error = self._on_actor_error
        self._ctx.command_validator = CommandParamsJsonValidator()

    @property
    def id(self) -> str:
        return str(self._id)

    @property
    def ctx(self) -> ConsumerContext:
        return self._ctx

    def open(self):
        self.ctx.logger.info("[{}] - Opened connection.".format(self.id))
        self._async_consumer = AsyncConsumer(self.ctx)
        self._async_consumer.start()

    def on_close(self):
        self.ctx.logger.info("[{}] - Connection closed.".format(self.id))
        self._async_consumer.stop()

    def on_message(self, message: Union[str, bytes]):
        try:
            parsed_cmd = self._parser.parse(message)
            cmd_instance = CommandFactory.get_instance(parsed_cmd.cmd_name, self.ctx)
            self.ctx.cmd_queue.put(QueuedCommand(cmd_instance, parsed_cmd.params))

        except Exception as e:
            self.write_message(dumps(Response.error(self.ctx, e)))

    def _on_actor_data(self, data):
        self.write_message(json.dumps(data))

    def _on_actor_error(self, e: Exception):
        self.write_message(Response.error(self.ctx, e))

