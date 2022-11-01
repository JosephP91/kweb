from __future__ import annotations

import json
from json import dumps
from typing import TYPE_CHECKING
from uuid import uuid4

from tornado.ioloop import IOLoop

if TYPE_CHECKING:
    from ..context import ApplicationContext

from tornado.websocket import WebSocketHandler

from .consumer import AsyncConsumer
from .command import CommandFactory
from ..command import QueuedCommand, CommandQueue, OutputQueue
from ..context import ConsumerContext
from ..utils import Response


class ConsumerWebSocketHandler(WebSocketHandler):
    def initialize(self, app_context: ApplicationContext):
        self._id = uuid4()
        self._parser = app_context.parser 
        self._async_consumer = None

        self._ctx = ConsumerContext()
        self._ctx.client_id = self.id
        self._ctx.io_loop = IOLoop.current()
        self._ctx.logger = app_context.logger
        self._ctx.config = app_context.config
        self._ctx.cmd_queue = CommandQueue()
        self._ctx.out_queue = OutputQueue()
        self._ctx.on_consumer_data = self._on_consumer_data
        self._ctx.on_consumer_error = self._on_consumer_error

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
            message = self._parser.cleanup(message)
            parsed_cmd = self._parser.parse(message)

            cmd_instance = CommandFactory.get_instance(parsed_cmd.cmd_name, self.ctx)
            self.ctx.cmd_queue.put(QueuedCommand(cmd_instance, parsed_cmd.params))
            cmd_output = self.ctx.out_queue.pop()

            self.write_message(cmd_output)

        except Exception as e:
            self.write_message(dumps(Response.error(self.ctx, e)))

    def _on_consumer_data(self, data):
        self.write_message(json.dumps(data))

    def _on_consumer_error(self, e: Exception):
        self.write_message(dumps(Response.error(self.ctx, e)))

