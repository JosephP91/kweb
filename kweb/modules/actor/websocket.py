from __future__ import annotations

import abc
import json
from typing import TYPE_CHECKING, Optional, Awaitable, Union
from uuid import uuid4

from tornado.ioloop import IOLoop
from tornado.websocket import WebSocketHandler, WebSocketClosedError

from ..command import CommandQueue, QueuedCommand
from ..utils import Response

if TYPE_CHECKING:
    from ..context import ApplicationContext, ActorContext
    from ..command import IValidator, AbstractCommandParser


class ActorWebSocketHandler(abc.ABC, WebSocketHandler):
    def initialize(self, app_ctx: ApplicationContext):
        self._id = uuid4()
        self._parser = app_ctx.parser

        self._ctx = self.get_context()
        self._ctx.command_validator = self._get_command_parser()
        self._ctx.logger = app_ctx.logger
        self._ctx.config = app_ctx.config
        self._ctx.client_id = self.id
        self._ctx.io_loop = IOLoop.current()
        self._ctx.cmd_queue = CommandQueue()
        self._ctx.on_actor_data = self._on_actor_data
        self._ctx.on_actor_error = self._on_actor_error

    @property
    def id(self) -> str:
        return str(self._id)

    @property
    def ctx(self):
        return self._ctx

    @property
    def parser(self) -> AbstractCommandParser:
        return self._parser

    @abc.abstractmethod
    def get_context(self) -> ActorContext:
        raise NotImplementedError()

    @abc.abstractmethod
    def _get_command_parser(self) -> IValidator:
        raise NotImplementedError()

    @abc.abstractmethod
    def _get_command_instance(self, cmd_name: str):
        raise NotImplementedError()

    def on_message(self, message: Union[str, bytes]):
        try:
            parsed_cmd = self.parser.parse(message)
            cmd_instance = self._get_command_instance(parsed_cmd.cmd_name)
            self.ctx.cmd_queue.put(QueuedCommand(cmd_instance, parsed_cmd.params))
        except Exception as e:
            self._safe_write_message(json.dumps(Response.error(self.ctx, e)))

    def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
        return super().data_received(chunk)

    def _on_actor_data(self, data):
        self._safe_write_message(json.dumps(data))

    def _on_actor_error(self, e: Exception):
        self._safe_write_message(Response.error(self.ctx, e))

    def _safe_write_message(self, data):
        try:
            self.write_message(data)
        except WebSocketClosedError as e:
            self.ctx.logger.error("[{}] - Socket closed. Cannot write.".format(self.id))
