from __future__ import annotations

from typing import TYPE_CHECKING

from .command import CommandFactory
from .consumer import AsyncConsumer
from ..actor import ActorWebSocketHandler
from ..command import ConsumerCommandParamJsonValidator
from ..context import ConsumerContext

if TYPE_CHECKING:
    from ..context import ApplicationContext


class ConsumerWebSocketHandler(ActorWebSocketHandler):
    def initialize(self, app_ctx: ApplicationContext):
        super().initialize(app_ctx)
        self._async_consumer = None

    def get_context(self):
        return ConsumerContext()

    def _get_command_parser(self):
        return ConsumerCommandParamJsonValidator()

    def _get_command_instance(self, cmd_name: str):
        return CommandFactory.get_instance(cmd_name, self.ctx)

    def open(self):
        self.ctx.logger.info("[{}] - Consumer connection opened.".format(self.id))
        self._async_consumer = AsyncConsumer(self.ctx)
        self._async_consumer.start()

    def on_close(self):
        self.ctx.logger.info("[{}] - Consumer connection closed.".format(self.id))
        self._async_consumer.stop()
