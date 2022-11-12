from __future__ import annotations

from typing import TYPE_CHECKING

from .command import CommandFactory
from .producer import AsyncProducer
from ..actor import ActorWebSocketHandler
from ..command import ProducerCommandParamJsonValidator
from ..context import ProducerContext

if TYPE_CHECKING:
    from ..context import ApplicationContext


class ProducerWebSocketHandler(ActorWebSocketHandler):
    def initialize(self, app_ctx: ApplicationContext):
        super().initialize(app_ctx)
        self._async_producer = None

    def get_context(self):
        return ProducerContext()

    def _get_command_parser(self):
        return ProducerCommandParamJsonValidator()

    def _get_command_instance(self, cmd_name: str):
        return CommandFactory.get_instance(cmd_name, self.ctx)

    def open(self):
        self.ctx.logger.info("[{}] - Producer connection opened.".format(self.id))
        self._async_producer = AsyncProducer(self.ctx)
        self._async_producer.start()

    def on_close(self):
        self.ctx.logger.info("[{}] - Producer connection closed.".format(self.id))
        self._async_producer.stop()
