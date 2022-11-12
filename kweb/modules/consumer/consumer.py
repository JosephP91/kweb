from __future__ import annotations

from typing import TYPE_CHECKING

from ..actor import AsyncActor
from ..command import CommandQueueEmptyException
from ..utils import KafkaUtils

if TYPE_CHECKING:
    from ..context import ConsumerContext


class AsyncConsumer(AsyncActor):
    def __init__(self, ctx: ConsumerContext):
        super().__init__(ctx)

    def _callback(self):
        self.logger.info("[{}] - Starting consumer.".format(self.ctx.client_id))
        while not self._should_stop:
            try:
                queued_cmd = self.ctx.cmd_queue.pop(block=True, timeout=1)
                cmd_output = queued_cmd.command.execute(queued_cmd.parameters)
                self._spawn_ioloop_callback(self.ctx.on_actor_data, cmd_output)

            except CommandQueueEmptyException:
                if self.ctx.consumer is None:
                    continue
                try:
                    records = self.ctx.consumer.poll(timeout_ms=100)
                    if len(records) > 0:
                        records = KafkaUtils.consumer_records_to_list(records)
                        self._spawn_ioloop_callback(self.ctx.on_actor_data, records)
                except Exception as e:
                    self._spawn_ioloop_callback(self.ctx.on_actor_error, e)

        self.logger.info("[{}] - Stopping consumer.".format(self.ctx.client_id))
        if self.ctx.consumer is not None:
            self.logger.info("[{}] - Closing consumer.".format(self.ctx.client_id))
            self.ctx.consumer.close(autocommit=False)
