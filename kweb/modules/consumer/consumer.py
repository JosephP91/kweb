from __future__ import annotations

from logging import Logger
from typing import TYPE_CHECKING, Callable

from tornado.ioloop import IOLoop

from ..command import CommandQueueEmptyException
from ..utils import KafkaUtils

if TYPE_CHECKING:
    from ..context import ConsumerContext


class AsyncConsumer:
    def __init__(self, ctx: ConsumerContext):
        self._should_stop = False
        self._ctx = ctx

    @property
    def ctx(self) -> ConsumerContext:
        return self._ctx

    @property
    def logger(self) -> Logger:
        return self.ctx.logger

    def start(self):
        IOLoop.current().run_in_executor(None, self._callback)

    def stop(self):
        self._should_stop = True

    def _callback(self):
        self.logger.info("[{}] - Starting consumer".format(self.ctx.client_id))
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

        self.logger.info("[{}] - Stopping consumer".format(self.ctx.client_id))
        if self.ctx.consumer is not None:
            self.logger.info("[{}] - Closing consumer".format(self.ctx.client_id))
            self.ctx.consumer.close(autocommit=False)

    def _spawn_ioloop_callback(self, callback: Callable, data):
        IOLoop.spawn_callback(self.ctx.io_loop, callback, data)

