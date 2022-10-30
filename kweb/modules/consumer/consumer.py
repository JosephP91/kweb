from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from tornado.ioloop import IOLoop

from ..command import CommandQueueEmptyException

if TYPE_CHECKING:
    from ..context import ConsumerContext



def content_to_json(data) -> list:
    output = dict()
    for tp, messages in data.items():
        output[(tp.topic, tp.partition)] = [message._asdict() for message in messages]
    return [{'key': key, 'value': value} for key, value in output.items()]


class AsyncConsumer:
    def __init__(self, ctx: ConsumerContext):
        self._should_stop = False
        self._ctx = ctx

    @property
    def ctx(self):
        self._ctx = ctx

    def start(self):
        IOLoop.current().run_in_executor(None, self._callback)

    def stop(self):
        self._should_stop = True

    def _callback(self):
        self.ctx.logger.info("Starting consumer {}".format(self.ctx.client_id))
        while not self._should_stop:
            try:
                queued_cmd = self.ctx.cmd_queue.pop(block=True, timeout=1)
                cmd_output = queued_cmd.command.execute(queued_cmd.parameters)
                self.ctx.out_queue.put(cmd_output)

            except CommandQueueEmptyException:
                if self.ctx.consumer is None:
                    continue
                try:
                    cmd_output = self.ctx.consumer.poll(timeout_ms=100)
                    if len(cmd_output) > 0:
                        cmd_output = content_to_json(cmd_output)
                        self._spawn_ioloop_callback(self.ctx.on_consumer_data, cmd_output)
                except Exception as e:
                    self._spawn_ioloop_callback(self.ctx.on_consumer_error, e)

        self.ctx.logger.info("Stopping consumer {}".format(self.ctx.client_id))
        if self.ctx.consumer is not None:
            self.ctx.logger.info("Closing consumer {}".format(self.ctx.client_id))
            self.ctx.consumer.close(autocommit=False)

    def _spawn_ioloop_callback(self, callback: Callable, data):
        IOLoop.spawn_callback(self.ctx.io_loop, callback, data)

