from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Callable

from tornado.ioloop import IOLoop

if TYPE_CHECKING:
    from logging import Logger
    from ..context import ActorContext


class AsyncActor:
    def __init__(self, ctx: ActorContext):
        self._should_stop = False
        self._ctx = ctx

    @property
    def ctx(self):
        return self._ctx

    @property
    def logger(self) -> Logger:
        return self.ctx.logger

    @abc.abstractmethod
    def _callback(self):
        raise NotImplementedError()

    def start(self):
        IOLoop.current().run_in_executor(None, self._callback)

    def stop(self):
        self._should_stop = True

    def _spawn_ioloop_callback(self, callback: Callable, data):
        IOLoop.spawn_callback(self.ctx.io_loop, callback, data)
