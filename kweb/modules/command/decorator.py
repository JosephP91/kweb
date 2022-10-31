from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..context import ConsumerContext

from .exception import *


def consumer_not_created(function):
    def _consumer_not_created(self, *args, **kwargs):
        if self.context.consumer is not None:
            raise ConsumerAlreadyCreatedException()
        return function(self, *args, **kwargs)
    return _consumer_not_created


def consumer_created(function):
    def _consumer_created(self, *args, **kwargs):
        if self.context.consumer is None:
            raise ConsumerNotCreatedException()
        return function(self, *args, **kwargs)
    return _consumer_created

