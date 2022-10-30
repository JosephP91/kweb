from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..context import ConsumerContext

from .exception import ConsumerNotCreatedException


def consumer_created(function):
    def _consumer_created(self, *args, **kwargs):
        if self.context.consumer is None:
            raise ConsumerNotCreatedException()
        return function(self, *args, **kwargs)
    return _consumer_created

