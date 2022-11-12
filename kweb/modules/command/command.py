from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from ..context import ActorContext

from ..utils import Response


class AbstractCommand(abc.ABC):
    def __init__(self, cmd_name: str, ctx: ActorContext):
        self._cmd_name = cmd_name
        self._ctx = ctx

    @property
    def cmd_name(self):
        return self._cmd_name

    @property
    def ctx(self):
        return self._ctx

    def execute(self, parameters: dict) -> Dict:
        try:
            if self.should_validate():
                self.ctx.command_validator.validate(self.cmd_name, parameters)

            output = self._execute(parameters)
            return Response.success(self.cmd_name, self.ctx, **output)

        except Exception as e:
            return Response.cmd_error(self.cmd_name, self.ctx, e)

    def should_validate(self) -> bool:
        return True

    @abc.abstractmethod
    def _execute(self, parameter: Dict) -> Dict:
        raise NotImplementedError()

    def __str__(self):
        return self.cmd_name
