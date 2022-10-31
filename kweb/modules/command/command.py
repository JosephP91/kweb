from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from jsonschema import validate

if TYPE_CHECKING:
    from ..context import Context

from ..utils import Response


class AbstractCommand(abc.ABC):
    def __init__(self, cmd_name: str):
        self._cmd_name = cmd_name

    @property
    def cmd_name(self):
        return self._cmd_name

    @property
    @abc.abstractmethod
    def context(self) -> Context:
        raise NotImplementedError()

    def execute(self, parameters: dict) -> dict:
        try:
            if self.validation_enabled():
                validate(instance=parameters, schema=self._get_schema())
            output = self._execute_command(parameters)
            return Response.success(self.cmd_name, self.context, **output)

        except Exception as e:
            return Response.cmd_error(self.cmd_name, self.context, e)

    def validation_enabled(self) -> bool:
        return True

    @abc.abstractmethod
    def _execute_command(self, parameter: dict) -> dict:
        raise NotImplementedError()

    @abc.abstractmethod
    def _get_schema(self) -> dict:
        raise NotImplementedError()

