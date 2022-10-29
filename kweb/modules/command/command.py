from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from jsonschema import validate, ValidationError

if TYPE_CHECKING:
	from ..context import Context

from .exception import CommandExecutionException


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
			validate(instance=parameters, schema=self._get_schema())
			return self._execute_command(parameters)

		except ValidationError as e:
			return self._make_error("Parameters validation failed!", e)

		except CommandExecutionException as e:
			return self._make_error("Command execution failed!", e)

		except Exception as e:
			return self._make_error("Generic error occurred!", e)

	@abc.abstractmethod
	def _execute_command(self, parameter: dict) -> dict:
		raise NotImplementedError()

	@abc.abstractmethod
	def _get_schema(self) -> dict:
		raise NotImplementedError()

	def _make_success(self, message: str, payload: dict = None) -> dict:
		return {
			"command_name": self.cmd_name,
			"client_id": self.context.client_id,
			"message": message,
			"payload": payload if payload is not None else {}
		}

	def _make_error(self, message: str, reason: Exception):
		return {
			"command_name": self.cmd_name,
			"client_id": self.context.client_id,
			"message": message,
			"reason": str(reason)
		}
