from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from ..context import Context


class ICommand(abc.ABC):
	@abc.abstractmethod
	def execute(self, parameters: dict) -> dict:
		raise NotImplementedError()

	@property
	@abc.abstractmethod
	def context(self) -> Context:
		raise NotImplementedError()
