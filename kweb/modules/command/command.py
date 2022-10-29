from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from ..context import ConsumerContext


class ICommand(abc.ABC):
	def __init__(self, context: ConsumerContext):
		self._context = context

	@abc.abstractmethod
	def execute(self, parameters: dict) -> dict:
		raise NotImplementedError()

	@property
	def context(self) -> ConsumerContext:
		return self._context


class CreateConsumerCommand(ICommand):
	def execute(self, parameters: dict) -> dict:
		self.context.logger.info("Executing create consumer command")
		return dict()

