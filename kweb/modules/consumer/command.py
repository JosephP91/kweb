from __future__ import annotations

import abc
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from ..context import ConsumerContext

from ..command import ICommand, UnsupportedCommandException


class ConsumerCommand(ICommand, abc.ABC):
	def __init__(self, context: ConsumerContext):
		self._context = context

	@property
	def context(self) -> ConsumerContext:
		return self._context


class CreateConsumerCommand(ConsumerCommand):
	def execute(self, parameters: dict) -> dict:
		self.context.logger.info("Creating consumer")
		return {}


class SubscribeCommand(ConsumerCommand):
	def execute(self, parameters: dict) -> dict:
		self.context.logger.info("Subscribing to topics")
		return {}


class CommandName(Enum):
	CREATE_CONSUMER = CreateConsumerCommand
	SUBSCRIBE = SubscribeCommand


class CommandFactory:
	@staticmethod
	def get_instance(cmd_name: str, context: ConsumerContext) -> ICommand:
		try:
			return CommandName[cmd_name.upper()].value(context)
		except KeyError:
			raise UnsupportedCommandException(cmd_name)
