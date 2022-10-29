from __future__ import annotations

import abc
import json
import os
from enum import Enum
from typing import TYPE_CHECKING

from kafka import KafkaConsumer

if TYPE_CHECKING:
	from ..context import ConsumerContext

from ..command import AbstractCommand, UnsupportedCommandException, CommandExecutionException
from .exception import NoSuchSchemaException


class ConsumerCommand(AbstractCommand, abc.ABC):
	def __init__(self, cmd_name: str, context: ConsumerContext):
		super().__init__(cmd_name)
		self._context = context

	@property
	def context(self) -> ConsumerContext:
		return self._context

	def _get_schema(self) -> dict:
		cur_abs_path = os.path.abspath(os.path.dirname(__file__))
		full_file_path = os.path.join(cur_abs_path, "../../schema/consumer.json")
		with open(full_file_path) as json_file_stream:
			try:
				return json.load(json_file_stream)[self.cmd_name]
			except KeyError:
				raise NoSuchSchemaException(self.cmd_name)


class CreateConsumerCommand(ConsumerCommand):
	def __init__(self, context: ConsumerContext):
		super().__init__("create_consumer", context)

	def _execute_command(self, parameters: dict) -> dict:
		if self.context.consumer is not None:
			raise CommandExecutionException("Consumer has already been created!")

		self.context.consumer = KafkaConsumer(**parameters, api_version=(2, 3, 0))
		self.context.logger.info("Consumer successfully created: {}".format(parameters))
		return self._make_success("Consumer successfully created!")


class CommandName(Enum):
	CREATE_CONSUMER = CreateConsumerCommand


class CommandFactory:
	@staticmethod
	def get_instance(cmd_name: str, context: ConsumerContext) -> AbstractCommand:
		try:
			return CommandName[cmd_name.upper()].value(context)
		except KeyError:
			raise UnsupportedCommandException(cmd_name)
