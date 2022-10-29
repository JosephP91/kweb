from __future__ import annotations

import abc
from json import loads, JSONDecodeError
from typing import TYPE_CHECKING

from jsonschema import validate, ValidationError

from .exception import ParserException
from .type import ParserType

if TYPE_CHECKING:
	from munch import DefaultMunch


class ParsedCommand:
	def __init__(self, command_name: str, parameters: dict):
		self._command_name = command_name
		self._parameters = parameters

	@property
	def command_name(self):
		return self._command_name

	@property
	def parameters(self):
		return self._parameters


class ICommandParser(abc.ABC):
	def cleanup(self, command_str: str) -> str:
		return command_str.replace("\n", "").strip()

	@abc.abstractmethod
	def parse(self, command_str: str) -> ParsedCommand:
		raise NotImplementedError()


class JsonCommandParser(ICommandParser):
	def parse(self, command_str: str) -> ParsedCommand:
		schema = {
			"command_name": "string",
			"parameters": "object",
			"required": ["command_name", "parameters"]
		}

		try:
			json_command = loads(command_str)
			validate(instance=json_command, schema=schema)
			return ParsedCommand(json_command["command_name"], json_command["parameters"])

		except JSONDecodeError:
			raise ParserException("'{}' cannot be parsed as JSON command!".format(command_str))

		except ValidationError:
			raise ParserException("JSON command must be compliant with this schema: '{}'".format(schema))


class CommandParserFactory:
	@staticmethod
	def get_instance(config: DefaultMunch) -> ICommandParser:
		selected_parser = config.command.parser.type
		try:
			parser_name = ParserType[selected_parser]
		except KeyError:
			raise ValueError("Unsupported parser type {}".format(selected_parser))

		if parser_name == ParserType.JSON:
			return JsonCommandParser()
