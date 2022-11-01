from __future__ import annotations

import abc
from enum import Enum
from json import loads, JSONDecodeError
from typing import TYPE_CHECKING

from collections import namedtuple
from jsonschema import validate

from .exception import ParserException

if TYPE_CHECKING:
    from munch import DefaultMunch


ParsedCommand = namedtuple("ParsedCommand", ["cmd_name", "params"])


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
            json_cmd = loads(command_str)
            validate(instance=json_cmd, schema=schema)
            return ParsedCommand(json_cmd["command_name"], json_cmd["parameters"])

        except Exception as e:
            raise ParserException(str(e))


class ParserType(Enum):
    JSON = "JSON"


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
