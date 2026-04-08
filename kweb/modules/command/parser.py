from __future__ import annotations

import abc
from collections import namedtuple
from enum import Enum
from json import loads, JSONDecodeError
from typing import TYPE_CHECKING, Dict

from .exception import ParserException, UnsupportedParserException
from .validation import IValidator, CommandJsonValidator

if TYPE_CHECKING:
    from munch import DefaultMunch


ParsedCommand = namedtuple("ParsedCommand", ["cmd_name", "params"])


class AbstractCommandParser(abc.ABC):
    def __init__(self, validator: IValidator):
        self._validator = validator

    def parse(self, command: str) -> ParsedCommand:
        command = command.replace("\n", "").strip()

        if self._validator is not None:
            self._validator.validate(command)

        parsed = self._parse(command)
        return ParsedCommand(parsed["command_name"], parsed["parameters"])

    @abc.abstractmethod
    def _parse(self, command: str) -> Dict:
        raise NotImplementedError()


class JsonCommandParser(AbstractCommandParser):
    def __init__(self):
        super().__init__(CommandJsonValidator())

    def _parse(self, command: str) -> Dict:
        try:
            return loads(command)
        except JSONDecodeError as e:
            raise ParserException("Malformed json command: {}".format(str(e)))


class ParserType(Enum):
    JSON = JsonCommandParser


class CommandParserFactory:
    @staticmethod
    def get_instance(config: DefaultMunch) -> AbstractCommandParser:
        selected_parser = config.command.parser.type
        try:
            return ParserType[selected_parser.upper()].value()
        except KeyError:
            raise UnsupportedParserException(selected_parser)
