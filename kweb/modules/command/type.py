import abc
from enum import Enum

from .command import *
from .exception import UnsupportedCommandException


class ParserType(Enum):
    JSON = "JSON"


class CommandName(Enum):
    CREATE_CONSUMER = CreateConsumerCommand
    LIST_CONSUMERS = ListConsumersCommand

    @classmethod
    def get_class(cls, command: str):
        try:
            return cls[command.upper()]
        except KeyError:
            raise UnsupportedCommandException(command)

