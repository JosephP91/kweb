from enum import Enum

from .command import *


class ParserType(Enum):
    JSON = "JSON"


class CommandName(Enum):
    CREATE_CONSUMER = CreateConsumerCommand
    LIST_CONSUMERS = ListConsumersCommand


