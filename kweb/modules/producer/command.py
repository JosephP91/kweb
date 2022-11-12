from enum import Enum
from typing import Dict

from kafka import KafkaProducer

from ..actor import UnsupportedCommandException
from ..command import AbstractCommand
from ..context import ProducerContext


class CreateProducerCommand(AbstractCommand):
    def __init__(self, ctx: ProducerContext):
        super().__init__("create_producer", ctx)

    def _execute(self, parameters: Dict) -> Dict:
        self.ctx.producer = KafkaProducer(**parameters)
        return {"message": "Producer successfully created!"}


class CommandName(Enum):
    CREATE_PRODUCER = CreateProducerCommand


class CommandFactory:
    @staticmethod
    def get_instance(cmd_name: str, ctx: ProducerContext) -> AbstractCommand:
        try:
            return CommandName[cmd_name.upper()].value(ctx)
        except KeyError:
            raise UnsupportedCommandException(cmd_name)
