import abc
from logging import Logger

from munch import DefaultMunch

from ..context import Context


class ICommand(abc.ABC):
    def __init__(self, context: Context):
        self._context = context

    @abc.abstractmethod
    def execute(self, parameters: dict) -> dict:
        raise NotImplementedError()

    @property
    def context(self) -> Context:
        return self._context


class CreateConsumerCommand(ICommand):
    def execute(self, parameters: dict) -> dict:
        self.context.logger.info("Executing create consumer command")
        return dict()


class ListConsumersCommand(ICommand):
    def execute(self, parameters: dict) -> dict:
        self.context.logger.info("Executing list consumer command")
        return dict()

