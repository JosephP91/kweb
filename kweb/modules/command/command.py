import abc
from logging import Logger

from munch import DefaultMunch


class ICommand(abc.ABC):
    def __init__(self, config: DefaultMunch, logger: Logger):
        self._config = config
        self._logger = logger

    @abc.abstractmethod
    def execute(self, parameters: dict) -> dict:
        raise NotImplementedError()

    @property
    def config(self):
        return self._config

    @property
    def logger(self):
        return self._logger


class CreateConsumerCommand(ICommand):
    def execute(self, parameters: dict) -> dict:
        self.logger.info("Executing create consumer command")
        return dict()


class ListConsumersCommand(ICommand):
    def execute(self, parameters: dict) -> dict:
        self.logger.info("Executing list consumer command")
        return dict()
