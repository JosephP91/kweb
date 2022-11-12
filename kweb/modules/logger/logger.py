import abc
import logging
import sys
from enum import Enum
from logging import Logger

from munch import DefaultMunch

from .exception import UnsupportedLoggerTypeException


class AbstractLoggerFactory(abc.ABC):
	@abc.abstractmethod
	def get_logger(self, config: DefaultMunch) -> Logger:
		raise NotImplementedError()


class ScreenLoggerFactory(AbstractLoggerFactory):
	def get_logger(self, config: DefaultMunch) -> Logger:
		log_formatter = logging.Formatter(config.logger.formatter)

		console_handler = logging.StreamHandler(sys.stdout)
		console_handler.setFormatter(log_formatter)
		console_handler.setLevel(logging.INFO)

		logger = logging.getLogger()
		logger.setLevel(logging.INFO)
		logger.addHandler(console_handler)
		return logger


class Loggers(Enum):
	SCREEN = ScreenLoggerFactory


class LoggerFactory:
	@staticmethod
	def get_logger(config: DefaultMunch) -> Logger:
		try:
			return Loggers[config.logger.type.upper()].value().get_logger(config)
		except KeyError:
			raise UnsupportedLoggerTypeException(config.logger.type)
