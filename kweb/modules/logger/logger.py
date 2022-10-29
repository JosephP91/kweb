import abc
import logging
import sys
from logging import Logger

from munch import DefaultMunch

from .type import LoggerType


class AbstractLoggerFactory(abc.ABC):
	@abc.abstractmethod
	def get_logger(self, config: DefaultMunch) -> Logger:
		raise NotImplementedError("This method has not been implemented")


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


class LoggerFactory:
	@staticmethod
	def get_logger(config: DefaultMunch, logger_type: LoggerType) -> Logger:
		if logger_type == LoggerType.SCREEN:
			logger_factory = ScreenLoggerFactory()
		else:
			raise RuntimeError("Logger type {} is not supported!".format(logger_type))

		return logger_factory.get_logger(config)
