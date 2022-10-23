import sys
import logging
from munch import DefaultMunch
from logging import Logger
from json import dumps
from uuid import uuid4

from tornado.websocket import WebSocketHandler

from .client_set import ClientSet
from ..logger import LoggerFactory, LoggerType


class WebSocketController(WebSocketHandler):
    _clients = ClientSet()

    def initialize(self, config: DefaultMunch, logger: Logger):
        self._config = config
        self._logger = logger

    def open(self):
        self._id = uuid4()

        WebSocketController._clients.add(self)
        self._logger.info("New connection {}".format(self._id))

    def on_close(self):
        WebSocketController._clients.remove(self)
        self._logger.info("Closed connection {}".format(self._id))

    def on_message(self, message):
        self._logger.info("Received message: {}".format(message))
