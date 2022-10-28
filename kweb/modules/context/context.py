from logging import Logger
from munch import DefaultMunch


class Context:
    def __init__(self):
        self._logger = None
        self._config = None

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def config(self) -> DefaultMunch:
        return self._config

    @logger.setter
    def logger(self, logger: Logger):
        self._logger = logger

    @config.setter
    def config(self, config: DefaultMunch):
        self._config = config


class ConsumerContext(Context):
    def __init__(self):
        super().__init__()
        self._client_id = None
        self._cmd_queue = None

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def cmd_queue(self):
        return self._cmd_queue

    @client_id.setter
    def client_id(self, client_id):
        self._client_id = client_id

    @cmd_queue.setter
    def cmd_queue(self, cmd_queue):
        self._cmd_queue = cmd_queue

