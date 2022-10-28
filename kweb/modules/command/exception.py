
class ParserException(Exception):
    pass


class UnsupportedCommandException(Exception):
    def __init__(self, command):
        super().__init__("Unsupported command {}".format(command))


class CommandExecutionException(Exception):
    pass


class CommandQueueFullException(Exception):
    pass


class CommandQueueEmptyException(Exception):
    pass
