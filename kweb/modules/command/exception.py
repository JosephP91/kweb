
class ParserException(Exception):
    pass


class UnsupportedParserException(Exception):
    def __init__(self, parser_name: str):
        super().__init__("Unsupported parser type {}".format(parser_name))


class CommandExecutionException(Exception):
    pass


class CommandQueueFullException(Exception):
    def __init__(self):
        super().__init__("Cannot accept more commands right now!")


class CommandQueueEmptyException(Exception):
    def __init__(self):
        super().__init__("No more command to process!")
