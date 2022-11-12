
class ParserException(Exception):
    pass


class UnsupportedParserException(Exception):
    def __init__(self, parser_name: str):
        super().__init__("Unsupported parser type {}".format(parser_name))


class CommandQueueFullException(Exception):
    def __init__(self):
        super().__init__("Cannot accept more commands right now!")


class CommandQueueEmptyException(Exception):
    def __init__(self):
        super().__init__("No more command to process!")


class NoSuchSchemaException(Exception):
    def __init__(self, cmd_name: str):
        super().__init__("No schema specified for command {}".format(cmd_name))
