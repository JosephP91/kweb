
class ParserException(Exception):
    pass


class UnsupportedCommandException(Exception):
    def __init__(self, command):
        super().__init__("Unsupported command {}".format(command))


class CommandExecutionException(Exception):
    pass


class CommandQueueFullException(Exception):
    def __init__(self):
        super().__init__("Cannot accept more commands right now!")


class CommandQueueEmptyException(Exception):
    def __init__(self):
        super().__init__("No more command to process!")


class ConsumerAlreadyCreatedException(Exception):
    def __init__(self):
        super().__init__("Consumer already created!")


class ConsumerNotCreatedException(Exception):
    def __init__(self):
        super().__init__("No consumer created! Please create one!")

