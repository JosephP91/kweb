
class NoSuchSchemaException(Exception):
    def __init__(self, cmd_name: str):
        super().__init__("No schema specified for command {}".format(cmd_name))


class ConsumerAlreadyCreatedException(Exception):
    def __init__(self):
        super().__init__("Consumer already created!")


class ConsumerNotCreatedException(Exception):
    def __init__(self):
        super().__init__("No consumer created! Please create one!")


class UnsupportedCommandException(Exception):
    def __init__(self, command):
        super().__init__("Unsupported command {}".format(command))
