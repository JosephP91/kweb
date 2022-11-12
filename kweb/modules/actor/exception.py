
class UnsupportedCommandException(Exception):
    def __init__(self, command):
        super().__init__("Unsupported command {}".format(command))
