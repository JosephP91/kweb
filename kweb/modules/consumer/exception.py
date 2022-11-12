
class ConsumerAlreadyCreatedException(Exception):
    def __init__(self):
        super().__init__("Consumer already created!")


class ConsumerNotCreatedException(Exception):
    def __init__(self):
        super().__init__("No consumer created! Please create one!")
