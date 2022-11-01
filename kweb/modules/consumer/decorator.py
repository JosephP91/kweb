from .exception import ConsumerAlreadyCreatedException, ConsumerNotCreatedException


def consumer_not_created(function):
    def _consumer_not_created(self, *args, **kwargs):
        if self.ctx.consumer is not None:
            raise ConsumerAlreadyCreatedException()
        return function(self, *args, **kwargs)
    return _consumer_not_created


def consumer_created(function):
    def _consumer_created(self, *args, **kwargs):
        if self.ctx.consumer is None:
            raise ConsumerNotCreatedException()
        return function(self, *args, **kwargs)
    return _consumer_created

