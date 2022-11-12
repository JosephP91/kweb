
class UnsupportedLoggerTypeException(Exception):
	def __init__(self, logger_type: str):
		super("Unsupported logger type: {}".format(logger_type))
