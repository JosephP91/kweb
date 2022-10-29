
class NoSuchSchemaException(Exception):
	def __init__(self, cmd_name: str):
		super().__init__("No schema specified for command {}".format(cmd_name))
