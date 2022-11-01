import os

from json import load

from jsonschema import validate

from ..command import IValidator
from .exception import NoSuchSchemaException


class CommandParamsJsonValidator(IValidator):
    def __init__(self):
        abs_path = os.path.abspath(os.path.dirname(__file__))
        file_path = os.path.join(abs_path, "../../schema/consumer.json")
        with open(file_path) as file_stream:
            self._schema = load(file_stream)

    def validate(self, command, parameters: dict = None):
        try:
            validate(instance=parameters, schema=self._schema[command])
        except KeyError:
            raise NoSuchSchemaException(command)

