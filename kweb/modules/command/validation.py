import abc
import os

from json import load
from typing import Dict

from jsonschema import validate


class IValidator(abc.ABC):
    @abc.abstractmethod
    def validate(self, command, parameters: Dict = None):
        raise NotImplementedError()


class CommandJsonValidator(IValidator):
    def __init__(self):
        cur_abs_path = os.path.abspath(os.path.dirname(__file__))
        file_path = os.path.join(cur_abs_path, "../../schema/command.json")
        with open(file_path) as file_stream:
            self._schema = load(file_stream)

    def validate(self, command, parameters: Dict = None):
        validate(instance=command, schema=self._schema)
