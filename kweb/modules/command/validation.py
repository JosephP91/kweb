import abc
import os
from json import load
from typing import Dict

from jsonschema import validate

from .exception import NoSuchSchemaException


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


class CommandParamsJsonValidator(IValidator):
    def __init__(self):
        self._schema = self._load_schema()

    @abc.abstractmethod
    def _get_schema_name(self) -> str:
        raise NotImplementedError()

    def _load_schema(self):
        abs_path = os.path.abspath(os.path.dirname(__file__))
        file_path = os.path.join(abs_path, "../../schema/{}".format(self._get_schema_name()))
        with open(file_path) as file_stream:
            return load(file_stream)

    def validate(self, command, parameters: Dict = None):
        try:
            validate(instance=parameters, schema=self._schema[command])
        except KeyError:
            raise NoSuchSchemaException(command)


class ConsumerCommandParamJsonValidator(CommandParamsJsonValidator):
    def _get_schema_name(self) -> str:
        return "consumer.json"


class ProducerCommandParamJsonValidator(CommandParamsJsonValidator):
    def _get_schema_name(self) -> str:
        return "producer.json"
