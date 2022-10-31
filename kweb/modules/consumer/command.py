from __future__ import annotations

import abc
import base64
import json
import os
from enum import Enum
from typing import TYPE_CHECKING
from tornado.ioloop import IOLoop

from kafka import KafkaConsumer, TopicPartition

if TYPE_CHECKING:
    from ..context import ConsumerContext

from ..command import AbstractCommand
from ..command.decorator import consumer_created, consumer_not_created
from ..command.exception import *
from .exception import NoSuchSchemaException
from ..utils import KafkaUtils, Response


class ConsumerCommand(AbstractCommand, abc.ABC):
    def __init__(self, cmd_name: str, context: ConsumerContext):
        super().__init__(cmd_name)
        self._context = context

    @property
    def context(self) -> ConsumerContext:
        return self._context

    def _get_schema(self) -> dict:
        cur_abs_path = os.path.abspath(os.path.dirname(__file__))
        full_file_path = os.path.join(cur_abs_path, "../../schema/consumer.json")
        with open(full_file_path) as json_file_stream:
            try:
                return json.load(json_file_stream)[self.cmd_name]
            except KeyError:
                raise NoSuchSchemaException(self.cmd_name)


class CreateConsumerCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("create_consumer", context)

    @consumer_not_created
    def _execute_command(self, parameters: dict) -> dict:
        self.context.consumer = KafkaConsumer(
            **parameters,
            api_version=(2, 3, 0),
            value_deserializer=lambda v: base64.b64encode(v).decode("ascii")
        )
        return {"message": "Consumer successfully created!"}


class SubscribeCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("subscribe", context)

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        consumer_topics = parameters["topics"]
        self.context.consumer.subscribe(topics=consumer_topics)
        return {"message": "Subscription started!"}


class UnsubscribeCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("unsubscribe", context)

    def validation_enabled(self) -> bool:
        return False 

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        self.context.consumer.unsubscribe()
        return {"message": "Unsubscribed from topic/partitions"}


class TopicsCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("topics", context)

    def validation_enabled(self) -> bool:
        return False

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        topics = self.context.consumer.topics()
        return {"message": "Topics retrieved", "topics": list(topics)}


class SubscriptionsCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("subscriptions", context)

    def validation_enabled(self) -> bool:
        return False

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        subs = self.context.consumer.subscription()
        return {"message": "Subscriptions retrieved", "subscriptions": list(subs)}


class SeekToEndCommand(ConsumerCommand):
    def __init__(self, context: ConsumerCommand):
        super().__init__("seek_to_end", context)

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.context.consumer.seek_to_end(*topic_parts)
        return {"message": "Seeked to end successfully!"}


class SeekToBeginningCommand(ConsumerCommand):
    def __init__(self, context: ConsumerCommand):
        super().__init__("seek_to_beginning", context)

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"]) 
        self.context.consumer.seek_to_end(*topic_parts)
        return {"message": "Seeked to beginning successfully!"}


class AssignCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("assign", context)

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.context.consumer.assign(topic_parts)
        return {"message": "Assigned successfully!"}


class AssignmentCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("assignment", context)

    def validation_enabled(self):
        return False

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        assignment = self.context.consumer.assignment()
        return {"message": "Assignment retrieved!", "assignment": list(assignment)}


class BootstrapConnectedCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("bootstrap_connected", context)

    def validation_enabled(self):
        return False
    
    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        status = self.context.consumer.bootstrap_connected()
        return {"message": "OK", "status": status}


class CommitAsyncCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("commit_async", context)

    def validation_enabled(self):
        return True

    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
       tp_om = KafkaUtils.to_topic_partition_offset_metadata(parameters["topic_offset_metadata"])
       self.context.consumer.commit_async(tp_om, lambda offset, response: self._callback(offset, response))
       return {"message": "Committing ..."}

    def _callback(self, offset, response):
        if isinstance(response, Exception):
            response_data = Response.cmd_error(self.cmd_name, self.context.client_id, response) 
        else:
            data = KafkaUtils.from_topic_partition_offset_metadata(offset)
            data = {"message": "Committed sucessfully!", "data": data, "response": response}
            response_data = Response.success(self.cmd_name, self.context, **data)
        
        IOLoop.spawn_callback(self.context.io_loop, self.context.on_consumer_data, response_data) 


class CommitCommand(ConsumerCommand):
    def __init__(self, context: ConsumerContext):
        super().__init__("commit", context)

    def validation_enabled(self):
        return True
    
    @consumer_created
    def _execute_command(self, parameters: dict) -> dict:
        tp_om = KafkaUtils.to_topic_partition_offset_metadata(parameters["topic_offset_metadata"])
        self.context.consumer.commit(tp_om)
        return {"message": "Committed!"}


class CommandName(Enum):
    CREATE_CONSUMER = CreateConsumerCommand
    SUBSCRIBE = SubscribeCommand
    UNSUBSCRIBE = UnsubscribeCommand
    TOPICS = TopicsCommand
    SUBSCRIPTIONS = SubscriptionsCommand
    SEEK_TO_END = SeekToEndCommand
    SEEK_TO_BEGINNING = SeekToBeginningCommand
    ASSIGN = AssignCommand
    ASSIGNMENT = AssignmentCommand
    BOOTSTRAP_CONNECTED = BootstrapConnectedCommand
    COMMIT_ASYNC = CommitAsyncCommand
    COMMIT = CommitCommand


class CommandFactory:
    @staticmethod
    def get_instance(cmd_name: str, context: ConsumerContext) -> AbstractCommand:
        try:
            return CommandName[cmd_name.upper()].value(context)
        except KeyError:
            raise UnsupportedCommandException(cmd_name)

