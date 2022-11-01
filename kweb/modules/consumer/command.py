from __future__ import annotations

import base64
from enum import Enum
from typing import TYPE_CHECKING

from kafka import KafkaConsumer, OffsetAndMetadata, ConsumerRebalanceListener
from tornado.ioloop import IOLoop

if TYPE_CHECKING:
    from ..context import ConsumerContext

from ..command import AbstractCommand
from .exception import UnsupportedCommandException
from .decorator import *
from ..utils import KafkaUtils, Response


class CreateConsumerCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("create_consumer", ctx)

    @consumer_not_created
    def _execute(self, parameters: dict) -> dict:
        self.ctx.consumer = KafkaConsumer(
            **parameters,
            api_version=(2, 3, 0),
            value_deserializer=lambda v: base64.b64encode(v).decode("ascii")
        )
        return {"message": "Consumer successfully created!"}


class SubscribeCommand(AbstractCommand, ConsumerRebalanceListener):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("subscribe", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        consumer_topics = parameters["topics"]
        self.ctx.consumer.subscribe(topics=consumer_topics, listener=self)
        return {"message": "Subscription started!"}

    def on_partitions_assigned(self, assigned):
        topic_partitions = KafkaUtils.from_topic_partitions(assigned)
        data = {"message": "Topic/Partitions assigned!", "assigned": topic_partitions}
        response_data = Response.success(self.cmd_name, self.ctx, **data)
        IOLoop.spawn_callback(self.ctx.io_loop, self.ctx.on_actor_data, response_data)

    def on_partitions_revoked(self, revoked):
        topic_partitions = KafkaUtils.from_topic_partitions(revoked)
        data = {"message": "Topic/Partition revoked!", "revoked": topic_partitions}
        response_data = Response.success(self.cmd_name, self.ctx, **data)
        IOLoop.spawn_callback(self.ctx.io_loop, self.ctx.on_actor_data, response_data)


class UnsubscribeCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("unsubscribe", ctx)

    def should_validate(self) -> bool:
        return False

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        self.ctx.consumer.unsubscribe()
        return {"message": "Unsubscribed from topic/partitions"}


class TopicsCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("topics", ctx)

    def should_validate(self) -> bool:
        return False

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topics = self.ctx.consumer.topics()
        return {"message": "Topics retrieved", "topics": list(topics)}


class SubscriptionsCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("subscriptions", ctx)

    def should_validate(self) -> bool:
        return False

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        subs = self.ctx.consumer.subscription()
        return {"message": "Subscriptions retrieved", "subscriptions": list(subs)}


class SeekToEndCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("seek_to_end", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.ctx.consumer.seek_to_end(*topic_parts)
        return {"message": "Seeked to end successfully!"}


class SeekToBeginningCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("seek_to_beginning", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.ctx.consumer.seek_to_end(*topic_parts)
        return {"message": "Seeked to beginning successfully!"}


class AssignCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("assign", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.ctx.consumer.assign(topic_parts)
        return {"message": "Assigned successfully!"}


class AssignmentCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("assignment", ctx)

    def should_validate(self):
        return False

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        assignment = self.ctx.consumer.assignment()
        return {"message": "Assignment retrieved!", "assignment": list(assignment)}


class BootstrapConnectedCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("bootstrap_connected", ctx)

    def should_validate(self):
        return False

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        status = self.ctx.consumer.bootstrap_connected()
        return {"message": "OK", "status": status}


class CommitAsyncCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("commit_async", ctx)

    def should_validate(self):
        return True

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        tp_om = KafkaUtils.to_topic_partition_offset_metadata(parameters["topic-offset-metadata"])
        self.ctx.consumer.commit_async(tp_om, lambda offset, response: self._callback(offset, response))
        return {"message": "Committing ..."}

    def _callback(self, offset, response):
        if isinstance(response, Exception):
            response_data = Response.cmd_error(self.cmd_name, self.ctx, response)
        else:
            data = KafkaUtils.from_topic_partition_offset_metadata(offset)
            data = {"message": "Committed sucessfully!", "data": data, "response": response}
            response_data = Response.success(self.cmd_name, self.ctx, **data)

        IOLoop.spawn_callback(self.ctx.io_loop, self.ctx.on_actor_data, response_data)


class CommitCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("commit", ctx)

    def should_validate(self):
        return True

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        tp_om = KafkaUtils.to_topic_partition_offset_metadata(parameters["topic-offset-metadata"])
        self.ctx.consumer.commit(tp_om)
        return {"message": "Committed!"}


class CommittedCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("committed", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        metadata = parameters["metadata"] if "metadata" in parameters else False
        topic_partition = KafkaUtils.to_topic_partition(parameters["topic-partition"])

        last_committed_offset = self.ctx.consumer.committed(topic_partition, metadata)
        if isinstance(last_committed_offset, OffsetAndMetadata):
            last_committed_offset = last_committed_offset._asdict()
        return {"message": "Fetched committed offsets", "last_committed_offset": last_committed_offset}


class PartitionsForTopic(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("partitions_for_topic", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        partitions = self.ctx.consumer.partitions_for_topic(parameters["topic"])
        return {"message": "Retrieved partitions", "partitions": list(partitions)}


class PositionCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("position", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_partition = KafkaUtils.to_topic_partition(parameters["topic-partition"])
        offset = self.ctx.consumer.position(topic_partition)
        return {"message": "Retrieved position", "offset": offset}


class HighwaterCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("highwater", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_partition = KafkaUtils.to_topic_partition(parameters["topic-partition"])
        offset = self.ctx.consumer.highwater(topic_partition)
        return {"message": "Retrieved highwater offset", "offset": offset}


class PauseCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("pause", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.ctx.consumer.pause(*topic_parts)
        return {"message": "Paused successfully!"}


class PausedCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("paused", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        paused_tp = self.ctx.consumer.paused()
        return {"message": "Retrieved paused topic partitions", "topic_partitions": paused_tp}


class ResumeCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("resume", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_parts = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        self.ctx.consumer.resume(*topic_parts)
        return {"message": "Resumed successfully!"}


class SeekCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("seek", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_partition = KafkaUtils.to_topic_partition(parameters["topic-partition"])
        self.ctx.consumer.seek(topic_partition, parameters["offset"])
        return {"message": "Seeked successfully!"}


class BeginningOffsetsCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("beginning_offsets", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_partitions = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        offset_tp = self.ctx.consumer.beginning_offsets(topic_partitions)
        offset_tp = KafkaUtils.from_topic_partition_offset(offset_tp)
        return {"message": "Offset retrieved!", "topic-partition-offset": offset_tp}


class EndOffsetsCommand(AbstractCommand):
    def __init__(self, ctx: ConsumerContext):
        super().__init__("end_offsets", ctx)

    @consumer_created
    def _execute(self, parameters: dict) -> dict:
        topic_partitions = KafkaUtils.to_topic_partitions(parameters["topic-partitions"])
        offset_tp = self.ctx.consumer.end_offsets(topic_partitions)
        offset_tp = KafkaUtils.from_topic_partition_offset(offset_tp)
        return {"message": "Offset retrieved!", "topic-partition-offset": offset_tp}


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
    COMMITTED = CommittedCommand
    PARTITIONS_FOR_TOPIC = PartitionsForTopic
    POSITION = PositionCommand
    HIGHWATER = HighwaterCommand
    PAUSE = PauseCommand
    PAUSED = PausedCommand
    RESUME = ResumeCommand
    SEEK = SeekCommand
    BEGINNING_OFFSETS = BeginningOffsetsCommand
    END_OFFSETS = EndOffsetsCommand


class CommandFactory:
    @staticmethod
    def get_instance(cmd_name: str, context: ConsumerContext) -> AbstractCommand:
        try:
            return CommandName[cmd_name.upper()].value(context)
        except KeyError:
            raise UnsupportedCommandException(cmd_name)

