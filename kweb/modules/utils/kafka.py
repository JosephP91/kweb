from kafka import TopicPartition, OffsetAndMetadata


class KafkaUtils:
    @staticmethod
    def consumer_records_to_list(data) -> list:
        output = dict()
        for tp, messages in data.items():
            output[(tp.topic, tp.partition)] = [message._asdict() for message in messages]
        return [{'key': key, 'value': value} for key, value in output.items()]

    @staticmethod
    def to_topic_partitions(topic_partitions: list) -> list:
        if topic_partitions is None or len(topic_partitions) == 0:
            return None

        topic_parts = list()
        for topic_part in topic_partitions:
            topic_parts.append(TopicPartition(topic_part["topic"], topic_part["partition"]))
        return topic_parts

    @staticmethod
    def to_topic_partition_offset_metadata(topic_partition_offset_metadata: list) -> dict:
        if topic_partition_offset_metadata is None or len(topic_partition_offset_metadata) == 0:
            return None

        tp_om = dict()
        for topic_part_offset_meta in topic_partition_offset_metadata:
            key = topic_part_offset_meta["key"]
            value = topic_part_offset_meta["value"]
            
            topic_partition = TopicPartition(key["topic"], key["partition"])
            offset_metadata = OffsetAndMetadata(value["offset"], value["metadata"])
            tp_om[topic_partition] = offset_metadata
        return tp_om

    @staticmethod
    def from_topic_partition_offset_metadata(topic_partition_offset_metadata: dict) -> list:
        tp_om = list()
        for key, value in topic_partition_offset_metadata.items():
            tp_om.append({
                "key": {"topic": key.topic, "partition": key.partition},
                "value": {"offset": value.offset, "metadata": value.metadata}
            })
        return tp_om

