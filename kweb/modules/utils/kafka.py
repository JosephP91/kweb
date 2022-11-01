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
            topic_parts.append(KafkaUtils.to_topic_partition(topic_part))
        return topic_parts

    @staticmethod
    def to_topic_partition_offset_metadata(topic_partition_offset_metadata: list) -> dict:
        if topic_partition_offset_metadata is None or len(topic_partition_offset_metadata) == 0:
            return None

        tp_om = dict()
        for topic_part_offset_meta in topic_partition_offset_metadata:
            key = topic_part_offset_meta["key"]
            value = topic_part_offset_meta["value"]
            
            topic_partition = KafkaUtils.to_topic_partition(key)
            offset_metadata = OffsetAndMetadata(value["offset"], value["metadata"])
            tp_om[topic_partition] = offset_metadata
        return tp_om

    @staticmethod
    def from_topic_partition_offset_metadata(topic_partition_offset_metadata: dict) -> list:
        tp_om = list()
        for key, value in topic_partition_offset_metadata.items():
            tp_om.append({"key": key._asdict(), "value": value._asdict()})
        return tp_om

    @staticmethod
    def to_topic_partition(topic_partition: dict) -> TopicPartition:
        return TopicPartition(topic_partition["topic"], topic_partition["partition"])

    @staticmethod
    def from_topic_partition_offset(topic_partition_offset: dict) -> list:
        tp_offset = list()
        for key, value in topic_partition_offset.items():
            tp_offset.append({**key._asdict(), "offset": value})
        return tp_offset

