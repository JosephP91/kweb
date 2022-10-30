from kafka import TopicPartition


class KafkaUtils:
    @staticmethod
    def consumer_record_to_list(data) -> list:
        output = dict()
        for tp, messages in data.items():
            output[(tp.topic, tp.partition)] = [message._asdict() for message in messages]
        return [{'key': key, 'value': value} for key, value in output.items()]

    @staticmethod
    def to_topic_partitions(topic_partitions: list) -> list:
        topic_parts = list()
        for topic_part in topic_partitions:
            topic_parts.append(TopicPartition(topic_part["topic"], topic_part["partition"]))
        return topic_parts

