from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
from ..i_message_client import IMessageClient
from src.utils.logger import setup_logger
from src.utils.config import Config


class KafkaClient(IMessageClient):
    def __init__(self, config: Config):
        self.logger = setup_logger(__name__)
        self.config = config
        self.topic = config.broker_topic
        self.group_id = config.broker_kafka_consumer_group
        self.bootstrap_servers = config.broker_kafka_bootstrap_servers

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=True,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        partition = TopicPartition(self.topic, 0)
        self.consumer.assign([partition])

    def get_queue_size(self):
        try:
            partitions = self.consumer.partitions_for_topic(self.topic)
            if partitions is None:
                return 0

            topic_partitions = [TopicPartition(self.topic, p) for p in partitions]
            self.consumer.assign(topic_partitions)

            end_offsets = self.consumer.end_offsets(topic_partitions)
            committed_offsets = {tp: self.consumer.committed(tp) or 0 for tp in topic_partitions}

            total_lag = sum(end_offsets[tp] - committed_offsets[tp] for tp in topic_partitions)
            return total_lag
        except Exception as e:
            self.logger.error(f"Error getting queue size: {e}")
            return 0

    def consume_messages(self, max_records):
        try:
            messages = []
            records = self.consumer.poll(timeout_ms=1000, max_records=max_records)
            if records:
                for records_list in records.values():
                    for record in records_list:
                        messages.append(record.value)
            return messages
        except KafkaError as e:
            self.logger.error(f"Error consuming messages: {e}")
            raise RuntimeError(f"Critical Kafka error: {e}")

    def send_message(self, message):
        try:
            self.producer.send(self.topic, value=message)
        except KafkaError as e:
            self.logger.error(f"Error sending message: {e}")
            raise RuntimeError(f"Critical Kafka error: {e}")