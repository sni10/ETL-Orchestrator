import pika
import json
from ..i_message_client import IMessageClient
from src.utils.logger import setup_logger
from src.utils.config import Config


class RabbitMQClient(IMessageClient):
    def __init__(self, config: Config):
        self.logger = setup_logger(__name__)
        self.config = config
        self.topic = config.broker_topic

        connection_params = pika.ConnectionParameters(
            host=config.broker_rabbitmq_host,
            port=config.broker_rabbitmq_port,
            credentials=pika.PlainCredentials(
                username=config.broker_rabbitmq_username,
                password=config.broker_rabbitmq_password
            )
        )
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.topic, durable=True)

    def get_queue_size(self):
        queue = self.channel.queue_declare(queue=self.topic, passive=True)
        return queue.method.message_count

    def consume_messages(self, max_records):
        messages = []
        for _ in range(max_records):
            method_frame, header_frame, body = self.channel.basic_get(self.topic, auto_ack=True)
            if method_frame:
                messages.append(json.loads(body))
            else:
                break
        return messages

    def send_message(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.topic,
            body=json.dumps(message)
        )

    def close(self):
        pass