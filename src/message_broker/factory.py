from .kafka.kafka_client import KafkaClient
from .rabbitmq.rabbitmq_client import RabbitMQClient
from .redis.redis_client import RedisClient
from src.utils.config import Config


class MessageClientFactory:
    @staticmethod
    def create_client(config: Config):
        """Создает клиента брокера на основе Config"""
        broker_type = config.broker_type

        if broker_type == 'kafka':
            return KafkaClient(config)

        elif broker_type in ['rabbit', 'rabbitmq']:
            return RabbitMQClient(config)

        elif broker_type == 'redis':
            return RedisClient(config)

        else:
            raise ValueError(f"Unknown broker type: {broker_type}")
