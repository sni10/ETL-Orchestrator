# src/validators/broker_validator.py
import re
from typing import TYPE_CHECKING
from .base_validator import BaseValidator

if TYPE_CHECKING:
    from src.utils.config import Config


class BrokerValidator(BaseValidator):
    def validate(self, config: 'Config') -> bool:
        self.errors.clear()

        broker_type = config.broker_type

        if broker_type == 'kafka':
            self._validate_kafka_type(config)
        elif broker_type == 'redis':
            self._validate_redis_type(config)
        elif broker_type in ['rabbit', 'rabbitmq']:
            self._validate_rabbit_type(config)
        else:
            self.add_error('broker.type', f"Неподдерживаемый тип брокера: {broker_type}")

        return len(self.errors) == 0

    def _validate_kafka_type(self, config: 'Config') -> None:
        topic = config.broker_topic
        servers = config.broker_kafka_bootstrap_servers

        if not topic:
            self.add_error('broker.topic', "Поле 'topic' обязательно")

        if not servers:
            self.add_error('broker.kafka_bootstrap_servers', "Поле 'kafka_bootstrap_servers' обязательно")
            return

        for server in servers:
            if not re.match(r'^[a-zA-Z0-9.-]+:\d+$', server):
                self.add_error('broker.kafka_bootstrap_servers', f"Неверный формат сервера: {server}")

    def _validate_redis_type(self, config: 'Config') -> None:
        redis_url = config.broker_redis_url
        topic = config.broker_topic

        if not redis_url:
            self.add_error('broker.redis_url', "Для типа 'redis' должен быть указан 'redis_url'")

        if not topic:
            self.add_error('broker.topic', "Поле 'topic' обязательно")

    def _validate_rabbit_type(self, config: 'Config') -> None:
        host = config.broker_rabbitmq_host
        port = config.broker_rabbitmq_port
        username = config.broker_rabbitmq_username
        password = config.broker_rabbitmq_password
        topic = config.broker_topic

        if not host:
            self.add_error('broker.host', "Поле 'host' обязательно")
        if not port or not (1 <= port <= 65535):
            self.add_error('broker.port', "Порт должен быть в диапазоне 1-65535")
        if not username:
            self.add_error('broker.username', "Поле 'username' обязательно")
        if not password:
            self.add_error('broker.password', "Поле 'password' обязательно")
        if not topic:
            self.add_error('broker.topic', "Поле 'topic' обязательно")
