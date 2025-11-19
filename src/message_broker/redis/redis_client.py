import redis
from ..i_message_client import IMessageClient
from src.utils.logger import setup_logger
from src.utils.config import Config


class RedisClient(IMessageClient):
    def __init__(self, config: Config):
        self.logger = setup_logger(__name__)
        self.config = config
        self.topic = config.broker_topic
        self.redis = redis.from_url(config.broker_redis_url)
        self.last_id = '0'

    def get_queue_size(self):
        try:
            stream_info = self.redis.xinfo_stream(self.topic)
            return stream_info['length']
        except redis.ResponseError:
            return 0

    def consume_messages(self, max_records):
        try:
            messages = self.redis.xread({self.topic: self.last_id}, count=max_records, block=1000)

            result = []
            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    self.last_id = msg_id.decode()
                    result.append({k.decode(): v.decode() for k, v in fields.items()})
            return result
        except redis.ResponseError:
            return []

    def send_message(self, message):
        self.redis.xadd(self.topic, message)