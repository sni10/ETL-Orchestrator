# src/message_broker/i_message_client.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class IMessageClient(ABC):

    @abstractmethod
    def get_queue_size(self) -> int:
        """Получить количество непрочитанных в очереди"""
        pass

    @abstractmethod
    def consume_messages(self, max_records: int) -> List[Dict[str, Any]]:
        """Получить непрочитанные сообщения"""
        pass

    @abstractmethod
    def send_message(self, message: Dict[str, Any]) -> None:
        """Отправить сообщение"""
        pass

    def close(self):
        pass